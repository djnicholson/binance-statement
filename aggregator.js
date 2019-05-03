const BigNumber = require('bignumber.js');

const Database = require('./database');

class Event {
    constructor(utcTimestamp, eventType) {
        this.utcTimestamp = utcTimestamp;
        this.eventType = eventType;
    }
}

const statusCallback = (callSite) => {
    return message => {
        // console.debug('%s: %s', callSite, message)
    };
};

const isBeforeStartDate = (utcTimestamp, startMonth, startYear) => {
    const date = new Date(utcTimestamp);
    return ((date.getFullYear() < startYear) ||
        ((date.getFullYear() == startYear) && ((date.getMonth() + 1) < startMonth)));
};

const appendPortfolioValuationAndEmit = async(enumerationState, event, startMonth, startYear) => {
    enumerationState.lastEventTimestamp = event.utcTimestamp;

    if (isBeforeStartDate(event.utcTimestamp, startMonth, startYear)) {
        return;
    }

    event.totalPortfolioValue = new BigNumber(0.0);
    event.valuationComposition = {};
    for (let asset in enumerationState.balances) {
        const balance = enumerationState.balances[asset];
        if (balance.isGreaterThan(0)) {
            const assetPrice = await enumerationState.aggregator.priceCache.getPrice(
                event.utcTimestamp,
                asset,
                enumerationState.aggregator.unitOfAccount,
                statusCallback('appendPortfolioValuationAndEmit'));

            if (assetPrice === null) {
                event.valuationComposition[asset] = null;
                event.totalPortfolioValue = null;
            } else if (assetPrice === undefined) {
                event.valuationComposition[asset] = undefined;
            } else {
                const assetValue = assetPrice.multipliedBy(balance);
                event.valuationComposition[asset] = assetValue;
                if (event.totalPortfolioValue !== null) {
                    event.totalPortfolioValue = event.totalPortfolioValue.plus(assetValue);
                }
            }
        }
    }

    await enumerationState.callback(event);
};

const emitSnapshotsUpUntil = async(enumerationState, utcTimestamp, startMonth, startYear) => {
    if (enumerationState.lastEventTimestamp > 0) {
        while ((enumerationState.lastEventTimestamp + (enumerationState.aggregator.valuationIntervalInMinutes * 60 * 1000)) < utcTimestamp) {
            await appendPortfolioValuationAndEmit(
                enumerationState,
                new Event(
                    enumerationState.lastEventTimestamp + (enumerationState.aggregator.valuationIntervalInMinutes * 60 * 1000),
                    Aggregator.EVENT_TYPE_SNAPSHOT),
                startMonth,
                startYear);
        }
    }
};

const adjustBalance = (enumerationState, asset, adjustment) => {
    if (!enumerationState.balances[asset]) {
        enumerationState.balances[asset] = new BigNumber(0.0);
    }

    enumerationState.balances[asset] = enumerationState.balances[asset].plus(new BigNumber(adjustment));
};

const addLot = async(enumerationState, asset, quantity, costBasisAsset, costBasisQuantity, sourceDescription, utcTimestamp) => {
    if (!enumerationState.lots[asset]) {
        enumerationState.lots[asset] = [];
    }

    // console.debug('Looking up historical (%s) price of %s so that cost-price of %f %s can be recorded', new Date(utcTimestamp), costBasisAsset, quantity, asset);
    const priceOfCostBasisAsset = await enumerationState.aggregator.priceCache.getPrice(
        utcTimestamp,
        costBasisAsset,
        enumerationState.aggregator.unitOfAccount,
        statusCallback('addLot(' + sourceDescription + ')'));

    const lots = enumerationState.lots[asset];
    lots.push({
        quantity: new BigNumber(quantity),
        costBasisPrice: priceOfCostBasisAsset ? priceOfCostBasisAsset.multipliedBy(costBasisQuantity).dividedBy(quantity) : null,
        sourceDescription: sourceDescription,
        utcTimestamp: utcTimestamp,
    });
};

const matchLots = (enumerationState, asset, amount) => {
    let lots = enumerationState.lots[asset] || [];
    const result = [];
    let amountRemaining = new BigNumber(amount);
    while (amountRemaining.isGreaterThan(0)) {
        if (lots.length == 0) {
            result.push({ asset: asset, quantity: amountRemaining, costBasisPrice: new BigNumber(0), sourceDescription: 'from an unknown source', utcTimestamp: null });
            amountRemaining = new BigNumber(0);
        } else {
            var peek = lots[0];
            if (peek.quantity.isGreaterThan(amountRemaining)) {
                result.push({ asset: asset, quantity: amountRemaining, costBasisPrice: peek.costBasisPrice, sourceDescription: peek.sourceDescription, utcTimestamp: peek.utcTimestamp });
                peek.quantity = peek.quantity.minus(amountRemaining);
                amountRemaining = new BigNumber(0);
            } else {
                result.push({ asset: asset, quantity: peek.quantity, costBasisPrice: peek.costBasisPrice, sourceDescription: peek.sourceDescription, utcTimestamp: peek.utcTimestamp });
                amountRemaining = amountRemaining.minus(peek.quantity);
                lots.shift();
            }
        }
    }

    return result;
};

const handleFill = async(enumerationState, record, startMonth, startYear) => {
    await emitSnapshotsUpUntil(enumerationState, record.UtcTimestamp);

    adjustBalance(enumerationState, record.CommissionAsset, new BigNumber(record.Commission).multipliedBy(-1));

    const commissionDebitedFromProceeds =
        (record.CommissionAsset !== 'BNB') ||
        ((record.BaseAsset === 'BNB') && record.IsBuyer) ||
        ((record.QuoteAsset === 'BNB') && !record.IsBuyer);
    const commissionLots = commissionDebitedFromProceeds ? [] : matchLots(enumerationState, 'BNB', record.Commission);
    let commissionCost = new BigNumber(0.0);
    for (let i = 0; i < commissionLots.length; i++) {
        commissionCost = commissionCost.plus(commissionLots[i].costBasisPrice.multipliedBy(commissionLots[i].quantity));
    }

    let event = null;
    if (record.IsBuyer) {

        adjustBalance(enumerationState, record.BaseAsset, record.Quantity);
        adjustBalance(enumerationState, record.QuoteAsset, new BigNumber(-1).multipliedBy(record.Quantity).multipliedBy(record.Price));

        await addLot(
            enumerationState,
            record.BaseAsset,
            record.Quantity,
            record.QuoteAsset,
            new BigNumber(record.Price).multipliedBy(record.Quantity),
            'bought using ' + record.QuoteAsset,
            record.UtcTimestamp);

        event = new Event(record.UtcTimestamp, Aggregator.EVENT_TYPE_BUY);
        event.lots = commissionLots;

    } else {

        adjustBalance(enumerationState, record.BaseAsset, new BigNumber(record.Quantity).multipliedBy(-1));
        adjustBalance(enumerationState, record.QuoteAsset, new BigNumber(record.Quantity).multipliedBy(record.Price));

        event = new Event(record.UtcTimestamp, Aggregator.EVENT_TYPE_SELL);
        event.lots = commissionLots.concat(matchLots(enumerationState, record.BaseAsset, record.Quantity));

    }

    if (isBeforeStartDate(record.UtcTimestamp, startMonth, startYear)) {
        return;
    }

    const commissionAssetPrice = await enumerationState.aggregator.priceCache.getPrice(
        record.UtcTimestamp,
        record.CommissionAsset,
        enumerationState.aggregator.unitOfAccount,
        statusCallback('handleFill/commissionAssetPrice'));

    const baseAssetPrice = await enumerationState.aggregator.priceCache.getPrice(
        record.UtcTimestamp,
        record.BaseAsset,
        enumerationState.aggregator.unitOfAccount,
        statusCallback('handleFill/baseAssetPrice'));

    const commissionValue = commissionAssetPrice ? commissionAssetPrice.multipliedBy(record.Commission) : null;

    event.fillId = record.Id;
    event.baseAsset = record.BaseAsset;
    event.quoteAsset = record.QuoteAsset;
    event.market = record.Symbol;
    event.orderId = record.OrderId;
    event.price = new BigNumber(record.Price);
    event.quantity = new BigNumber(record.Quantity);
    event.commission = new BigNumber(record.Commission);
    event.commissionAsset = record.CommissionAsset;
    event.commissionDebitedFromProceeds = !!commissionDebitedFromProceeds;
    event.commissionCost = commissionDebitedFromProceeds ? commissionValue : commissionCost;
    event.isMaker = !!record.IsMaker;
    event.commissionValue = commissionValue;
    event.value = baseAssetPrice ? baseAssetPrice.multipliedBy(record.Quantity) : null;

    await appendPortfolioValuationAndEmit(enumerationState, event, startMonth, startYear);
};

const handleDeposit = async(enumerationState, record, startMonth, startYear) => {
    await emitSnapshotsUpUntil(enumerationState, record.UtcTimestamp);

    if (record.Status == 0) {
        return;
    }

    adjustBalance(enumerationState, record.Asset, record.Amount);

    await addLot(enumerationState, record.Asset, record.Amount, record.Asset, record.Amount, 'deposited', record.UtcTimestamp);

    if (isBeforeStartDate(record.UtcTimestamp, startMonth, startYear)) {
        return;
    }

    const assetPrice = await enumerationState.aggregator.priceCache.getPrice(
        record.UtcTimestamp,
        record.Asset,
        enumerationState.aggregator.unitOfAccount,
        statusCallback('handleDeposit'));

    const event = new Event(record.UtcTimestamp, Aggregator.EVENT_TYPE_DEPOSIT);
    event.asset = record.Asset;
    event.amount = new BigNumber(record.Amount);
    event.value = assetPrice ? assetPrice.multipliedBy(record.Amount) : null;
    await appendPortfolioValuationAndEmit(enumerationState, event, startMonth, startYear);
};

const handleWithdrawal = async(enumerationState, record, startMonth, startYear) => {
    await emitSnapshotsUpUntil(enumerationState, record.UtcTimestamp);

    if (record.Status != 6) {
        return;
    }

    adjustBalance(enumerationState, record.Asset, new BigNumber(-1).multipliedBy(record.Amount));

    const event = new Event(record.UtcTimestamp, Aggregator.EVENT_TYPE_WITHDRAWAL);
    event.asset = record.Asset;
    event.amount = new BigNumber(record.Amount);
    event.lots = matchLots(enumerationState, record.Asset, record.Amount);

    if (isBeforeStartDate(record.UtcTimestamp, startMonth, startYear)) {
        return;
    }

    const assetPrice = await enumerationState.aggregator.priceCache.getPrice(
        record.UtcTimestamp,
        record.Asset,
        enumerationState.aggregator.unitOfAccount,
        statusCallback('handleWithdrawal'));
    event.value = assetPrice ? assetPrice.multipliedBy(record.Amount) : null;

    await appendPortfolioValuationAndEmit(enumerationState, event, startMonth, startYear);
};

const handleBalanceCheckpoint = async(enumerationState, record, startMonth, startYear) => {
    await emitSnapshotsUpUntil(enumerationState, record.UtcTimestamp);

    const expectedBalance = enumerationState.balances[record.Asset] || new BigNumber(0.0);
    const actualBalance = new BigNumber(record.Free).plus(record.Locked);
    const adjustment = actualBalance.minus(expectedBalance);
    if (!adjustment.isZero()) {

        adjustBalance(enumerationState, record.Asset, adjustment);

        console.debug('%s adjustment on %s, expected: %f, actual: %f', record.Asset, new Date(record.UtcTimestamp), expectedBalance, actualBalance);

        let event;
        if (adjustment.isGreaterThan(0.0)) {

            await addLot(enumerationState, record.Asset, adjustment, record.Asset, 0, 'credited by Binance', record.UtcTimestamp);

            event = new Event(record.UtcTimestamp, Aggregator.EVENT_TYPE_BINANCE_CREDIT);
            event.amount = adjustment;

        } else {

            event = new Event(record.UtcTimestamp, Aggregator.EVENT_TYPE_BINANCE_DEBIT);
            event.amount = adjustment.multipliedBy(-1);
            event.lots = matchLots(enumerationState, record.Asset, adjustment.multipliedBy(-1));

        }

        if (isBeforeStartDate(record.UtcTimestamp, startMonth, startYear)) {
            return;
        }

        event.asset = record.Asset;

        const assetPrice = await enumerationState.aggregator.priceCache.getPrice(
            record.UtcTimestamp,
            record.Asset,
            enumerationState.aggregator.unitOfAccount,
            statusCallback('handleBalanceCheckpoint'));
        event.value = assetPrice ? assetPrice.multipliedBy(event.amount) : null;

        await appendPortfolioValuationAndEmit(enumerationState, event, startMonth, startYear);
    }
};

class Aggregator {

    static get Event() { return Event; }

    constructor(db, priceCache, unitOfAccount, startMonth, startYear, valuationIntervalInMinutes) {
        this.db = db;
        this.priceCache = priceCache;
        this.unitOfAccount = unitOfAccount;
        this.startMonth = startMonth;
        this.startYear = startYear;
        this.valuationIntervalInMinutes = valuationIntervalInMinutes;
    }

    static get EVENT_TYPE_SNAPSHOT() { return 'EVENT_TYPE_SNAPSHOT'; }
    static get EVENT_TYPE_BUY() { return 'EVENT_TYPE_BUY'; }
    static get EVENT_TYPE_SELL() { return 'EVENT_TYPE_SELL'; }
    static get EVENT_TYPE_DEPOSIT() { return 'EVENT_TYPE_DEPOSIT'; }
    static get EVENT_TYPE_WITHDRAWAL() { return 'EVENT_TYPE_WITHDRAWAL'; }
    static get EVENT_TYPE_BINANCE_CREDIT() { return 'EVENT_TYPE_BINANCE_CREDIT'; }
    static get EVENT_TYPE_BINANCE_DEBIT() { return 'EVENT_TYPE_BINANCE_DEBIT'; }
    static get EVENT_TYPE_BUY_AGGREGATION() { return 'EVENT_TYPE_BUY_AGGREGATION'; }
    static get EVENT_TYPE_SELL_AGGREGATION() { return 'EVENT_TYPE_SELL_AGGREGATION'; }

    async enumerateEvents(callback) {

        const enumerationState = {
            aggregator: this,
            callback: callback,
            lastEventTimestamp: 0,
            balances: {},
            lots: {},
        };

        await this.db.forEachRecord(async record => {
            if (record.UtcTimestamp + (1000 * 60) < (new Date).getTime()) { // need enclosing 1m candle to be complete for pricing
                switch (record.RecordType) {
                    case Database.RECORD_TYPE_FILL:
                        await handleFill(enumerationState, record, this.startMonth, this.startYear);
                        break;
                    case Database.RECORD_TYPE_DEPOSIT:
                        await handleDeposit(enumerationState, record, this.startMonth, this.startYear);
                        break;
                    case Database.RECORD_TYPE_WITHDRAWAL:
                        await handleWithdrawal(enumerationState, record, this.startMonth, this.startYear);
                        break;
                    case Database.RECORD_TYPE_BALANCE:
                        await handleBalanceCheckpoint(enumerationState, record, this.startMonth, this.startYear);
                        break;
                    default:
                        console.warn('Ignoring data record of unknown type: %j', record);
                        break;
                }
            }
        });

        await emitSnapshotsUpUntil(enumerationState, (new Date).getTime(), this.startMonth, this.startYear);
    };

}

module.exports = Aggregator;