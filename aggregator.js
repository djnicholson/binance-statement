const Database = require('./database');

class Event {
    constructor(utcTimestamp, eventType) {
        this.utcTimestamp = utcTimestamp;
        this.eventType = eventType;
    }
}

const appendPortfolioValuationAndEmit = async(enumerationState, event) => {
    event.totalPortfolioValue = 0.0;
    event.valuationComposition = {};
    for (let asset in enumerationState.balances) {
        const balance = enumerationState.balances[asset];
        if (balance > 0) {
            const assetPrice = await enumerationState.aggregator.priceCache.getPrice(
                event.utcTimestamp,
                asset,
                enumerationState.aggregator.unitOfAccount);

            if (assetPrice === null) {
                event.valuationComposition[asset] = null;
                event.totalPortfolioValue = null;
            } else if (assetPrice === undefined) {
                event.valuationComposition[asset] = undefined;
            } else {
                const assetValue = assetPrice * balance;
                event.valuationComposition[asset] = assetValue;
                if (event.totalPortfolioValue !== null) {
                    event.totalPortfolioValue += assetValue;
                }
            }
        }
    }

    await enumerationState.callback(event);

    enumerationState.lastEventTimestamp = event.utcTimestamp;
};

const emitSnapshotsUpUntil = async(enumerationState, utcTimestamp) => {
    if (enumerationState.lastEventTimestamp > 0) {
        while ((enumerationState.lastEventTimestamp + (enumerationState.aggregator.valuationIntervalInMinutes * 60 * 1000)) < utcTimestamp) {
            await appendPortfolioValuationAndEmit(
                enumerationState,
                new Event(
                    enumerationState.lastEventTimestamp + (enumerationState.aggregator.valuationIntervalInMinutes * 60 * 1000),
                    Aggregator.EVENT_TYPE_SNAPSHOT));
        }
    }
};

const adjustBalance = (enumerationState, asset, adjustment) => {
    if (!enumerationState.balances[asset]) {
        enumerationState.balances[asset] = 0;
    }

    enumerationState.balances[asset] += parseFloat(adjustment);
};

const addLot = async(enumerationState, asset, quantity, costBasisAsset, costBasisQuantity, sourceDescription, utcTimestamp) => {
    if (!enumerationState.lots[asset]) {
        enumerationState.lots[asset] = [];
    }

    const priceOfCostBasisAsset = await enumerationState.aggregator.priceCache.getPrice(utcTimestamp, costBasisAsset, enumerationState.aggregator.unitOfAccount);

    const lots = enumerationState.lots[asset];
    lots.push({
        quantity: parseFloat(quantity),
        costBasisPrice: priceOfCostBasisAsset ? (priceOfCostBasisAsset * costBasisQuantity) / quantity : null,
        sourceDescription: sourceDescription,
        utcTimestamp: utcTimestamp,
    });
};

const matchLots = (enumerationState, asset, amount) => {
    const lots = enumerationState.lots[asset] || [];
    const result = [];
    let amountRemaining = amount;
    while (amountRemaining > 0) {
        if (lots.length == 0) {
            result.push({ asset: asset, quantity: amountRemaining, costBasisPrice: null, sourceDescription: 'Unknown', utcTimestamp: null });
            amountRemaining = 0;
        } else {
            var peek = lots[0];
            if (peek.quantity > amountRemaining) {
                result.push({ asset: asset, quantity: amountRemaining, costBasisPrice: peek.costBasisPrice, sourceDescription: peek.sourceDescription, utcTimestamp: peek.utcTimestamp });
                amountRemaining = 0;
                peek.quantity -= amountRemaining;
            } else {
                result.push({ asset: asset, quantity: peek.quantity, costBasisPrice: peek.costBasisPrice, sourceDescription: peek.sourceDescription, utcTimestamp: peek.utcTimestamp });
                amountRemaining -= peek.quantity;
                lots.shift();
            }
        }
    }

    return result;
};

const handleFill = async(enumerationState, record) => {
    await emitSnapshotsUpUntil(enumerationState, record.UtcTimestamp);

    adjustBalance(enumerationState, record.CommissionAsset, -1 * record.Commission);

    const commissionDebitedFromProceeds =
        (record.CommissionAsset !== 'BNB') ||
        ((record.BaseAsset === 'BNB') && record.IsBuyer) ||
        ((record.QuoteAsset === 'BNB') && !record.IsBuyer);
    const commissionLots = commissionDebitedFromProceeds ? [] : matchLots(enumerationState, 'BNB', record.Commission);

    let event = null;
    if (record.IsBuyer) {

        adjustBalance(enumerationState, record.BaseAsset, record.Quantity);
        adjustBalance(enumerationState, record.QuoteAsset, -1 * (record.Quantity * record.Price));

        await addLot(
            enumerationState,
            record.BaseAsset,
            record.Quantity,
            record.QuoteAsset,
            record.Price * record.Quantity,
            'Bought using ' + record.QuoteAsset,
            record.UtcTimestamp);

        event = new Event(record.UtcTimestamp, Aggregator.EVENT_TYPE_BUY);
        event.lots = commissionLots;

    } else {

        adjustBalance(enumerationState, record.BaseAsset, -1 * record.Quantity);
        adjustBalance(enumerationState, record.QuoteAsset, record.Quantity * record.Price);

        event = new Event(record.UtcTimestamp, Aggregator.EVENT_TYPE_SELL);
        event.lots = commissionLots.concat(matchLots(enumerationState, record.BaseAsset, record.Quantity));

    }

    const commissionAssetPrice = await enumerationState.aggregator.priceCache.getPrice(
        record.UtcTimestamp,
        record.CommissionAsset,
        enumerationState.aggregator.unitOfAccount);

    const baseAssetPrice = await enumerationState.aggregator.priceCache.getPrice(
        record.UtcTimestamp,
        record.BaseAsset,
        enumerationState.aggregator.unitOfAccount);

    event.fillId = record.Id;
    event.baseAsset = record.BaseAsset;
    event.quoteAsset = record.QuoteAsset;
    event.market = record.Symbol;
    event.orderId = record.OrderId;
    event.price = parseFloat(record.Price);
    event.quantity = parseFloat(record.Quantity);
    event.commission = parseFloat(record.Commission);
    event.commissionAsset = record.CommissionAsset;
    event.commissionDebitedFromProceeds = !!commissionDebitedFromProceeds;
    event.isMaker = !!record.IsMaker;
    event.commissionValue = commissionAssetPrice ? commissionAssetPrice * record.Commission : null;
    event.value = baseAssetPrice ? baseAssetPrice * record.Quantity : null;

    await appendPortfolioValuationAndEmit(enumerationState, event);
};

const handleDeposit = async(enumerationState, record) => {
    await emitSnapshotsUpUntil(enumerationState, record.UtcTimestamp);

    if (record.Status == 0) {
        return;
    }

    adjustBalance(enumerationState, record.Asset, record.Amount);

    await addLot(enumerationState, record.Asset, record.Amount, record.Asset, record.Amount, 'Deposited', record.UtcTimestamp);

    const event = new Event(record.UtcTimestamp, Aggregator.EVENT_TYPE_DEPOSIT);
    event.asset = record.Asset;
    event.amount = record.Amount;
    await appendPortfolioValuationAndEmit(enumerationState, event);
};

const handleWithdrawal = async(enumerationState, record) => {
    await emitSnapshotsUpUntil(enumerationState, record.UtcTimestamp);

    if (record.Status != 6) {
        return;
    }

    adjustBalance(enumerationState, record.Asset, -1 * record.Amount);

    const event = new Event(record.UtcTimestamp, Aggregator.EVENT_TYPE_WITHDRAWAL);
    event.asset = record.Asset;
    event.amount = record.Amount;
    event.lots = matchLots(enumerationState, record.Asset, record.Amount);
    await appendPortfolioValuationAndEmit(enumerationState, event);
};

const handleBalanceCheckpoint = async(enumerationState, record) => {
    await emitSnapshotsUpUntil(enumerationState, record.UtcTimestamp);

    const expectedBalance = enumerationState.balances[record.Asset] || 0.0;
    const actualBalance = parseFloat(record.Free) + parseFloat(record.Locked);
    const adjustment = actualBalance - expectedBalance;
    if (adjustment != 0) {

        adjustBalance(enumerationState, record.Asset, adjustment);

        if (adjustment > 0) {

            await addLot(enumerationState, record.Asset, adjustment, record.Asset, 0, 'Credited by Binance', record.UtcTimestamp);

            const event = new Event(record.UtcTimestamp, Aggregator.EVENT_TYPE_BINANCE_CREDIT);
            event.asset = record.Asset;
            event.amount = adjustment;
            await appendPortfolioValuationAndEmit(enumerationState, event);

        } else {

            const event = new Event(record.UtcTimestamp, Aggregator.EVENT_TYPE_BINANCE_DEBIT);
            event.asset = record.Asset;
            event.amount = -1 * adjustment;
            event.lots = matchLots(enumerationState, record.Asset, -1 * adjustment);
            await appendPortfolioValuationAndEmit(enumerationState, event);

        }
    }
};

class Aggregator {

    static get Event() { return Event; }

    constructor(db, priceCache, unitOfAccount, valuationIntervalInMinutes) {
        this.db = db;
        this.priceCache = priceCache;
        this.unitOfAccount = unitOfAccount;
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
            switch (record.RecordType) {
                case Database.RECORD_TYPE_FILL:
                    await handleFill(enumerationState, record);
                    break;
                case Database.RECORD_TYPE_DEPOSIT:
                    await handleDeposit(enumerationState, record);
                    break;
                case Database.RECORD_TYPE_WITHDRAWAL:
                    await handleWithdrawal(enumerationState, record);
                    break;
                case Database.RECORD_TYPE_BALANCE:
                    await handleBalanceCheckpoint(enumerationState, record);
                    break;
                default:
                    console.warn('Ignoring data record of unknown type: %j', record);
                    break;
            }
        });

        await emitSnapshotsUpUntil(enumerationState, (new Date).getTime());
    };

}

module.exports = Aggregator;