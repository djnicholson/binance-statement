const Binance = require('binance-api-node').default;

const Aggregator = require('./aggregator');
const Database = require('./database');
const FillCombiner = require('./fill-combiner');
const HtmlWriter = require('./html-writer');
const PriceCache = require('./price-cache');

const sleepForBinance = async(speed) => {
    if (speed < 10) {
        return new Promise(resolve => setTimeout(resolve, (10 - speed) * 250));
    } else {
        return Promise.resolve();
    }
}

const synchronizeFills = async(binance, db, speed) => {
    console.debug('Synchronizing fills');
    const isTty = !!process.stdout.clearLine;
    isTty && process.stdout.write("[0%] Preparing...");
    const exchangeInfo = await binance.exchangeInfo();
    try {
        for (let i = 0; i < exchangeInfo.symbols.length; i++) {
            const progressString = Math.round((i / exchangeInfo.symbols.length) * 100) + '%';
            const baseAsset = exchangeInfo.symbols[i].baseAsset;
            const quoteAsset = exchangeInfo.symbols[i].quoteAsset;
            const symbol = exchangeInfo.symbols[i].symbol;
            isTty && process.stdout.clearLine();
            isTty && process.stdout.cursorTo(0);
            isTty && process.stdout.write('[' + progressString + '] Synchronizing fills in ' + symbol);
            !isTty && console.debug('[' + progressString + '] Synchronizing fills in ' + symbol);
            let mostRecentFill = await db.getMostRecentFillId(symbol);
            let newRecords;
            do {
                newRecords = false;
                const trades = await binance.myTrades({ symbol: symbol, fromId: mostRecentFill });
                for (let j = 0; j < trades.length; j++) {
                    const trade = trades[j];
                    if (trade.id > mostRecentFill) {
                        mostRecentFill = trade.id;
                        newRecords = true;
                    }

                    isTty && process.stdout.clearLine();
                    isTty && process.stdout.cursorTo(0);
                    isTty && process.stdout.write('[' + progressString + '] Logging fill in ' + symbol + ' (' + trade.qty + ' @ ' + trade.price + ' on ' + trade.time + ')');
                    !isTty && console.debug('[' + progressString + '] Logging fill in ' + symbol + ' (' + trade.qty + ' @ ' + trade.price + ' on ' + trade.time + ')');
                    await db.logFill(trade.id, baseAsset, quoteAsset, symbol, trade.orderId, trade.price, trade.qty,
                        trade.commission, trade.commissionAsset, trade.time, trade.isBuyer, trade.isMaker, trade.isBestMatch);
                }

                await sleepForBinance(speed);
            } while (newRecords);
        }
    } finally {
        isTty && process.stdout.clearLine();
        isTty && process.stdout.cursorTo(0);
    }
};

const synchronizeDeposits = async(binance, db, speed) => {
    console.debug('Synchronizing deposits');
    let mostRecentTimestamp = await db.getMostRecentDepositTime();
    let newRecords;
    do {
        newRecords = false;
        await sleepForBinance(speed);
        const records = await binance.depositHistory({ startTime: mostRecentTimestamp });
        for (let i = 0; i < records.depositList.length; i++) {
            const record = records.depositList[i];
            if (record.insertTime > mostRecentTimestamp) {
                mostRecentTimestamp = record.insertTime;
                newRecords = true;
            }

            console.debug('Logging deposit of %f %s on %d', record.amount, record.asset, record.insertTime);
            await db.logDeposit(record.insertTime, record.asset, record.amount, record.status);
        }
    } while (newRecords);
};

const synchronizeWithdrawals = async(binance, db, speed) => {
    console.debug('Synchronizing withdrawals');
    let mostRecentTimestamp = await db.getMostRecentWithdrawalTime();
    let newRecords;
    do {
        newRecords = false;
        await sleepForBinance(speed);
        const records = await binance.withdrawHistory({ startTime: mostRecentTimestamp });
        for (let i = 0; i < records.withdrawList.length; i++) {
            const record = records.withdrawList[i];
            if (record.applyTime > mostRecentTimestamp) {
                mostRecentTimestamp = record.applyTime;
                newRecords = true;
            }

            console.debug('Logging withdrawal of %f %s on %d', record.amount, record.asset, record.applyTime);
            await db.logDeposit(record.applyTime, record.asset, record.amount, record.address, record.status);
        }
    } while (newRecords);
};

const takeBalanceSnapshot = async(binance, db, speed) => {
    const asOf = Math.round((new Date).getTime() / 1000);
    const recodingInterval = 60 * 60 * 24; // one row per day, max (assuming the program runs at least once per day)
    const recordTimestamp = Math.round(asOf / recodingInterval) * recodingInterval;
    console.debug('Taking balance snapshot; record timestamp: %d; actual time: %d', recordTimestamp, asOf);
    await sleepForBinance(speed);
    const accountInfo = await binance.accountInfo();
    await db.logBalanceSnapshot(asOf, recordTimestamp, accountInfo.balances);
};

const makePrecisoinTable = async(binance) => {
    const exchangeInfo = await binance.exchangeInfo();
    const result = { asQuantity: {}, asPrice: {} };
    for (let i = 0; i < exchangeInfo.symbols.length; i++) {
        const market = exchangeInfo.symbols[i];
        const baseAsset = market.baseAsset.toUpperCase();
        const quoteAsset = market.quoteAsset.toUpperCase();
        for (let j = 0; j < market.filters.length; j++) {
            const filter = market.filters[j];
            let x, table, asset = undefined;
            if (filter.filterType === 'LOT_SIZE') {
                x = filter.stepSize;
                table = result.asQuantity;
                asset = baseAsset;
            } else if (filter.filterType === 'PRICE_FILTER') {
                x = filter.tickSize;
                table = result.asPrice;
                asset = quoteAsset
            }

            if (asset) {
                let precision = 0;
                while (x != Math.round(x)) {
                    precision++;
                    x = x * 10;
                }

                table[asset] = Math.max(table[asset] || 0, precision);
            }
        }
    }

    return result;
};

const logInterval = 2 * 1000;

let lastLogEmission = 0;

const logEvent = event => {
    const utcNow = (new Date).getTime();
    if (lastLogEmission + logInterval > utcNow) {
        return;
    }

    const numberString = n => n ? n.toFixed() : "(unknown)";
    switch (event.eventType) {
        case Aggregator.EVENT_TYPE_BUY_AGGREGATION:
            console.log('%s: Bought          %s %s @ %s %s; value: %s; portfolio value: %f', new Date(event.utcTimestamp), numberString(event.quantity), event.baseAsset, numberString(event.price), event.quoteAsset, numberString(event.value), numberString(event.totalPortfolioValue));
            break;
        case Aggregator.EVENT_TYPE_SELL_AGGREGATION:
            console.log('%s: Sold            %s %s @ %s %s; value: %s; portfolio value: %f', new Date(event.utcTimestamp), numberString(event.quantity), event.baseAsset, numberString(event.price), event.quoteAsset, numberString(event.value), numberString(event.totalPortfolioValue));
            break;
        case Aggregator.EVENT_TYPE_BINANCE_CREDIT:
            console.log('%s: Binance credit: %s %s; portfolio value: %s', new Date(event.utcTimestamp), numberString(event.amount), event.asset, numberString(event.totalPortfolioValue));
            break;
        case Aggregator.EVENT_TYPE_BINANCE_DEBIT:
            console.log('%s: Binance debit:  %s %s; portfolio value: %s', new Date(event.utcTimestamp), numberString(event.amount), event.asset, numberString(event.totalPortfolioValue));
            break;
        case Aggregator.EVENT_TYPE_DEPOSIT:
            console.log('%s: Deposit:        %s %s; portfolio value: %s', new Date(event.utcTimestamp), numberString(event.amount), event.asset, numberString(event.totalPortfolioValue));
            break;
        case Aggregator.EVENT_TYPE_WITHDRAWAL:
            console.log('%s: Withdrawal:     %s %s; portfolio value: %s', new Date(event.utcTimestamp), numberString(event.amount), event.asset, numberString(event.totalPortfolioValue));
            break;
        case Aggregator.EVENT_TYPE_SNAPSHOT:
            console.log('%s:                              portfolio value: %s', new Date(event.utcTimestamp), numberString(event.totalPortfolioValue));
            break;
    }

    lastLogEmission = utcNow;
};

const main = async(apiKey, apiSecret, startMonth, startYear, outputFile, dataFile, cacheFile, syncFillsFromBinance, speed, unitsOfAccount) => {
    try {
        const db = await Database.open(dataFile);
        const binance = new Binance({ apiKey: apiKey, apiSecret: apiSecret });
        const priceCache = await PriceCache.create(cacheFile, binance, async() => { await sleepForBinance(speed); });

        await takeBalanceSnapshot(binance, db, speed);
        await synchronizeDeposits(binance, db, speed);
        await synchronizeWithdrawals(binance, db, speed);
        syncFillsFromBinance && await synchronizeFills(binance, db, speed);

        const htmlWriter = new HtmlWriter(outputFile);
        try {
            await htmlWriter.begin(await makePrecisoinTable(binance));
            for (let i = 0; i < unitsOfAccount.length; i++) {
                const unitOfAccount = unitsOfAccount[i];
                const aggregator = new Aggregator(db, priceCache, unitOfAccount, startMonth, startYear, /*valuationIntervalInMinutes*/ 60 * 6);
                const fillCombiner = new FillCombiner(aggregator);
                await fillCombiner.enumerateEvents(event => {
                    logEvent(event);
                    htmlWriter.consumeEvent(unitOfAccount, event);
                });
            }
        } finally {
            htmlWriter.end();
        }

    } catch (e) {
        console.error(e);
    }
};

module.exports = main;