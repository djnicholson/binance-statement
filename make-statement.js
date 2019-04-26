const Binance = require('binance-api-node').default;

const Aggregator = require('./aggregator');
const Database = require('./database');
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
    process.stdout.write("[0%] Preparing...");
    const exchangeInfo = await binance.exchangeInfo();
    try {
        for (let i = 0; i < exchangeInfo.symbols.length; i++) {
            const progressString = Math.round((i / exchangeInfo.symbols.length) * 100) + '%';
            const baseAsset = exchangeInfo.symbols[i].baseAsset;
            const quoteAsset = exchangeInfo.symbols[i].quoteAsset;
            const symbol = exchangeInfo.symbols[i].symbol;
            process.stdout.clearLine();
            process.stdout.cursorTo(0);
            process.stdout.write('[' + progressString + '] Synchronizing fills in ' + symbol);
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

                    process.stdout.clearLine();
                    process.stdout.cursorTo(0);
                    process.stdout.write('[' + progressString + '] Logging fill in ' + symbol + ' (' + trade.qty + ' @ ' + trade.price + ' on ' + trade.time + ')');
                    await db.logFill(trade.id, baseAsset, quoteAsset, symbol, trade.orderId, trade.price, trade.qty,
                        trade.commission, trade.commissionAsset, trade.time, trade.isBuyer, trade.isMaker, trade.isBestMatch);
                }

                await sleepForBinance(speed);
            } while (newRecords);
        }
    } finally {
        process.stdout.clearLine();
        process.stdout.cursorTo(0);
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

const main = async(apiKey, apiSecret, outputFile, dataFile, cacheFile, syncFillsFromBinance, speed) => {
    try {
        const db = await Database.open(dataFile);
        const binance = new Binance({ apiKey: apiKey, apiSecret: apiSecret });
        const priceCache = await PriceCache.create(cacheFile, binance, async() => { await sleepForBinance(speed); });
        const aggregator = new Aggregator(db, priceCache, 'USDT', /*valuationIntervalInMinutes*/ 60);

        await takeBalanceSnapshot(binance, db, speed);
        await synchronizeDeposits(binance, db, speed);
        await synchronizeWithdrawals(binance, db, speed);
        syncFillsFromBinance && await synchronizeFills(binance, db, speed);

        await aggregator.enumerateEvents(console.log);
    } catch (e) {
        console.error(e);
    }
};

module.exports = main;