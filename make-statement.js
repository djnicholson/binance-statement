const Binance = require('binance-api-node').default;
const Database = require('./database');

const synchronizeFills = async(binance, db) => {
    const exchangeInfo = await binance.exchangeInfo();
    for (let i = 0; i < exchangeInfo.symbols.length; i++) {
        const baseAsset = exchangeInfo.symbols[i].baseAsset;
        const quoteAsset = exchangeInfo.symbols[i].quoteAsset;
        const symbol = exchangeInfo.symbols[i].symbol;
        console.debug('Synchronizing fills in %s', symbol);
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

                console.debug('Logging fill in %s (%f @ %f on %d)', symbol, trade.qty, trade.price, trade.time);
                await db.logFill(trade.id, baseAsset, quoteAsset, symbol, trade.orderId, trade.price, trade.qty,
                    trade.commission, trade.commissionAsset, trade.time, trade.isBuyer, trade.isMaker, trade.isBestMatch);
            }
        } while (newRecords);
    }
};

const synchronizeDeposits = async(binance, db) => {
    console.debug('Synchronizing deposits');
    let mostRecentTimestamp = await db.getMostRecentDepositTime();
    let newRecords;
    do {
        newRecords = false;
        const records = await binance.depositHistory({ startTime: mostRecentTimestamp });
        for (let i = 0; i < records.length; i++) {
            const record = records[i];
            if (record.insertTime > mostRecentTimestamp) {
                mostRecentTimestamp = record.insertTime;
                newRecords = true;
            }

            console.debug('Logging deposit of %f %s on %d', record.amount, record.asset, record.insertTime);
            await db.logDeposit(record.insertTime, record.asset, record.amount, record.status);
        }
    } while (newRecords);
};

const synchronizeWithdrawals = async(binance, db) => {
    console.debug('Synchronizing withdrawals');
    let mostRecentTimestamp = await db.getMostRecentWithdrawalTime();
    let newRecords;
    do {
        newRecords = false;
        const records = await binance.withdrawHistory({ startTime: mostRecentTimestamp });
        for (let i = 0; i < records.length; i++) {
            const record = records[i];
            if (record.applyTime > mostRecentTimestamp) {
                mostRecentTimestamp = record.applyTime;
                newRecords = true;
            }

            console.debug('Logging withdrawal of %f %s on %d', record.amount, record.asset, record.applyTime);
            await db.logDeposit(record.applyTime, record.asset, record.amount, record.address, record.status);
        }
    } while (newRecords);
};

const main = async(apiKey, apiSecret, outputFile, dataFile) => {
    try {
        console.log("Making statement...", apiKey, apiSecret, outputFile, dataFile);
        const db = await Database.open(dataFile);
        const binance = new Binance({ apiKey: apiKey, apiSecret: apiSecret });
        await synchronizeDeposits(binance, db);
        await synchronizeWithdrawals(binance, db);
        await synchronizeFills(binance, db);
    } catch (e) {
        console.error(e);
    }
};

module.exports = main;