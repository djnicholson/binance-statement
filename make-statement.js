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

const main = async(apiKey, apiSecret, outputFile, dataFile) => {
    try {
        console.log("Making statement...", apiKey, apiSecret, outputFile, dataFile);
        const db = await Database.open(dataFile);
        const binance = new Binance({ apiKey: apiKey, apiSecret: apiSecret });
        await synchronizeFills(binance, db);
    } catch (e) {
        console.error(e);
    }
};

module.exports = main;