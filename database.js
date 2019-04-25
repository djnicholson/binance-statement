const SqliteDatabase = require('sqlite-async');

const initializeSchema = async(db) => {
    await db.run('CREATE TABLE IF NOT EXISTS Fills (Id, BaseAsset, QuoteAsset, Symbol, OrderId, Price, Quantity, Commission, ' +
        'CommissionAsset, UtcTimestamp, IsBuyer, IsMaker, IsBestMatch)');
    await db.run('CREATE UNIQUE INDEX IF NOT EXISTS IdIndex ON Fills (Id)');
};

const normalizeAssetCase = asset => asset.toUpperCase();

class Database {

    static async open(dataFile) {
        console.debug("Opening database ", dataFile);
        const db = await SqliteDatabase.open(dataFile);
        await initializeSchema(db);
        return new Database(db);
    }

    constructor(db) {
        this.db = db;
    }

    async getMostRecentFillId(symbol) {
        symbol = normalizeAssetCase(symbol);
        const query = 'SELECT Id FROM Fills WHERE Symbol = $symbol ORDER BY Id DESC LIMIT 1';
        const row = await this.db.get(query, { $symbol: symbol });
        return row ? row.Id : 0;
    }

    async logFill(id, baseAsset, quoteAsset, symbol, orderId, price, quantity, commission, commissionAsset,
        utcTimestamp, isBuyer, isMaker, isBestMatch) {
        symbol = normalizeAssetCase(symbol);
        baseAsset = normalizeAssetCase(baseAsset);
        quoteAsset = normalizeAssetCase(quoteAsset);
        const query = 'INSERT OR REPLACE INTO Fills VALUES ( $id , $baseAsset, $quoteAsset, $symbol, $orderId, $price, $quantity, $commission, ' +
            '$commissionAsset, $utcTimestamp, $isBuyer, $isMaker, $isBestMatch )';
        await this.db.run(query, {
            $id: id,
            $baseAsset: baseAsset,
            $quoteAsset: quoteAsset,
            $symbol: symbol,
            $orderId: orderId,
            $price: price,
            $quantity: quantity,
            $commission: commission,
            $commissionAsset: commissionAsset,
            $utcTimestamp: utcTimestamp,
            $isBuyer: isBuyer,
            $isMaker: isMaker,
            $isBestMatch: isBestMatch
        });
    }
};

module.exports = Database;