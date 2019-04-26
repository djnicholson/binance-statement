const SqliteDatabase = require('sqlite-async');

const initializeSchema = async(db) => {
    await db.run('CREATE TABLE IF NOT EXISTS Fills (Id, BaseAsset, QuoteAsset, Symbol, OrderId, Price, Quantity, Commission, ' +
        'CommissionAsset, UtcTimestamp, IsBuyer, IsMaker, IsBestMatch)');
    await db.run('CREATE UNIQUE INDEX IF NOT EXISTS IdIndex ON Fills (Id)');

    await db.run('CREATE TABLE IF NOT EXISTS Deposits (UtcTimestamp, Asset, Amount, Status)');
    await db.run('CREATE UNIQUE INDEX IF NOT EXISTS UniqueDeposit ON Deposits (UtcTimestamp, Asset, Amount)');

    await db.run('CREATE TABLE IF NOT EXISTS Withdrawals (UtcTimestamp, Asset, Amount, Address, Status)');
    await db.run('CREATE UNIQUE INDEX IF NOT EXISTS UniqueWithdrawal ON Withdrawals (UtcTimestamp, Asset, Amount, Address)');

    await db.run('CREATE TABLE IF NOT EXISTS Balances (RecordTimestamp, CollectionTime, Asset, Free, Locked)');
    await db.run('CREATE UNIQUE INDEX IF NOT EXISTS UniqueRecordTime ON Balances (RecordTimestamp, Asset)');
};

const normalizeAssetCase = asset => asset.toUpperCase();

const lowerTimestamp = (thisRecord, otherRecords) => {
    if (!thisRecord) {
        return false;
    }

    for (let i = 0; i < otherRecords.length; i++) {
        if (otherRecords[i] && (otherRecords[i].UtcTimestamp < thisRecord.UtcTimestamp)) {
            return false;
        }
    }

    return true;
}

class Database {

    static async open(dataFile) {
        const db = await SqliteDatabase.open(dataFile);
        await initializeSchema(db);
        return new Database(db);
    }

    constructor(db) {
        this.db = db;
    }

    static get RECORD_TYPE_FILL() { return 'RECORD_TYPE_FILL'; }
    static get RECORD_TYPE_DEPOSIT() { return 'RECORD_TYPE_DEPOSIT'; }
    static get RECORD_TYPE_WITHDRAWAL() { return 'RECORD_TYPE_WITHDRAWAL'; }
    static get RECORD_TYPE_BALANCE() { return 'RECORD_TYPE_BALANCE'; }

    async forEachRecord(callback) {
        const fillsReader = await this.db.prepare('SELECT * FROM Fills ORDER BY UtcTimestamp ASC');
        const depositReader = await this.db.prepare('SELECT * FROM Deposits ORDER BY UtcTimestamp ASC');
        const withdrawalReader = await this.db.prepare('SELECT * FROM Withdrawals ORDER BY UtcTimestamp ASC');
        const balanceReader = await this.db.prepare('SELECT *, CollectionTime * 1000 AS UtcTimestamp FROM Balances ORDER BY RecordTimestamp ASC');
        try {
            let nextFill = await fillsReader.get();
            let nextDeposit = await depositReader.get();
            let nextWithdrawal = await withdrawalReader.get();
            let nextBalance = await balanceReader.get();
            while (nextFill || nextDeposit || nextWithdrawal || nextBalance) {
                if (lowerTimestamp(nextFill, [nextDeposit, nextWithdrawal, nextBalance])) {
                    nextFill.RecordType = Database.RECORD_TYPE_FILL;
                    await callback(nextFill);
                    nextFill = await fillsReader.get();
                } else if (lowerTimestamp(nextDeposit, [nextFill, nextWithdrawal, nextBalance])) {
                    nextDeposit.RecordType = Database.RECORD_TYPE_DEPOSIT;
                    await callback(nextDeposit);
                    nextDeposit = await depositReader.get();
                } else if (lowerTimestamp(nextWithdrawal, [nextFill, nextDeposit, nextBalance])) {
                    nextWithdrawal.RecordType = Database.RECORD_TYPE_WITHDRAWAL;
                    await callback(nextWithdrawal);
                    nextWithdrawal = await withdrawalReader.get();
                } else {
                    nextBalance.RecordType = Database.RECORD_TYPE_BALANCE;
                    await callback(nextBalance);
                    nextBalance = await balanceReader.get();
                }
            }
        } finally {
            await fillsReader.finalize();
            await depositReader.finalize();
            await withdrawalReader.finalize();
            await balanceReader.finalize();
        }

        return Promise.resolve();
    }

    async getMostRecentFillId(symbol) {
        symbol = normalizeAssetCase(symbol);
        const query = 'SELECT Id FROM Fills WHERE Symbol = $symbol ORDER BY Id DESC LIMIT 1';
        const row = await this.db.get(query, { $symbol: symbol });
        return row ? row.Id : 0;
    }

    async getMostRecentDepositTime() {
        const query = 'SELECT UtcTimestamp FROM Deposits ORDER BY UtcTimestamp DESC LIMIT 1';
        const row = await this.db.get(query);
        return row ? row.UtcTimestamp : 0;
    }

    async getMostRecentWithdrawalTime() {
        const query = 'SELECT UtcTimestamp FROM Withdrawals ORDER BY UtcTimestamp DESC LIMIT 1';
        const row = await this.db.get(query);
        return row ? row.UtcTimestamp : 0;
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
            $isBestMatch: isBestMatch,
        });
    }

    async logDeposit(utcTimestamp, asset, amount, status) {
        asset = normalizeAssetCase(asset);
        const query = 'INSERT OR REPLACE INTO Deposits VALUES ( $utcTimestamp, $asset, $amount, $status )';
        await this.db.run(query, {
            $utcTimestamp: utcTimestamp,
            $asset: asset,
            $amount: amount,
            $status: status,
        });
    }

    async logWithdrawal(utcTimestamp, asset, amount, address, status) {
        asset = normalizeAssetCase(asset);
        const query = 'INSERT OR REPLACE INTO Withdrawals VALUES ( $utcTimestamp, $asset, $amount, $address, $status )';
        await this.db.run(query, {
            $utcTimestamp: utcTimestamp,
            $asset: asset,
            $amount: amount,
            $address: address,
            $status: status,
        });
    }

    async logBalanceSnapshot(asOf, recordTimestamp, balanceRecords) {
        for (let i = 0; i < balanceRecords.length; i++) {
            const balanceRecord = balanceRecords[i];
            const asset = normalizeAssetCase(balanceRecord.asset);
            const free = balanceRecord.free;
            const locked = balanceRecord.locked;
            const query = 'INSERT OR REPLACE INTO Balances VALUES ( $recordTimestamp, $collectionTime, $asset, $free, $locked )';
            await this.db.run(query, {
                $recordTimestamp: recordTimestamp,
                $collectionTime: asOf,
                $asset: asset,
                $free: free,
                $locked: locked,
            });
        }
    }
};

module.exports = Database;