const Database = require('./database');

const adjustBalance = (balances, symbol, amount, isDebit) => {
    if (!balances[symbol]) {
        balances[symbol] = 0.0;
    }

    balances[symbol] += amount * (isDebit ? -1 : 1);
};

class Aggregator {

    constructor(db) {
        this.db = db;
    }

    async go() {
        const balances = {};
        await this.db.forEachRecord(record => {
            switch (record.RecordType) {
                case Database.RECORD_TYPE_FILL:
                    adjustBalance(balances, record.BaseAsset, record.Quantity, !record.IsBuyer);
                    adjustBalance(balances, record.QuoteAsset, record.Price * record.Quantity, record.IsBuyer);
                    adjustBalance(balances, record.CommissionAsset, record.Commission, true);
                    console.log(
                        '%d - Fill: %s%f %s %s%f %s -%f %s',
                        record.UtcTimestamp,
                        record.IsBuyer ? '+' : '-',
                        record.Quantity,
                        record.BaseAsset,
                        record.IsBuyer ? '-' : '+',
                        record.Price * record.Quantity,
                        record.QuoteAsset,
                        record.Commission,
                        record.CommissionAsset);
                    break;
                case Database.RECORD_TYPE_DEPOSIT:
                    if (record.Status != 0) {
                        adjustBalance(balances, record.Asset, record.Amount, false);
                        console.log('%d - Deposit: %s %f', record.UtcTimestamp, record.Asset, record.Amount);
                    }
                    break;
                case Database.RECORD_TYPE_WITHDRAWAL:
                    if (record.Status == 6) {
                        adjustBalance(balances, record.Asset, record.Amount, true);
                        console.log('%d - Withdrawal: %s %f', record.UtcTimestamp, record.Asset, record.Amount * -1);
                    }
                    break;
                case Database.RECORD_TYPE_BALANCE:
                    const calculatedBalance = balances[record.Asset] || 0;
                    const actualBalance = parseFloat(record.Free) + parseFloat(record.Locked);
                    if (calculatedBalance != actualBalance) {
                        balances[record.Asset] = actualBalance;
                        console.log('%d - Adjustment: %s %s', record.UtcTimestamp, record.Asset, (actualBalance - calculatedBalance).toFixed(16));
                    }
                    break;
                default:
                    break;
            }
        });
    };

}

module.exports = Aggregator;