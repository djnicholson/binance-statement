const SqliteDatabase = require('sqlite-async');

const initializeSchema = async(db) => {
    await db.run('CREATE TABLE IF NOT EXISTS Prices (UtcTimestamp, BaseAsset, QuoteAsset, Price)');
    await db.run('CREATE UNIQUE INDEX IF NOT EXISTS OnePricePerSymbolPerTimestamp ON Prices (UtcTimestamp, BaseAsset, QuoteAsset)');

    await db.run('CREATE TABLE IF NOT EXISTS Candles (UtcTimestamp, Symbol, Interval, OpenTime, CloseTime, Open, High, Low, Close, QuoteVolume, BaseVolume, Trades)');
    await db.run('CREATE UNIQUE INDEX IF NOT EXISTS OneCandlePerSymbolPerTimestampPerInterval ON Candles (UtcTimestamp, Symbol, Interval)');
};

const normalizeAssetCase = asset => asset.toUpperCase();

class PriceCache {

    static async create(dataFile, binance, preBinanceCallback) {
        const db = await SqliteDatabase.open(dataFile);
        await initializeSchema(db);
        return new PriceCache(db, binance, preBinanceCallback);
    }

    constructor(db, binance, preBinanceCallback) {
        this.db = db;
        this.binance = binance;
        this.preBinanceCallback = preBinanceCallback;
    }

    static get INTERVAL_1_MINUTE() { return '1m'; }
    static get INTERVAL_3_MINUTE() { return '3m'; }
    static get INTERVAL_5_MINUTE() { return '5m'; }
    static get INTERVAL_15_MINUTE() { return '15m'; }
    static get INTERVAL_30_MINUTE() { return '30m'; }
    static get INTERVAL_1_HOUR() { return '1h'; }
    static get INTERVAL_2_HOUR() { return '2h'; }
    static get INTERVAL_4_HOUR() { return '4h'; }
    static get INTERVAL_6_HOUR() { return '6h'; }
    static get INTERVAL_8_HOUR() { return '8h'; }
    static get INTERVAL_12_HOUR() { return '12h'; }
    static get INTERVAL_1_DAY() { return '1d'; }
    static get INTERVAL_3_DAY() { return '3d'; }
    static get INTERVAL_1_WEEK() { return '1w'; }
    static get INTERVAL_1_MONTH() { return '1M'; }

    async getPrice(utcTimestamp, baseAsset, quoteAsset, statusCallback, avoidIndirectCalculation) {
        baseAsset = normalizeAssetCase(baseAsset);
        quoteAsset = normalizeAssetCase(quoteAsset);

        // use the midpoint of the containing minute for result caching purposes:
        utcTimestamp = (Math.round(utcTimestamp / (1000 * 60)) * (1000 * 60) + (30 * 1000));

        if (baseAsset === quoteAsset) {
            return 1.0;
        }

        const selectQuery = 'SELECT * FROM Prices WHERE UtcTimestamp = $utcTimestamp AND BaseAsset = $baseAsset AND QuoteAsset = $quoteAsset LIMIT 1';
        const row = await this.db.get(selectQuery, { $utcTimestamp: utcTimestamp, $baseAsset: baseAsset, $quoteAsset: quoteAsset });
        if (row) {
            return row.Price || undefined;
        }

        let price = null; // a null result implies that the price is currently not known

        const directCandle = await this.getNearestCandle(baseAsset + quoteAsset, PriceCache.INTERVAL_1_MINUTE, utcTimestamp, statusCallback);
        if (directCandle) {
            price = (parseFloat(directCandle.High) + parseFloat(directCandle.Low)) / 2.0;
        } else if (directCandle === undefined) { // candle for this asset pair will never exist
            if (avoidIndirectCalculation) {
                return undefined;
            } else {
                price = await this.getPrice(utcTimestamp, quoteAsset, baseAsset, statusCallback, /*avoidIndirectCalculation*/ true);
                if (price === undefined) { // candle for inverted asset pair will never exist
                    const baseAssetBtcPrice = await this.getPrice(utcTimestamp, baseAsset, 'BTC', statusCallback, /*avoidIndirectCalculation*/ true);
                    const quoteAssetBtcPrice = await this.getPrice(utcTimestamp, quoteAsset, 'BTC', statusCallback, /*avoidIndirectCalculation*/ true);
                    if ((baseAssetBtcPrice === undefined) || (quoteAssetBtcPrice === undefined)) {
                        price = undefined; // price will never be known
                    } else if ((baseAssetBtcPrice === null) || (quoteAssetBtcPrice === null)) {
                        price = null; // come back later
                    } else {
                        price = baseAssetBtcPrice / quoteAssetBtcPrice;
                    }
                }
            }
        }

        if (price !== null) {
            const insertQuery = 'INSERT INTO Prices VALUES ($utcTimestamp, $baseAsset, $quoteAsset, $price)';
            await this.db.run(insertQuery, {
                $utcTimestamp: utcTimestamp,
                $baseAsset: baseAsset,
                $quoteAsset: quoteAsset,
                $price: price || null,
            });
        }

        return price;
    }

    async getNearestCandle(symbol, interval, utcTimestamp, statusCallback, useCacheOnly) {
        symbol = normalizeAssetCase(symbol);
        const selectQuery = 'SELECT * FROM Candles WHERE UtcTimestamp = $utcTimestamp AND Symbol = $symbol AND Interval = $interval LIMIT 1';
        const row = await this.db.get(selectQuery, { $utcTimestamp: utcTimestamp, $symbol: symbol, $interval: interval });
        if (row) {
            return row;
        }

        let candle = null; // a null result implies that the candle is currently not known

        if (useCacheOnly) {
            return candle;
        }

        try {
            await this.preBinanceCallback();
            const candles = await this.binance.candles({ symbol: symbol, interval: interval, endTime: utcTimestamp, limit: 1 });
            candles[0] && (candles[0].closeTime < (new Date).getTime()) && (candle = candles[0]);
            statusCallback && statusCallback('Retrieved ' + interval + ' candle for ' + symbol + ' enclosing time ' + utcTimestamp + '...');
        } catch (e) {
            candle = undefined; // an undefined result implies that the candle will never exist
            if ((e + '').toLowerCase().indexOf('invalid symbol') == -1) {
                throw e;
            }
        }

        if (candle) {
            const insertQuery = 'INSERT INTO Candles VALUES ($utcTimestamp, $symbol, $interval, $openTime, $closeTime, $open, $high, $low, $close, $quoteVolume, $baseVolume, $trades)';
            await this.db.run(insertQuery, {
                $utcTimestamp: utcTimestamp,
                $symbol: symbol,
                $interval: interval,
                $openTime: candle.openTime,
                $closeTime: candle.closeTime,
                $open: candle.open,
                $high: candle.high,
                $low: candle.low,
                $close: candle.close,
                $quoteVolume: candle.quoteAssetVolume,
                $baseVolume: candle.baseAssetVolume,
                $trades: candle.trades,
            });

            candle = await this.getNearestCandle(symbol, interval, utcTimestamp, statusCallback, /*useCacheOnly*/ true);
        }

        return candle;
    }

};

module.exports = PriceCache;