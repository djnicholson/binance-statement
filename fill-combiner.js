const Aggregator = require('./aggregator');

const emitBufferContents = async(fillBuffer, callback) => {

    if (fillBuffer.length == 0) {
        return;
    }

    const aggregateEventType = (fillBuffer[0].eventType == Aggregator.EVENT_TYPE_BUY) ?
        Aggregator.EVENT_TYPE_BUY_AGGREGATION :
        Aggregator.EVENT_TYPE_SELL_AGGREGATION;
    const aggregateEvent = new Aggregator.Event(
        fillBuffer[fillBuffer.length - 1].utcTimestamp,
        aggregateEventType);

    aggregateEvent.baseAsset = fillBuffer[0].baseAsset;
    aggregateEvent.quoteAsset = fillBuffer[0].quoteAsset;
    aggregateEvent.market = fillBuffer[0].market;
    aggregateEvent.orderId = fillBuffer[0].orderId;

    let totalSpend = 0;
    aggregateEvent.commissionValue = 0;
    aggregateEvent.value = 0;
    aggregateEvent.quantity = 0;
    for (let i = 0; i < fillBuffer.length; i++) {
        totalSpend += fillBuffer[i].price * fillBuffer[i].quantity;
        aggregateEvent.commissionValue += fillBuffer[i].commissionValue;
        aggregateEvent.value += fillBuffer[i].value;
        aggregateEvent.quantity += fillBuffer[i].quantity;
    }

    aggregateEvent.price = totalSpend / aggregateEvent.quantity;

    aggregateEvent.fills = fillBuffer;

    aggregateEvent.totalPortfolioValue = fillBuffer[fillBuffer.length - 1].totalPortfolioValue;
    aggregateEvent.valuationComposition = fillBuffer[fillBuffer.length - 1].valuationComposition;

    await callback(aggregateEvent);

    fillBuffer.length = 0; // empty the buffer
};

class FillCombiner {

    constructor(aggregator) {
        this.aggregator = aggregator;
    }

    async enumerateEvents(callback) {

        const fillBuffer = [];

        await this.aggregator.enumerateEvents(async event => {
            const isFillEvent = (event.eventType == Aggregator.EVENT_TYPE_BUY) || (event.eventType == Aggregator.EVENT_TYPE_SELL);
            if (isFillEvent) {

                const isStartOfNewOrder = (fillBuffer.length > 0) && (fillBuffer[0].orderId != event.orderId);
                if (isStartOfNewOrder) {
                    await emitBufferContents(fillBuffer, callback);
                }

                fillBuffer.push(event);

            } else {

                await emitBufferContents(fillBuffer, callback);
                await callback(event);

            }
        });
    };
}

module.exports = FillCombiner;