const BigNumber = require('bignumber.js');

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

    let totalSpend = new BigNumber(0.0);
    aggregateEvent.commissionValue = new BigNumber(0.0);
    aggregateEvent.commissionCost = new BigNumber(0.0);
    aggregateEvent.value = new BigNumber(0.0);
    aggregateEvent.quantity = new BigNumber(0.0);
    for (let i = 0; i < fillBuffer.length; i++) {
        totalSpend = totalSpend.plus(fillBuffer[i].price.multipliedBy(fillBuffer[i].quantity));
        aggregateEvent.commissionValue = aggregateEvent.commissionValue.plus(fillBuffer[i].commissionValue);
        aggregateEvent.commissionCost = aggregateEvent.commissionCost.plus(fillBuffer[i].commissionCost);
        aggregateEvent.value = aggregateEvent.value.plus(fillBuffer[i].value);
        aggregateEvent.quantity = aggregateEvent.quantity.plus(fillBuffer[i].quantity);
    }

    aggregateEvent.price = totalSpend.dividedBy(aggregateEvent.quantity);

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