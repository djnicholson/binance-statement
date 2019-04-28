var Statement = function() {

    var allEvents = {};
    var statementPages = {};
    var anyStatementPages = false;
    var activeMonth = null;

    this.isLoading = true;

    var quantityString = function(quantity, asset) {
        var precision = assetPrecisions.asQuantity[asset]; // assetPrecisions is an injected global variable
        (precision === undefined) && (precision = 8);
        return parseFloat(quantity).toFixed(precision) + ' ' + asset.toUpperCase();
    };

    var priceString = function(quantity, asset) {
        var precision = assetPrecisions.asPrice[asset]; // assetPrecisions is an injected global variable
        (precision === undefined) && (precision = 8);
        return parseFloat(quantity).toFixed(precision) + ' ' + asset.toUpperCase();
    };

    var switchUnit = function(unitOfAccount) {
        $('.bs-statement').hide();
        statementPages[unitOfAccount].show();
        activeMonth && switchMonth(statementPages[unitOfAccount].monthPages, activeMonth);
    };

    var switchMonth = function(allMonthPages, pageIdToShow) {
        activeMonth = pageIdToShow;
        $('a.bs-month').removeClass('active');
        $('a.bs-' + activeMonth).addClass('active');
        for (var pageId in allMonthPages) {
            allMonthPages[pageId][(pageId == activeMonth) ? 'show' : 'hide']();
        }
    };

    var getStatementPageForUnitOfAccount = function(unitOfAccount) {
        if (!statementPages[unitOfAccount]) {
            var switcherLink = $($('#bs-unit-switch-template').html());
            switcherLink.find('a').text(unitOfAccount).click(() => {
                switchUnit(unitOfAccount);
                switcherLink.parent().find('a').removeClass('active');
                switcherLink.find('a').addClass('active');
            });
            !anyStatementPages && switcherLink.find('a').addClass('active');
            $('#bs-unit-selector').removeClass('d-none');
            $('#bs-unit-selector').append(switcherLink);
            statementPages[unitOfAccount] = $($('#bs-statement-template').html());
            statementPages[unitOfAccount].monthPages = {};
            statementPages[unitOfAccount].anyMonthPages = false;
            $('#bs-statements').append(statementPages[unitOfAccount]);
            !anyStatementPages || statementPages[unitOfAccount].hide();
            anyStatementPages = true;
        }

        return statementPages[unitOfAccount];
    };

    var getPageForMonth = function(statementPage, eventDate) {
        var year = eventDate.getFullYear();
        var month = eventDate.getMonth() + 1;
        var pageId = year + '-' + month;
        if (!statementPage.monthPages[pageId]) {
            var monthName = eventDate.toLocaleString('default', { month: 'long' });
            var monthNameShort = eventDate.toLocaleString('default', { month: 'short' });
            statementPage.monthPages[pageId] = $($('#bs-month-page-template').html());
            statementPage.monthPages[pageId].find('.bs-title').text(monthName + ' ' + year);
            statementPage.find('.bs-month-pages').append(statementPage.monthPages[pageId]);
            !statementPage.anyMonthPages || statementPage.monthPages[pageId].hide();
            var switcherLink = $($('#bs-month-switch-template').html());
            switcherLink.find('a').addClass('bs-month bs-' + pageId).text(monthNameShort + ' ' + year).click(() => {
                switchMonth(statementPage.monthPages, pageId);
            });
            !statementPage.anyMonthPages && switcherLink.find('a').addClass('active');
            statementPage.anyMonthPages = true;
            statementPage.find('.bs-month-selector').append(switcherLink);
        }

        return statementPage.monthPages[pageId];
    };

    var activityDescriptions = {
        'EVENT_TYPE_BUY_AGGREGATION': 'Bought',
        'EVENT_TYPE_SELL_AGGREGATION': 'Sold',
        'EVENT_TYPE_DEPOSIT': 'Deposited',
        'EVENT_TYPE_WITHDRAWAL': 'Withdrew',
        'EVENT_TYPE_BINANCE_CREDIT': 'Account credited',
        'EVENT_TYPE_BINANCE_DEBIT': 'Account debited',
    };

    var activitySubDescriptions = {
        'EVENT_TYPE_BUY_AGGREGATION': 'using',
        'EVENT_TYPE_SELL_AGGREGATION': 'for',
    };

    var populateMainRow = function(row, event, eventDate) {
        row.find('.bs-date').text(eventDate.toLocaleString('default', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit', }));
        row.find('.bs-action .bs-activity').text(activityDescriptions[event.eventType]);
        event.asset && row.find('.bs-action .bs-amount').text(priceString(event.amount, event.asset));
        event.baseAsset && row.find('.bs-action .bs-amount').text(quantityString(event.quantity, event.baseAsset));
        if (event.eventType.indexOf('_AGGREGATION') !== -1) {
            row.find('.bs-sub-action .bs-activity').text(activitySubDescriptions[event.eventType]);
            row.find('.bs-sub-action .bs-amount').text(priceString(event.price * event.quantity, event.quoteAsset));
        } else {
            row.find('.bs-sub-action').hide();
        }
    }

    var renderEvent = function(unitOfAccount, event) {
        var statementPage = getStatementPageForUnitOfAccount(unitOfAccount);
        var eventDate = new Date(event.utcTimestamp);
        var monthPage = getPageForMonth(statementPage, eventDate);
        if (event.eventType != 'EVENT_TYPE_SNAPSHOT') {
            var tableBody = monthPage.find('tbody');
            var row = $($('#bs-activity-row-template').html());
            populateMainRow(row, event, eventDate);
            tableBody.append(row);
        }

        event.rendered = true;
    };

    this.renderNewEvents = function() {
        for (var unitOfAccount in allEvents) {
            var events = allEvents[unitOfAccount];
            for (var i = 0; i < events.length; i++) {
                var event = events[i];
                event.rendered || renderEvent(unitOfAccount, event);
            }
        }
    };

    this.pushEvent = function(unitOfAccount, event) {
        allEvents[unitOfAccount] = allEvents[unitOfAccount] || [];
        allEvents[unitOfAccount].push(event);
    };

    this.loadingComplete = function() {
        this.isLoading = false;
    };

};

var statement = new Statement();

var RENDER_INTERVAL = 1500;
var FIRST_RENDER = 500;

var onTimer = function() {
    statement.renderNewEvents();
    if (statement.isLoading) {
        setTimeout(onTimer, RENDER_INTERVAL);
    }
};

setTimeout(onTimer, FIRST_RENDER);