var Statement = function() {

    var allEvents = {};
    var statementPages = {};
    var anyStatementPages = false;
    var activeMonth = null;

    this.isLoading = true;

    var switchUnit = function(unitOfAccount) {
        $('.bs-statement').hide();
        statementPages[unitOfAccount].show();
        activeMonth && switchMonth(statementPages[unitOfAccount].monthPages, activeMonth);
    };

    var switchMonth = function(allMonthPages, pageIdToShow) {
        activeMonth = pageIdToShow;
        for (var pageId in allMonthPages) {
            allMonthPages[pageId][(pageId == pageIdToShow) ? 'show' : 'hide']();
        }
    };

    var getStatementPageForUnitOfAccount = function(unitOfAccount) {
        if (!statementPages[unitOfAccount]) {
            var switcherLink = $($('#bs-unit-switch-template').html());
            switcherLink.find('a').click(() => switchUnit(unitOfAccount)).text(unitOfAccount);
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

    var getPageForMonth = function(statementPage, year, month) {
        var pageId = year + '-' + month;
        if (!statementPage.monthPages[pageId]) {
            statementPage.monthPages[pageId] = $($('#bs-month-page-template').html());
            statementPage.find('.bs-month-pages').append(statementPage.monthPages[pageId]);
            !statementPage.anyMonthPages || statementPage.monthPages[pageId].hide();
            statementPage.anyMonthPages = true;
            var switcherLink = $($('#bs-month-switch-template').html());
            switcherLink.find('a').click(() => switchMonth(statementPage.monthPages, pageId)).text(pageId);
            statementPage.find('.bs-month-selector').append(switcherLink);
        }

        return statementPage.monthPages[pageId];
    };

    var renderEvent = function(unitOfAccount, event) {
        var statementPage = getStatementPageForUnitOfAccount(unitOfAccount);
        var eventDate = new Date(event.utcTimestamp);
        var monthPage = getPageForMonth(statementPage, eventDate.getFullYear(), eventDate.getMonth() + 1);
        if (event.eventType != 'EVENT_TYPE_SNAPSHOT') {
            var tableBody = monthPage.find('tbody');
            var row = $($('#bs-activity-row-template').html());
            row.find('.bs-date').text(eventDate.toString());
            row.find('.bs-activity').text(event.eventType);
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

var RENDER_INTERVAL = 1000;

var onTimer = function() {
    statement.renderNewEvents();
    if (statement.isLoading) {
        setTimeout(onTimer, RENDER_INTERVAL);
    }
};

onTimer();