var Statement = function() {

    var statementPages = {};
    var anyStatementPages = false;
    var activeMonth = null;
    var rendererPointers = {};
    var dateFormatter = new Intl.DateTimeFormat('default', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit', });
    var monthChartOptions = {
        scales: {
            yAxes: [{
                stacked: true
            }],
            xAxes: [{
                type: 'time',
                time: {
                    unit: 'day'
                }
            }]
        }
    };

    this.allEvents = {};
    this.isLoading = true;

    var quantityString = function(quantity, asset) {
        var precision = assetPrecisions.asQuantity[asset]; // assetPrecisions is an injected global variable
        (precision === undefined) && (precision = 8);
        return isNaN(parseFloat(quantity)) ? '' : parseFloat(quantity).toFixed(precision) + ' ' + asset.toUpperCase();
    };

    var priceString = function(quantity, asset) {
        var precision = assetPrecisions.asPrice[asset]; // assetPrecisions is an injected global variable
        (precision === undefined) && (precision = 8);
        return isNaN(parseFloat(quantity)) ? '' : parseFloat(quantity).toFixed(precision) + ' ' + asset.toUpperCase();
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
            allMonthPages[pageId].page[(pageId == activeMonth) ? 'show' : 'hide']();
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

    var getElementsForMonth = function(statementPage, eventDate) {
        var year = eventDate.getFullYear();
        var month = eventDate.getMonth() + 1;
        var pageId = year + '-' + month;
        if (!statementPage.monthPages[pageId]) {
            var monthName = eventDate.toLocaleString('default', { month: 'long' });
            var monthNameShort = eventDate.toLocaleString('default', { month: 'short' });
            var pageArea = $($('#bs-month-page-template').html());
            var chartArea = pageArea.find('.bs-month-chart');
            var chart = new Chart(chartArea.find('canvas'), {
                type: 'line',
                data: { datasets: [] },
                options: monthChartOptions,
            });
            pageArea.find('.bs-title').text(monthName + ' ' + year);
            statementPage.find('.bs-month-pages').append(pageArea);
            !statementPage.anyMonthPages || pageArea.hide();
            var switcherLink = $($('#bs-month-switch-template').html());
            switcherLink.find('a').addClass('bs-month bs-' + pageId).text(monthNameShort + ' ' + year).click(() => {
                switchMonth(statementPage.monthPages, pageId);
            });
            !statementPage.anyMonthPages && switcherLink.find('a').addClass('active');
            statementPage.anyMonthPages = true;
            statementPage.find('.bs-month-selector').append(switcherLink);
            statementPage.monthPages[pageId] = { page: pageArea, table: pageArea.find('tbody'), chart: chart };
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

    var populateMainRow = function(row, event, eventDate, unitOfAccount) {
        row.find('.bs-date').text(dateFormatter.format(eventDate));
        row.find('.bs-action .bs-activity').text(activityDescriptions[event.eventType]);
        event.asset && row.find('.bs-action .bs-amount').text(priceString(event.amount, event.asset));
        event.baseAsset && row.find('.bs-action .bs-amount').text(quantityString(event.quantity, event.baseAsset));
        row.find('.bs-value').text(priceString(event.value, unitOfAccount));
        if (event.eventType.indexOf('_AGGREGATION') !== -1) {
            row.find('.bs-sub-action .bs-activity').text(activitySubDescriptions[event.eventType]);
            row.find('.bs-sub-action .bs-amount').text(priceString(event.price * event.quantity, event.quoteAsset));
        } else {
            row.find('.bs-sub-action').hide();
        }
    }

    var findDataSetForAsset = function(eventTime, datasets, asset) {
        var earlierPointsPresent = false;
        for (var i = 0; i < datasets.length; i++) {
            var dataset = datasets[i];
            earlierPointsPresent = earlierPointsPresent || (dataset.data.length > 1);
            if (dataset.label === asset) {
                return dataset;
            }
        }

        var dataset = {
            label: asset,
            data: [],
            pointRadius: 0,
            fill: true,
        };
        earlierPointsPresent && dataset.data.push({ t: eventTime - 1, y: 0 });
        datasets.push(dataset);
        return dataset;
    }

    var addValuationToChart = function(eventTime, valuationComposition, chart) {
        for (var asset in valuationComposition) {
            var valuation = valuationComposition[asset];
            findDataSetForAsset(eventTime, chart.data.datasets, asset).data.push({ t: eventTime, y: valuation });
        }

        for (var i = 0; i < chart.data.datasets.length; i++) {
            var dataset = chart.data.datasets[i];
            if (!valuationComposition[dataset.label]) {
                dataset.data.push({ t: eventTime, y: 0 });
            }
        }
    };

    var renderEvent = function(unitOfAccount, event) {
        var statementPage = getStatementPageForUnitOfAccount(unitOfAccount);
        var eventDate = new Date(event.utcTimestamp);
        var monthElements = getElementsForMonth(statementPage, eventDate);
        addValuationToChart(event.utcTimestamp, event.valuationComposition, monthElements.chart);
        if (event.eventType != 'EVENT_TYPE_SNAPSHOT') {
            var tableBody = monthElements.table;
            var row = $('#bs-activity-row-template').clone();
            populateMainRow(row, event, eventDate, unitOfAccount);
            tableBody.append(row);
        }
    };

    this.renderNewEvents = function() {
        for (var unitOfAccount in this.allEvents) {
            var events = this.allEvents[unitOfAccount];
            var pointer = rendererPointers[unitOfAccount] || 0;
            while (pointer < events.length) {
                renderEvent(unitOfAccount, events[pointer]);
                pointer++;
            }

            rendererPointers[unitOfAccount] = pointer;
        }

        for (var i = 0; i < Chart.instances.length; i++) {
            Chart.instances[i].update();
        }
    };

    this.pushEvent = function(unitOfAccount, event) {
        this.allEvents[unitOfAccount] = this.allEvents[unitOfAccount] || [];
        this.allEvents[unitOfAccount].push(event);
    };

    this.loadingComplete = function() {
        this.isLoading = false;
        this.renderNewEvents();
    };
};

var statement = new Statement();

var RENDER_INTERVAL = 500;

var onTimer = function() {
    statement.renderNewEvents();
    if (statement.isLoading) {
        setTimeout(onTimer, RENDER_INTERVAL);
    }
};

setTimeout(onTimer, RENDER_INTERVAL);