var Statement = function() {

    var statementPages = {};
    var anyStatementPages = false;
    var activeMonth = null;
    var rendererPointers = {};
    var allCharts = [];
    var allMonthSummaries = [];
    var dateFormatter = new Intl.DateTimeFormat('default', { year: 'numeric', day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit', });
    var monthChartLayout = {
        // margins:
        l: 0,
        r: 0,
        t: 0,
        b: 0,
        // animation:
        transition: { duration: 0 },
        // interactivity:
        clickmode: 'none',
        dragmode: 'none',
        xaxis: { type: 'date', tickformat: '%B %e', },
        yaxis: { visible: true, },
        grid: { yside: 'right plot' }, // not working?
    };
    var monthChartOptions = {
        responsive: true,
        displayModeBar: false,
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

    var getElementsForMonth = function(statementPage, eventDate, unitOfAccount) {
        var year = eventDate.getFullYear();
        var month = eventDate.getMonth() + 1;
        var pageId = year + '-' + month;
        if (!statementPage.monthPages[pageId]) {
            var monthName = eventDate.toLocaleString('default', { month: 'long' });
            var monthNameShort = eventDate.toLocaleString('default', { month: 'short' });
            var pageArea = $($('#bs-month-page-template').html());
            var chartArea = pageArea.find('.bs-month-chart');
            var chartData = [];
            var monthSummary = { openingValue: 0, grossProfit: 0, totalCommission: 0, deposits: 0, adjustments: 0, closingValue: 0 };
            allCharts.push({ chartArea: chartArea, chartData: chartData, plotted: false });
            allMonthSummaries.push({ unitOfAccount: unitOfAccount, summaryArea: pageArea.find('.bs-month-summary'), monthSummary: monthSummary });
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
            statementPage.monthPages[pageId] = {
                page: pageArea,
                table: pageArea.find('tbody'),
                chartData: chartData,
                monthSummary: monthSummary,
            };
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

    var renderCommissionTable = function(contentArea, event, unitOfAccount, profitLossEntries, monthSummary) {
        var table = $('#bs-commission-table-template').clone().removeAttr('id');
        contentArea.append(table);
        var totalValue = 0.0;
        for (var i = 0; i < event.fills.length; i++) {
            var fill = event.fills[i];
            totalValue += parseFloat(fill.commissionValue);
            var row = $('#bs-commission-row-template').clone().removeAttr('id');
            row.find('.bs-fill-time').text(dateFormatter.format(new Date(fill.utcTimestamp)));
            row.find('.bs-fill-detail').text(priceString(fill.quantity, fill.baseAsset) + ' for ' + priceString(fill.quantity * fill.price, fill.quoteAsset));
            row.find('.bs-commission').text(priceString(fill.commission, fill.commissionAsset));
            row.find('.bs-method').text(fill.commissionDebitedFromProceeds ? 'Proceeds debit' : 'Asset liquidation');
            row.find('.bs-value').text(priceString(fill.commissionValue, unitOfAccount));
            table.find('tbody').append(row);
        }

        table.find('tfoot .bs-value').text(priceString(totalValue, unitOfAccount));
        monthSummary.totalCommission += (-1 * totalValue);
        profitLossEntries.push([
            'Commission paid', -1 * totalValue
        ]);
    };

    var renderProfitTable = function(contentArea, event, unitOfAccount, profitLossEntries) {
        var table = $('#bs-profit-table-template').clone().removeAttr('id');
        contentArea.append(table);
        var netProfit = 0.0;
        for (var i = 0; i < profitLossEntries.length; i++) {
            var entry = profitLossEntries[i];
            netProfit += entry[1];
            var row = $('#bs-profit-row-template').clone().removeAttr('id');
            row.find('.bs-description').text(entry[0]);
            row.find('.bs-value').text(priceString(entry[1], unitOfAccount));
            table.find('tbody').append(row);
        }

        table.find('tfoot .bs-total').text(priceString(netProfit, unitOfAccount));
    };

    var renderLotsTable = function(contentArea, asset, lots, unitOfAccount, event, profitLossEntries, monthSummary, currentValue) {
        var table = $('#bs-lot-table-template').clone().removeAttr('id');
        contentArea.append(table);
        table.find('.bs-asset').text(asset);
        var totalCost = 0.0;
        for (var i = 0; i < lots.length; i++) {
            var lot = lots[i];
            var costBasis = lot.costBasisPrice * lot.quantity;
            totalCost += costBasis;
            var row = $('#bs-lot-row-template').clone().removeAttr('id');
            row.find('.bs-lot').text(priceString(lot.quantity, asset) + ' ' + lot.sourceDescription);
            lot.utcTimestamp && row.find('.bs-purchase-time').text(dateFormatter.format(new Date(lot.utcTimestamp)));
            row.find('.bs-cost-basis').text(priceString(costBasis, unitOfAccount));
            table.find('tbody').append(row);
        }

        table.find('tfoot .bs-cost-basis').text(priceString(totalCost, unitOfAccount));
        table.find('tfoot .bs-value').text(priceString(currentValue, unitOfAccount));
        table.find('tfoot .bs-gross-profit').text(priceString(currentValue - totalCost, unitOfAccount));
        monthSummary.grossProfit += (currentValue - totalCost);
        profitLossEntries.push([
            'Realizing ' + (event.value > totalCost ? 'profit' : 'loss') + ' from sale of ' + asset,
            currentValue - totalCost
        ]);
    };

    var renderLotsTables = function(contentArea, event, unitOfAccount, profitLossEntries, monthSummary) {
        var allLots = event.lots || [];
        event.fills && event.fills.forEach(f => f.lots && (allLots = allLots.concat(f.lots)));
        if (allLots.length > 0) {
            var lotsByAsset = {};
            for (var i = 0; i < allLots.length; i++) {
                var lot = allLots[i];
                lotsByAsset[lot.asset] = lotsByAsset[lot.asset] || [];
                lotsByAsset[lot.asset].push(lot);
            }

            for (var asset in lotsByAsset) {
                renderLotsTable(
                    contentArea,
                    asset,
                    lotsByAsset[asset],
                    unitOfAccount,
                    event,
                    profitLossEntries,
                    monthSummary,
                    (asset == event.baseAsset) || (asset == event.asset) ? event.value : event.commissionValue);
            }
        }
    };

    var maybePopulateSecondRow = function(row, event, unitOfAccount, monthSummary) {
        if (event.eventType.indexOf('DEPOSIT') !== -1) {
            monthSummary.deposits += parseFloat(event.value);
        } else if (event.eventType.indexOf('WITHDRAW') !== -1) {
            monthSummary.deposits += (-1 * parseFloat(event.value));
        } else if (event.eventType.indexOf('CREDIT') !== -1) {
            monthSummary.adjustments += parseFloat(event.value);
        } else if (event.eventType.indexOf('DEBIT') !== -1) {
            monthSummary.adjustments += (-1 * parseFloat(event.value));
        }

        if ((event.eventType.indexOf('DEPOSIT') !== -1) || event.eventType.indexOf('CREDIT') !== -1) {
            return false;
        } else {
            var contentArea = row.find('.bs-content');
            var profitLossEntries = [];
            event.fills && event.fills.length && renderCommissionTable(contentArea, event, unitOfAccount, profitLossEntries, monthSummary);
            renderLotsTables(contentArea, event, unitOfAccount, profitLossEntries, monthSummary);
            profitLossEntries.reverse();
            renderProfitTable(contentArea, event, unitOfAccount, profitLossEntries);
            return true;
        }
    }

    var findTraceForAsset = function(datasets, asset) {
        for (var i = 0; i < datasets.length; i++) {
            var dataset = datasets[i];
            if (dataset.name === asset) {
                return dataset;
            }
        }

        var dataset = {
            name: asset,
            x: [],
            y: [],
            stackgroup: 'one',
            line: {
                color: assetColors[asset.toUpperCase()] || assetColors['GENERIC'],
            },
        };

        datasets.push(dataset);
        return dataset;
    }

    var addValuationToChart = function(eventTime, valuationComposition, chartData) {
        for (var asset in valuationComposition) {
            var valuation = valuationComposition[asset];
            var trace = findTraceForAsset(chartData, asset);
            trace.x.push(eventTime);
            trace.y.push(valuation);
        }
    };

    var renderEvent = function(unitOfAccount, event) {
        var statementPage = getStatementPageForUnitOfAccount(unitOfAccount);
        var eventDate = new Date(event.utcTimestamp);
        var monthElements = getElementsForMonth(statementPage, eventDate, unitOfAccount);
        addValuationToChart(event.utcTimestamp, event.valuationComposition, monthElements.chartData);
        if (event.eventType != 'EVENT_TYPE_SNAPSHOT') {
            var tableBody = monthElements.table;
            var row = $('#bs-activity-row-template').clone().removeAttr('id');
            var secondRow = $('#bs-activity-detail-row-template').clone().removeAttr('id');
            populateMainRow(row, event, eventDate, unitOfAccount);
            tableBody.append(row);
            row.addClass('clickable').click(() => {
                $('.bs-activity-row.bs-selected').removeClass('bs-selected');
                var wasVisible = secondRow && secondRow.is(':visible');
                $('.bs-activity-detail-row').hide();
                !wasVisible && secondRow && secondRow.show();
                row.addClass('bs-selected');
            });
            monthElements.monthSummary.closingValue = event.totalPortfolioValue;
            monthElements.monthSummary.openingValue = monthElements.monthSummary.openingValue || event.totalPortfolioValue;
            if (maybePopulateSecondRow(secondRow, event, unitOfAccount, monthElements.monthSummary)) {
                tableBody.append(secondRow);
                secondRow.hide();
            } else {
                secondRow = null;
            }
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

        for (var i = 0; i < allCharts.length; i++) {
            if (allCharts[i].plotted) {
                Plotly.restyle(allCharts[i].chartArea[0], allCharts[i].chartData);
            } else {
                Plotly.newPlot(allCharts[i].chartArea[0], allCharts[i].chartData, monthChartLayout, monthChartOptions);
            }
        }

        for (var i = 0; i < allMonthSummaries.length; i++) {
            var summaryArea = allMonthSummaries[i].summaryArea;
            var monthSummary = allMonthSummaries[i].monthSummary;
            var unitOfAccount = allMonthSummaries[i].unitOfAccount;
            var change = monthSummary.closingValue - monthSummary.openingValue - monthSummary.grossProfit - monthSummary.totalCommission - monthSummary.deposits - monthSummary.adjustments;
            summaryArea.find('.bs-open').text(priceString(monthSummary.openingValue, unitOfAccount));
            summaryArea.find('.bs-gross-profit').text(priceString(monthSummary.grossProfit, unitOfAccount));
            summaryArea.find('.bs-commission').text(priceString(monthSummary.totalCommission, unitOfAccount));
            summaryArea.find('.bs-change').text(priceString(change, unitOfAccount));
            summaryArea.find('.bs-deposits').text(priceString(monthSummary.deposits, unitOfAccount));
            summaryArea.find('.bs-adjustments').text(priceString(monthSummary.adjustments, unitOfAccount));
            summaryArea.find('.bs-close').text(priceString(monthSummary.closingValue, unitOfAccount));
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