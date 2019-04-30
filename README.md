# binance-statement

A command-line tool and library to create pretty financial statements for an arbitrary Binance account.

## Usage

A command-line tool and an API are provided.  

In both cases a standalone HTML file is generated that can be used to view the statement in a web browser.

### Command line

At a command prompt:

    npm install -g binance-statement
    binance-statement --help
    binance-statement --api-key "FOO" --api-key "BAR" --output-file "statement.html"
    open statement.html

### API

At a command prompt:

    npm install --save binance-statement

In a JavaScript file:

    const makeStatement = require('binance-statement');
    // ...
    await makeStatement(
        apiKey, 
        apiSecret, 
        startMonth, 
        startYear, 
        outputFile, 
        dataFile, 
        cacheFile, 
        syncFillsFromBinance, 
        speed, 
        unitsOfAccount);
    
## Tips

* Run the script about once per day
  * Certain transactions (e.g. selling dust, receiving affiliate commission, receiving dividends/airdrops) are
    only detected by polling of your account balance. Running the command regularly ensures these events have an
    accurate date on your statement.
* The first time may take a while
  * The first time you generate a statement, price history must be obtained for all assets you have held; this
    can take a while, especially if your account has a lot of historic activity. Price information is cached and
    subsequent invocations will be faster. You can speed up this process by specifying a more recent statement
    start date using the `--start-month` and `--start-year` command line parameters.
* Binance may throttle your API requests
  * If you get throttled, use the `--speed` comaand line parameter to add a delay between Binance calls.
