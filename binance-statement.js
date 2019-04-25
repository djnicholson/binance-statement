#!/usr/bin/env node

const colors = require('colors');
const program = require('commander');

const makeStatement = require('./make-statement');

program
    .version('1.0.0', '-v, --version')
    .description('Creates a financial statement for a Binance account')
    .option('-k, --api-key <key>', 'Binance API key')
    .option('-s, --api-secret <secret>', 'Binance API secret')
    .option('-o, --output-file <file>', 'Output HTML file')
    .option('-d, --data-file [file]', 'Data file')
    .option('-n, --no-fills-sync', 'Skip syncing fill data from Binance (takes a long time)')
    .option('-S --speed <n>', 'A number between 1 and 10 (10 is fastest). Too fast may cause Binance throttling.', parseInt, 9)
    .action(async(options) => {
        if (!options.apiKey) {
            program.outputHelp();
            console.error(colors.red('\nThe --api-key option is required\n'));
        } else if (!options.apiSecret) {
            program.outputHelp();
            console.error(colors.red('\nThe --api-secret option is required\n'));
        } else if (!options.outputFile) {
            program.outputHelp();
            console.error(colors.red('\nThe --output-file option is required\n'));
        } else {
            if (!options.fillsSync) {
                console.warn(colors.yellow('\nNot retriving fills from Binance; statement may be out of date.\n'));
            }

            await makeStatement(
                options.apiKey,
                options.apiSecret,
                options.outputFile,
                options.dataFile || (options.apiKey + ".db"),
                options.fillsSync,
                Math.max(1, Math.min(10, options.speed)));
        }
    });

program.parse(process.argv);