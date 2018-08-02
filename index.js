const fs = require('fs');
const parse = require('csv-parse');
const glob = require('glob');

const SOURCE_EMAILS = 'master-email-recipient.csv';
const INPUT_FOLDER = 'input-csv';
const OUTPUT_EMAIL_FILE = 'email-counts.csv';
const OUTPUT_COUNT_FILE = 'open-counts.csv';

const PARSE_OPTS = {quote: '"', ltrim: true, rtrim: true, delimiter: ','};
const EMAIL_COL_INDEX = 1;

let fileCount = 0;
let emailCount = {};
let openCount = {};

glob(INPUT_FOLDER + '/*.csv', function (err, files) {
    if (err) {
        throw err;
    }

    if (fs.existsSync(OUTPUT_EMAIL_FILE)) {
        fs.unlinkSync(OUTPUT_EMAIL_FILE);
    }

    if (fs.existsSync(OUTPUT_COUNT_FILE)) {
        fs.unlinkSync(OUTPUT_COUNT_FILE);
    }

    fs.createReadStream(SOURCE_EMAILS)
        .pipe(parse(PARSE_OPTS))
        .on('data', function (csvRow) {
            let emailAddress = csvRow[EMAIL_COL_INDEX].toLowerCase().replace(/\s/, '');

            if (emailCount[emailAddress] === undefined) {
                emailCount[emailAddress] = 0;
            }
        })
        .on('error', function (err) {
            console.error(err);
        })
        .on('end', function () {
            // parse inputs CSVs

            files.forEach(function (file) {
                if (file.indexOf('.csv') === -1) {
                    return;
                }

                fs.createReadStream(file)
                    .pipe(parse(PARSE_OPTS))
                    .on('data', function (csvRow) {

                        //console.log(csvRow);

                        if (csvRow[EMAIL_COL_INDEX]) {
                            let emailAddress = csvRow[EMAIL_COL_INDEX].toLowerCase().replace(/\s/, '');

                            if (emailCount[emailAddress] !== undefined) {
                                emailCount[emailAddress]++;
                            }
                        }
                    })
                    .on('error', function (err) {
                        console.error(err);
                    })
                    .on('end', function () {
                        fileCount++;

                        if (fileCount === files.length) {
                            outputEmailCounts();
                        }
                    });
            });
        });
});

function outputEmailCounts() {
    for (let entry in emailCount) {
        let count = emailCount[entry];

        writeLine(OUTPUT_EMAIL_FILE, entry, count);

        if (openCount[count] === undefined) {
            openCount[count] = 1;
        } else {
            openCount[count]++;
        }
    }

    outputOpenCounts();
}

function outputOpenCounts() {
    fs.appendFileSync(OUTPUT_COUNT_FILE, '"Open Count","Occurrences"\n');
    for (let count in openCount) {
        let occurrences = openCount[count];

        writeLine(OUTPUT_COUNT_FILE, count, occurrences);
    }
}

function writeLine(file, ...args) {
    let row = '';

    args.forEach(function (arg) {
        row += '"' + arg + '",';
    });

    row = row.substr(0, row.length - 1);
    row += '\n';

    fs.appendFileSync(file, row);
}
