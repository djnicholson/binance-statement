var fs = require('fs');

class HtmlWriter {

    constructor(outputFile) {
        this.file = fs.createWriteStream(outputFile);
    }

    consumeEvent(event) {
        this.file.write('statement.pushEvent(' + JSON.stringify(event) + ');\r\n');
    }

    end() {
        this.file.end();
    }

}

module.exports = HtmlWriter;