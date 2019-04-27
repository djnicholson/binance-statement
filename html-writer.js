var fs = require('fs');
var path = require('path');
const util = require('util');

class HtmlWriter {

    constructor(outputFile) {
        this.file = fs.createWriteStream(outputFile);
    }

    async begin() {
        const readFile = util.promisify(fs.readFile);
        const templateFile = path.join(__dirname, 'template.html');
        const templateContents = await readFile(templateFile);
        this.templateParts = templateContents.toString().split('%pushEvents', 2);
        this.file.write(this.templateParts[0]);
    }

    consumeEvent(unitOfAccount, event) {
        this.file.write('statement.pushEvent(' + JSON.stringify(unitOfAccount) + ', ' + JSON.stringify(event) + ');\r\n');
    }

    end() {
        this.templateParts && this.file.write(this.templateParts[1]);
        this.file.end();
    }

}

module.exports = HtmlWriter;