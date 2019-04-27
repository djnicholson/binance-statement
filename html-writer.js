var fs = require('fs');
var path = require('path');
const util = require('util');

const readFileContents = async(file) => {
    const readFile = util.promisify(fs.readFile);
    return (await readFile(file)).toString();
}

const getDependencySource = async(dependency) => {
    const fullPath = require.resolve(dependency);
    return await readFileContents(fullPath);
};

class HtmlWriter {

    constructor(outputFile) {
        this.file = fs.createWriteStream(outputFile);
    }

    async begin() {

        const templateFile = path.join(__dirname, 'template.html');
        const templateContents = await readFileContents(templateFile);
        const templateParts = templateContents.split('<!--inject(events)-->', 2);
        const preludeTemplate = templateParts[0];
        this.epilogue = templateParts[1];

        const rendererFile = path.join(__dirname, 'renderer.js');
        const rendererCode = await readFileContents(rendererFile);

        const bootstrapCss = await getDependencySource('bootstrap/dist/css/bootstrap.min.css');
        const jqueryJs = await getDependencySource('jquery/dist/jquery.min.js');
        const popperJs = await getDependencySource('popper.js/dist/umd/popper.min.js');
        const bootstrapJs = await getDependencySource('bootstrap/dist/js/bootstrap.min.js');

        const prelude = preludeTemplate
            .replace('<!--inject(bootstrapCSS)-->', '<style type="text/css">' + bootstrapCss + '</style>')
            .replace('<!--inject(jqueryJs)-->', '<script>' + jqueryJs + '</script>')
            .replace('<!--inject(popperJs)-->', '<script>' + popperJs + '</script>')
            .replace('<!--inject(bootstrapJs)-->', '<script>' + bootstrapJs + '</script>')
            .replace('<!--inject(rendererCode)-->', '<script>' + rendererCode + '</script>');

        this.file.write(prelude);
    }

    consumeEvent(unitOfAccount, event) {
        this.file.write('<script>statement.pushEvent(' + JSON.stringify(unitOfAccount) + ', ' + JSON.stringify(event) + ');</script>\r\n');
    }

    end() {
        this.file.write('<script>');
        this.file.write('statement.loadingComplete();\r\n');
        this.file.write('</script>');
        this.epilogue && this.file.write(this.epilogue);
        this.file.end();
    }

}

module.exports = HtmlWriter;