var fs = require('fs');
var path = require('path');
var sass = require('sass');
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

    async begin(assetPrecisionTable) {

        const templateFile = path.join(__dirname, 'template.html');
        const templateContents = await readFileContents(templateFile);
        const templateParts = templateContents.split('<!--inject(events)-->', 2);
        const preludeTemplate = templateParts[0];
        this.epilogue = templateParts[1];

        const rendererFile = path.join(__dirname, 'renderer.js');
        const rendererCode = await readFileContents(rendererFile);

        const scssFile = path.join(__dirname, 'styles.scss');
        const result = await sass.renderSync({ file: scssFile });
        const css = result.css.toString();

        const jqueryJs = await getDependencySource('jquery/dist/jquery.min.js');
        const popperJs = await getDependencySource('popper.js/dist/umd/popper.min.js');
        const bootstrapJs = await getDependencySource('bootstrap/dist/js/bootstrap.min.js');
        const plotlyJs = await getDependencySource('plotly.js/dist/plotly.min.js');

        const prelude = preludeTemplate
            .replace('<!--inject(css)-->', () => '<style type="text/css">' + css + '</style>')
            .replace('<!--inject(jqueryJs)-->', () => '<script>' + jqueryJs + '</script>')
            .replace('<!--inject(popperJs)-->', () => '<script>' + popperJs + '</script>')
            .replace('<!--inject(bootstrapJs)-->', () => '<script>' + bootstrapJs + '</script>')
            .replace('<!--inject(plotlyJs)-->', () => '<script>' + plotlyJs + '</script>')
            .replace('<!--inject(rendererCode)-->', () => '<script>' + rendererCode + '</script>');

        this.file.write(prelude);

        this.file.write('<script>assetPrecisions = ' + JSON.stringify(assetPrecisionTable) + ';</script>\r\n');

        const assetManifestJson = await getDependencySource('cryptocurrency-icons/manifest.json');
        const assetManifest = JSON.parse(assetManifestJson);
        const assetColors = {};
        for (let i = 0; i < assetManifest.length; i++) {
            assetColors[assetManifest[i].symbol] = assetManifest[i].color;
        }

        this.file.write('<script>assetColors = ' + JSON.stringify(assetColors) + ';</script>\r\n');
    }

    consumeEvent(unitOfAccount, event) {
        this.file.write('<script>statement.pushEvent(' + JSON.stringify(unitOfAccount) + ', ' + JSON.stringify(event) + ');</script>\r\n');
    }

    end() {
        this.file.write('<script>statement.loadingComplete();</script>\r\n');
        this.epilogue && this.file.write(this.epilogue);
        this.file.end();
    }

}

module.exports = HtmlWriter;