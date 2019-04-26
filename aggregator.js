class Aggregator {

    constructor(db) {
        this.db = db;
    }

    async go() {
        // await this.db.forEachRecord(console.log);
    };

}

module.exports = Aggregator;