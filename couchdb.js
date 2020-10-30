'use strict';

const Nano = require('nano');

const GLOBAL_CHANGES_DB = "_global_changes"
const USERS_DB = "_users"
const REPLICATOR_DB = "_replicator"
const SCHEDULER_DB = "_scheduler"

class CouchDBClient {

    /**
     * 
     * @param {String} host The CouchDB host/ip
     * @param {String} port The CouchDB port
     * @param {String} user The CouchDB user
     * @param {String} password The CouchDB password
     */
    constructor(host, port, user, password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.instanceUrl = 'http://' + user + ':' + password + '@' + host + ':' + port;
        this.nano = Nano(this.instanceUrl);
        this.dbConn = {}
    }

    /**
     * Returns all design docs from a databae
     * @param {String} dbName The database name
     */
    async getDesignDocs(dbName) {
        const db = this.nano.use(dbName);
        return db.list({ startkey: '_design/', endkey: '_design0', include_docs: true });
    }

    /**
     * Returns the specified view
     * @param {String} dbName The database name to get the view from
     * @param {String} designName The design doc name
     * @param {String} viewName The view name
     */
    async getView(dbName, designName, viewName) {
        const db = this.nano.use(dbName);
        return db.view(designName, viewName);
    }

    /**
     * Creates a database in couchdb
     * @param {String} name The database name
     */
    async createDatabase(name) {
        return this.nano.db.create(name);
    }

    /**
     * Deletes a database from couchdb
     * @param {String} name The database to delete
     */
    async deleteDatabase(name) {
        return this.nano.db.destroy(name);
    }

    /**
     * 
     * @param {String} name The database to compact
     */
    async compactDatabase(name) {
        return this.nano.db.compact(name);
    }

    /**
     * 
     * @param {String} dbName The database name to add the document
     * @param {JSON} doc The document object
     * @param {String} id The document id
     */
    async createDocument(dbName, doc, id) {
        let db = null;
        if (this.dbConn.hasOwnProperty(dbName)) {
            db = this.dbConn[dbName];
        } else {
            db = this.nano.use(dbName);
            this.dbConn[dbName] = db;
        }
        return db.insert(doc, id);
    }

    /**
     * 
     * @param {List} exclude_list Databases to exclude from the list
     * @param {Boolean} exclude_system Exclude system databases from the list
     */
    async getDatabases({ exclude_list = [], exclude_system = true } = {}) {

        let exclude_dbs = exclude_list;

        if (exclude_system) {
            exclude_dbs = exclude_dbs.concat([GLOBAL_CHANGES_DB, USERS_DB, REPLICATOR_DB]);
        }

        let db_list = await this.nano.db.list();

        return db_list.filter(db => !exclude_dbs.includes(db));
    }

    /**
     * 
     * @param {String} db The database name
     */
    async getDatabaseDetails(db) {
        return await this.nano.db.get(db);
    }

    /**
     * 
     * @param {CouchDBClient} sourceInstance The instance of the source CouchDB client
     * @param {String} db The database name to replicate
     * @param {Boolean} continuous If it will be a continous replication
     */
    async startReplication(sourceInstance, db, continuous = true) {

        if ((this.host === sourceInstance.host) &&
            (this.port === sourceInstance.port)) {
            throw Error('Can\'t replicate from/to the same server');
        }

        let sourceDb = sourceInstance.instanceUrl + '/' + encodeURIComponent(db);
        return this.nano.db.replication.enable(
            sourceDb, db,
            { create_target: true, continuous: continuous });
    }

    /**
     * 
     * @param {String} jobId The Job Document ID
     * @param {String} jobRev The Job Document Revision
     */
    async stopReplication(jobId, jobRev) {
        return this.nano.db.replication.disable(jobId, jobRev);
    }

    /**
     * This method returns only the *running* replication jobs, it does
     * not include the rescheduled ones. So, if this method is being used
     * to cancel running jobs, it should be called multiple time until it
     * returns no jobs
     */
    async getRunningReplicationJobs() {
        let docs = [];
        let replicator = this.nano.use(REPLICATOR_DB);

        let sched = await this.nano.request({ db: SCHEDULER_DB, path: 'jobs' });
        for (const sJob of sched.jobs) {
            const repJob = await replicator.get(sJob.doc_id);
            docs.push(repJob);
        }

        return Promise.all(docs);
    }

}

module.exports = {
    CouchDBClient
};