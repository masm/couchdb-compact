var mu = require("masm-util");

var argv = require("optimist")
        .default("host", process.env["COUCHDB_PORT_5984_TCP_ADDR"] || process.env["COUCHDB_HOST"] || "localhost")
        .default("port", process.env["COUCHDB_PORT_5984_TCP_PORT"] || process.env["COUCHDB_PORT"]  || 5984)
        .default("username", process.env["COUCHDB_USERNAME"])
        .default("password", process.env["COUCHDB_PASSWORD"])
        .default("proxy", null)
        .default("force", false)
        .argv;

var cdb = require("masm-cdb").connection(argv.host, argv.port, argv.username, argv.password);

if (argv.proxy) {
    var m = argv.proxy.match(/^([^:]+):([0-9]+)$/);
    if (!m) {
        console.log("Invalid proxy specification");
        process.exit(1);
    }
    cdb.withProxy(m[1], parseInt(m[2], 10));
}

var rcdb = require("masm-rcdb")(cdb);

rcdb.allDBs({
    ok: function (data) {
        mu.eachSync(data, function (dbName, callback) {
            console.log(dbName);
            handleDB(dbName, function (err) {
                console.dir(err);
                callback();
            }, callback);
        }, function () {
            console.log("All done.");
        });
    }
});

function handleDB (dbName, escapeCallback, callback) {
    getDBInfo(dbName, escapeCallback, function (dbInfo) {
        compactDB(dbName, escapeCallback, function () {
            ddocNames(dbName, escapeCallback, function (ddocs) {
                viewCleanup(dbName, escapeCallback, function () {
                    mu.eachSync(ddocs, function (ddoc, next) {
                        compactView(dbName, ddoc, escapeCallback, callback);
                    }, callback);
                });
            });
        });
    });
}

function databaseIsFragmented (dbInfo) {
    return argv.force || dbInfo.data_size / dbInfo.disk_size  < 0.5;
}

function ddocNames (dbName, escapeCallback, callback) {
    rcdb.getAllDocs(dbName, {startkey: JSON.stringify("_design/"), endkey: JSON.stringify("_design/~")}, {
        not_found: function (err, retry) { escapeCallback(err); },
        ok: function (data) {
            var ddocs = (data.rows || []).map(function (row) {
                var m =  row.id.match(/^_design\/(.*)$/);
                return m[1];
            });
            callback(ddocs);
        }
    });
}

function compactDB (dbName, escapeCallback, callback) {
    rcdb.compactDB(dbName, {
        not_found: function (err, retry) { escapeCallback(err); },
        ok: function () {
            waitCompactionDone(dbName, escapeCallback, callback);
        }
    });
}

function waitCompactionDone (dbName, escapeCallback, callback) {
    mu.retryWithThrottling(function (retry) {
        getDBInfo(dbName, escapeCallback, function (data) {
            if (data.compact_running) {
                retry();
            } else {
                callback();
            }
        });
    });
}

function getDBInfo (dbName, escapeCallback, callback) {
    rcdb.getDB(dbName, {
        not_found: function (err, retry) { escapeCallback(err); },
        ok: callback
    });
}

function viewCleanup (dbName, escapeCallback, callback) {
    rcdb.viewCleanup(dbName, {
        not_found: function (err, retry) { escapeCallback(err); },
        ok: callback
    });
}

function compactView (dbName, ddoc, escapeCallback, callback) {
    rcdb.compactViews(dbName, ddoc, {
        not_found: function (err, rerty) { escapeCallback(err); },
        ok: function () {
            waitViewCompactionDone(dbName, ddoc, escapeCallback, callback);
        }
    });
}

function waitViewCompactionDone (dbName, ddoc, escapeCallback, callback) {
    mu.retryWithThrottling(function (retry) {
        getDdocInfo(dbName, ddoc, escapeCallback, function (data) {
            if (data.view_index.compact_running) {
                retry();
            } else {
                callback();
            }
        });
    });
}

function getDdocInfo (dbName, ddoc, escapeCallback, callback) {
    rcdb.ddocInfo(dbName, ddoc,  {
        not_found: function (err, retry) { escapeCallback(err); },
        ok:  callback
    });
}
