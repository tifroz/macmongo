/**
* Database helpers to provide easy database access.
*
* Usage:
* db = require(thismodule)
* db.initialize(function(){
* 	db.dbname.createCollection(...)
*	 	db.dbname.collectionname.find(...)
* })
 */

(function() {
  var BSON, MongoClient, Seq, assert, db, dbperf, logger, mongodb, util, _,
    __indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

  _ = require('underscore');

  mongodb = require('mongodb');

  assert = require('assert');

  util = require('util');

  Seq = require('seq');

  BSON = mongodb.pure().BSON;

  logger = require('maclogger');

  dbperf = {};

  MongoClient = function(dbname, host, port, options, loggerInstance) {
    var addCollections, collections, db, server, shortCollectionName;
    if (loggerInstance) {
      logger = loggerInstance;
    }
    collections = {};
    if (!dbname) {
      throw new Error('A database name must be provided to create a new db client');
    }
    logger.log("Initializing MongoDb server " + host + ":" + port + " with options", options);
    server = new mongodb.Server(host, port, options);
    db = new mongodb.Db(dbname, server, {
      w: 1
    });
    logger.log("Created client for the '" + dbname + "' db.");
    shortCollectionName = function(raw) {
      return raw.split('.').pop();
    };
    addCollections = function(cols, indexesDef) {
      var col, ensureIndexCallback, indexDef, name, _i, _len, _results;
      logger.log(util.format("indexesDef at %j", indexesDef));
      _results = [];
      for (_i = 0, _len = cols.length; _i < _len; _i++) {
        col = cols[_i];
        name = col.collectionName;
        if (name.substr(0, 6) !== 'system') {
          if (collections[name] === void 0) {
            collections[name] = col;
            logger.log("OK looking at indexes for collection " + name);
            if (indexesDef[name] !== void 0) {
              _results.push((function() {
                var _j, _len1, _ref, _results1;
                _ref = indexesDef[name];
                _results1 = [];
                for (_j = 0, _len1 = _ref.length; _j < _len1; _j++) {
                  indexDef = _ref[_j];
                  ensureIndexCallback = function(name) {
                    return function(err, indexName) {
                      if (err) {
                        return logger.error("ensureIndex", err);
                      } else {
                        return logger.log(util.format("Collection " + name + " has index named " + indexName));
                      }
                    };
                  };
                  _results1.push(col.ensureIndex(indexDef, {
                    background: true
                  }, ensureIndexCallback(name)));
                }
                return _results1;
              })());
            } else {
              _results.push(void 0);
            }
          } else {
            throw Error('Can\'t override existing member ' + name);
          }
        } else {
          _results.push(void 0);
        }
      }
      return _results;
    };
    return {
      getCollections: function() {
        return collections;
      },
      getClient: function() {
        return db;
      },
      initialize: function(params, fn) {
        var names;
        names = _.keys(params);
        logger.info("MongoClient.init: initializing " + names);
        return db.open(function(err, conn) {
          return Seq().seq(function() {
            return db.collections(this);
          }).seq(function(cols) {
            var existing, missing;
            addCollections(cols, params);
            existing = _.pluck(cols, 'collectionName');
            logger.log("MongoClient.init: existing collections '" + existing + "'");
            missing = _.difference(names, existing);
            if (missing.length > 0) {
              logger.info("MongoClient.init: missing collections " + missing);
            }
            return this(null, missing);
          }).flatten().parMap(function(name) {
            logger.info("Creating missing collection '" + name + "'");
            return db.createCollection(name, this);
          }).unflatten().seq(function(cols) {
            if (cols.length > 0) {
              logger.info("MongoClient.init: still missing collections " + (_.pluck(cols, 'collectionName')));
              addCollections(cols, params);
            }
            return fn(null, this);
          })["catch"](function(boo) {
            logger.error("MongoClient.initialize " + boo);
            return fn(boo);
          });
        });
      }
    };
  };

  db = {

    /**
    	* A utility method to generate GUIDs on the fly
     */
    uid: function() {
      var n, ui, uid;
      n = 4;
      uid = (function() {
        var _results;
        _results = [];
        while (n -= 1) {
          _results.push((Math.abs((Math.random() * 0xFFFFFFF) | 0)).toString(16));
        }
        return _results;
      })();
      ui = uid.join('');
      return ui;
    },

    /**
    	* Utility methods to escape dots from attribute names
     */
    escapeDot: function(str) {
      return str.replace(/\./g, "#dot;");
    },
    unescapeDot: function(o) {
      var p, res;
      res = {};
      if (_.isObject(o) && !_.isArray(o) && !_.isDate(o)) {
        for (p in o) {
          if (o.hasOwnProperty(p)) {
            res[p.replace(/#dot;/g, ".")] = db.unescapeDot(o[p]);
          }
        }
      } else {
        res = o;
      }
      return res;
    },
    clients: {},

    /**
    	* Connects the sysdb, local and shared clients to their respective databases
    	* and makes the collections available via thismodule.db.collectionname
    	* options:
    	*		host: 127.0.0.1
    	* 	port: 3002
    	*		databases:
    	*			dbname1:
    	*				collection1: [<list of indexes>]
    	*				collection2: []
    	*			dbname2:
    	*				collection: []
    	* etc..
     */
    initialize: function(params, fn) {
      var collections, dbnames;
      logger.log("DB initializing...");
      collections = {};
      dbnames = _.keys(params != null ? params.databases : void 0);
      return Seq(dbnames).flatten().seqEach(function(dbname) {
        db.clients[dbname] = new MongoClient(dbname, params.host, params.port, params.options);
        if (__indexOf.call(_.keys(db), dbname) >= 0) {
          fn(new Error("Conflicting database name(s): " + dbname));
        } else {
          db[dbname] = db.clients[dbname];
        }
        return db.clients[dbname].initialize(params.databases[dbname], this);
      })["catch"](function(err) {
        return fn(err);
      }).seq(function() {
        var dbName, intersect, _i, _len;
        collections = [];
        for (_i = 0, _len = dbnames.length; _i < _len; _i++) {
          dbName = dbnames[_i];
          intersect = _.intersection(_.keys(db.clients[dbName].getCollections()), _.keys(db));
          if (intersect.length > 0) {
            fn(new Error("Conflicting collection name(s): " + intersect));
          } else {
            logger.log("Adding collections	" + (_.keys(db.clients[dbName].getCollections())) + " to db (from the " + dbName + " db)");
          }
          _.extend(db, db.clients[dbName].getCollections());
          collections = _.union(collections, _.values(db.clients[dbName].getCollections()));
        }
        return this(null, collections);
      }).flatten().parEach(function(collection) {
        if (params.empty) {
          logger.warning("Emptying collection " + collection.collectionName + " (epmty=true)");
          return collection.remove({}, this);
        } else {
          return this();
        }
      }).seq(function() {
        setInterval(db.dumpPerf, 10 * 60 * 1000);
        logger.log("DB initialized " + (_.keys(db)));
        return fn(null);
      })["catch"](function(boo) {
        return fn(boo);
      });
    },

    /**
    	* Logs db performance & explain
     */
    perfLog: function(cursor) {
      return cursor.explain(function(err, doc) {
        var agg, collectionName, stats;
        if (err) {
          console.error('db.perf', err);
        }
        if (doc) {
          collectionName = cursor.collection.collectionName;
          if (doc.millis > 300) {
            logger.warn("db.perf." + collectionName, "Latency " + doc.millis + "ms: " + doc.n + " records returned, " + doc.nscanned + " scanned");
            logger.warn('db.perf.query', cursor.selector);
            logger.warn('db.perf.explain', doc);
          } else {
            logger.trace("db.perf." + collectionName, "" + doc.nscanned + " records scanned, " + doc.n + " returned in " + doc.millis + "ms");
            logger.trace('db.perf.query', cursor.selector);
            logger.trace('db.perf.explain', doc);
          }
          stats = dbperf[collectionName] != null ? dbperf[collectionName] : dbperf[collectionName] = {
            total: 0,
            min: 0,
            max: 0,
            count: 0,
            collection: collectionName
          };
          stats.min = Math.min(stats.min, doc.millis);
          stats.max = Math.max(stats.max, doc.millis);
          stats.total += doc.millis;
          stats.count += 1;
          agg = dbperf.aggregate != null ? dbperf.aggregate : dbperf.aggregate = {
            total: 0,
            min: 0,
            max: 0,
            count: 0,
            collection: 'aggregate'
          };
          agg.min = Math.min(agg.min, doc.millis);
          agg.max = Math.max(agg.max, doc.millis);
          agg.total += doc.millis;
          return agg.count += 1;
        }
      });
    },
    dumpPerf: function() {
      var allStats, coll, now, stats;
      return;
      now = new Date();
      allStats = (function() {
        var _results;
        _results = [];
        for (coll in dbperf) {
          stats = dbperf[coll];
          _results.push(_.extend(stats, {
            timestamp: now
          }));
        }
        return _results;
      })();
      return db.dbperf.insert(allStats, function(err, inserted) {
        if (err) {
          return console.error('db.dumpPerf', err);
        } else if (inserted) {
          return dbperf = {};
        }
      });
    },
    history: function(hrs, collection, fn) {
      if (hrs == null) {
        hrs = 24;
      }
      if (collection == null) {
        collection = 'aggregate';
      }
      return Seq().seq(function() {
        return db.dbperf.find({
          collection: collection,
          timestamp: {
            $gt: new Date(Date.now() - hrs * 60 * 60 * 1000)
          }
        }, this);
      }).seq(function(cursor) {
        return cursor.toArray(fn);
      })["catch"](function(err) {
        return fn(err);
      });
    }
  };

  module.exports = db;

}).call(this);