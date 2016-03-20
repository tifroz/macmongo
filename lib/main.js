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
  var DB, MongoClient, Seq, assert, dbperf, logger, mongodb, util, _,
    __indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

  _ = require('underscore');

  mongodb = require('mongodb');

  assert = require('assert');

  util = require('util');

  Seq = require('seq');

  logger = console;

  dbperf = {};

  MongoClient = (function() {
    function MongoClient(_dbname, _host, _port, _options) {
      this._dbname = _dbname;
      this._host = _host;
      this._port = _port;
      this._options = _options;
      if (!this._dbname) {
        throw new Error('A database name must be provided to create a new db client');
      }
      logger.log("Initializing MongoDb server " + this._host + ":" + this._port + " with options", this._options);
    }

    MongoClient.prototype.shortCollectionName = function(raw) {
      return raw.split('.').pop();
    };

    MongoClient.prototype.addCollections = function(cols, indexesDef) {
      var col, ensureIndexCallback, indexDef, name, _i, _len, _results;
      this.collections = {};
      logger.log(util.format("indexesDef at %j", indexesDef));
      _results = [];
      for (_i = 0, _len = cols.length; _i < _len; _i++) {
        col = cols[_i];
        name = col.collectionName;
        if (name.substr(0, 6) !== 'system') {
          if (this.collections[name] === void 0) {
            this.collections[name] = col;
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

    MongoClient.prototype.getAdmin = function() {
      return this.db.admin();
    };

    MongoClient.prototype.getCollections = function() {
      return this.collections;
    };

    MongoClient.prototype.initialize = function(params, fn) {
      var client, names;
      names = _.keys(params);
      client = this;
      logger.info("MongoClient.init: initializing " + names);
      return Seq().seq(function() {
        var Client;
        Client = mongodb.MongoClient;
        return Client.connect("mongodb://" + client._host + ":" + client._port + "/" + client._dbname, {
          db: {
            w: 1
          },
          server: client._options
        }, this);
      }).seq(function(db) {
        client.db = db;
        return client.db.collections(this);
      }).seq(function(cols) {
        var existing, missing;
        client.addCollections(cols, params);
        existing = _.pluck(cols, 'collectionName');
        logger.log("MongoClient.init: existing collections '" + existing + "'");
        missing = _.difference(names, existing);
        if (missing.length > 0) {
          logger.info("MongoClient.init: missing collections " + missing);
        }
        return this(null, missing);
      }).flatten().parMap(function(name) {
        logger.info("Creating missing collection '" + name + "'");
        return client.db.createCollection(name, this);
      }).unflatten().seq(function(cols) {
        if (cols.length > 0) {
          logger.info("MongoClient.init: still missing collections " + (_.pluck(cols, 'collectionName')));
          client.addCollections(cols, params);
        }
        return fn(null, this);
      })["catch"](function(boo) {
        logger.error("MongoClient.initialize " + boo);
        return fn(boo);
      });
    };


    /*
    	* Connects the sysdb, local and shared databases to their respective databases
    	* and makes the collections available via thismodule.db.collectionname
    	* signature: params, [logger], [fn]
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

    return MongoClient;

  })();

  DB = (function() {
    function DB() {}

    DB.prototype.initialize = function(params, lgger, fn) {
      var databases, db, dbnames;
      if (fn === void 0 && (lgger != null ? lgger.log : void 0) === void 0) {
        fn = lgger;
      } else if (lgger !== void 0) {
        logger = lgger;
      }
      this._host = params.host;
      this._port = params.port;
      this._options = params.options;
      this._linkingInitiated = {};
      this.databases = {};
      logger.log("DB initializing...");
      databases = this.databases;
      db = this;
      dbnames = _.keys(params != null ? params.databases : void 0);
      return Seq(dbnames).flatten().seqEach(function(dbname) {
        return db.addDatabase(dbname, params.databases[dbname], this);
      }).seq(function() {
        return typeof fn === "function" ? fn() : void 0;
      })["catch"](function(err) {
        return typeof fn === "function" ? fn(err) : void 0;
      });
    };


    /* 
    	* Links a database (a.k.a makes the database available via db[dbname], or db.databases[dbname]), and creates it if necessary with all specified collections and indexes
    	* 
    	* signature: dbname, [collectionsDef], [fn]
    	* collectionsDef is a plain object, e.g
    	* 	collection1: [<list of indexes>]
    	* 	collection1: [<list of indexes>]
    	* 	etc..
    	*
     */

    DB.prototype.addDatabase = function(dbname, collectionsDef, fn) {
      var db;
      if (fn === void 0) {
        if (_.isFunction(collectionsDef)) {
          fn = collectionsDef;
          collectionsDef = {};
        }
      }
      db = this;
      return Seq().seq(function() {
        return db.linkDatabase(dbname, collectionsDef, this);
      }).seq(function() {
        var collectionName, collectionNames, collections, intersect, _i, _len;
        collections = db.databases[dbname].getCollections();
        collectionNames = _.keys(collections);
        intersect = _.intersection(collectionNames, _.keys(db));
        for (_i = 0, _len = collectionNames.length; _i < _len; _i++) {
          collectionName = collectionNames[_i];
          db.databases[dbname][collectionName] = collections[collectionName];
          if (__indexOf.call(intersect, collectionName) >= 0) {
            logger.warn("Conflicting collections name(s), db." + collectionName + " shortcut unavailable, use db.databases." + dbname + "." + collectionName);
          } else {
            logger.info("OK  db." + collectionName + " is a valid shortcut for db.databases." + dbname + "." + collectionName);
            db[collectionName] = db.databases[dbname][collectionName];
          }
        }
        return typeof fn === "function" ? fn() : void 0;
      })["catch"](function(boo) {
        return typeof fn === "function" ? fn(boo) : void 0;
      });
    };


    /*
    	* Links a database (a.k.a makes the database available via db[dbname] if it already exists
     */

    DB.prototype.linkDatabaseIfExists = function(dbname, fn) {
      var db, _ref;
      db = this;
      logger.log("OK linkDatabaseIfExists " + dbname);
      if (((_ref = db.databases[dbname]) != null ? _ref.collections : void 0) !== void 0) {
        logger.log("OK database '" + dbname + "' is already linked");
        return typeof fn === "function" ? fn() : void 0;
      } else if (_.keys(db.databases).length === 0 && dontCreate) {
        return typeof fn === "function" ? fn(new Error("MacMongo can't get an admin (no existing db to get an admin instance off of)")) : void 0;
      } else {
        if (this._linkingInitiated[dbname]) {
          return setTimeout(fn, 500);
        } else {
          this._linkingInitiated[dbname] = true;
          return Seq().seq(function() {
            var firstDb;
            firstDb = db.databases[_.keys(db.databases)[0]];
            return firstDb.getAdmin().listDatabases(this);
          }).seq(function(dbs) {
            var dbnames, _ref1;
            dbnames = _.pluck(dbs.databases, "name");
            if (_ref1 = dbname.toLowerCase(), __indexOf.call(dbnames, _ref1) < 0) {
              return typeof fn === "function" ? fn(new Error("" + (dbname.toLowerCase()) + " not found amongst collections of " + dbname + " (" + dbnames + ")")) : void 0;
            }
            return db.linkDatabase(dbname, this);
          }).seq(function() {
            return typeof fn === "function" ? fn() : void 0;
          })["catch"](function(boo) {
            return typeof fn === "function" ? fn(boo) : void 0;
          });
        }
      }
    };


    /*
    	* Links a database (a.k.a makes the database available via db[dbname], or db.databases[dbname]), and creates it it doesn't already exists
     */

    DB.prototype.linkDatabase = function(dbname, collectionsDef, fn) {
      var db;
      if (fn === void 0) {
        if (_.isFunction(collectionsDef)) {
          fn = collectionsDef;
          collectionsDef = {};
        }
      }
      db = this;
      if (db.databases[dbname] !== void 0) {
        logger.log("OK database '" + dbname + "' is already linked");
        return typeof fn === "function" ? fn() : void 0;
      }
      return Seq().seq(function() {
        db.databases[dbname] = new MongoClient(dbname, db._host, db._port, db._options);
        if (__indexOf.call(_.keys(db), dbname) >= 0) {
          logger.error("Conflicting database name, db." + dbname + " shortcut is unavailable, use db.databases." + dbname);
        } else {
          db[dbname] = db.databases[dbname];
        }
        return db.databases[dbname].initialize(collectionsDef, this);
      }).seq(function() {
        return typeof fn === "function" ? fn() : void 0;
      })["catch"](function(boo) {
        return typeof fn === "function" ? fn(boo) : void 0;
      });
    };


    /**
    	* A utility method to generate GUIDs on the fly
     */

    DB.prototype.uid = function() {
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
    };


    /**
    	* Utility methods to escape dots from attribute names
     */

    DB.prototype.escapeDot = function(str) {
      return str.replace(/\./g, "#dot;");
    };

    DB.prototype.unescapeDot = function(o) {
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
    };

    DB.prototype.getCollectionNames = function(dbname) {
      var collections, database, name, _ref;
      collections = [];
      if (dbname === void 0) {
        _ref = this.databases;
        for (name in _ref) {
          database = _ref[name];
          logger.info("" + database + " has " + (_.keys(database.getCollections())));
          collections = collections.concat(_.keys(database.getCollections()));
        }
      } else {
        collections = _.keys(this.databases[dbname].getCollections());
      }
      return collections;
    };

    DB.prototype.getLayout = function() {
      var collection, colname, database, dbname, layout, _ref, _ref1;
      layout = {};
      _ref = this.databases;
      for (dbname in _ref) {
        database = _ref[dbname];
        if (layout[dbname] === void 0) {
          layout[dbname] = [];
        }
        _ref1 = database.getCollections();
        for (colname in _ref1) {
          collection = _ref1[colname];
          layout[dbname].push(colname);
        }
      }
      return layout;
    };


    /**
    	* Logs db performance & explain
     */

    DB.prototype.perfLog = function(cursor) {
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
            logger.log("db.perf." + collectionName, "" + doc.nscanned + " records scanned, " + doc.n + " returned in " + doc.millis + "ms");
            logger.log('db.perf.query', cursor.selector);
            logger.log('db.perf.explain', doc);
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
    };

    DB.prototype.dumpPerf = function() {
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
    };

    DB.prototype.history = function(hrs, collection, fn) {
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
    };

    return DB;

  })();

  module.exports = new DB();

}).call(this);