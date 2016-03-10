###*
* Database helpers to provide easy database access.
*
* Usage:
* db = require(thismodule)
* db.initialize(function(){
* 	db.dbname.createCollection(...)
*	 	db.dbname.collectionname.find(...)
* })
###
_ = require('underscore')
mongodb = require('mongodb')
assert = require('assert')
util = require('util')
Seq = require('seq')

BSON = mongodb.pure().BSON

logger		= require 'maclogger'
dbperf = {}


MongoClient = (dbname, host, port, options, loggerInstance) ->
	if loggerInstance
		logger = loggerInstance
	collections = {}
	if not dbname then throw new Error 'A database name must be provided to create a new db client'
	logger.debug "Initializing MongoDb server #{host}:#{port} with options", options
	server = new mongodb.Server(host, port, options)
	db = new mongodb.Db(dbname, server, {w: 1})
	logger.debug "Created client for the '#{dbname}' db."

	shortCollectionName = (raw) ->
		raw.split('.').pop()
	addCollections = (cols, indexesDef) ->
		logger.debug util.format("indexesDef at %j", indexesDef)
		for col in cols
			name = col.collectionName
			if name.substr(0,6) isnt 'system'
				if collections[name] is undefined
					collections[name] = col
					logger.debug "OK looking at indexes for collection #{name}"
					if indexesDef[name] isnt undefined
						for indexDef in indexesDef[name]
							ensureIndexCallback = (name)->
								return (err, indexName)->
									if err
										logger.error("ensureIndex",err)
									else
										logger.debug util.format("Collection #{name} has index named #{indexName}")
							
							col.ensureIndex indexDef, background: true,  ensureIndexCallback(name)
				
				else
					throw Error('Can\'t override existing member '+name)
	
	getCollections: ->
		return collections
	
	getClient: ->
		return db
	
	initialize: (params, fn) ->
		names = _.keys params
		logger.info("MongoClient.init: initializing #{names}")
		db.open (err, conn) ->
			Seq().seq ->
				db.collections(this)
			.seq (cols) ->
				addCollections(cols, params)
				existing = _.pluck(cols, 'collectionName')
				logger.debug("MongoClient.init: existing collections '#{existing}'")
				missing = _.difference(names, existing)
				if missing.length > 0
					logger.info("MongoClient.init: missing collections #{missing}")
				this(null, missing)
			.flatten()
			.parMap (name)->
				logger.info "Creating missing collection '#{name}'"
				db.createCollection name, this
			.unflatten()
			.seq (cols) ->
				if cols.length > 0
					logger.info("MongoClient.init: still missing collections #{_.pluck cols, 'collectionName'}")
					addCollections(cols, params)
				fn(null,@)
			.catch (boo)->
				logger.error("MongoClient.initialize #{boo}")
				fn(boo)

db =
	###*
	* A utility method to generate GUIDs on the fly
	###
	uid: ->
		n = 4
		uid = while n-=1
			(Math.abs((Math.random() * 0xFFFFFFF) | 0)).toString(16)
		ui = uid.join('')
		return ui

	###*
	* Utility methods to escape dots from attribute names
	###
	escapeDot: (str) ->
		return str.replace(/\./g, "#dot;")
	
	unescapeDot: (o) ->
		res = {}
		if _.isObject(o) and not _.isArray(o) and not _.isDate(o)
			for p of o
				res[p.replace(/#dot;/g, ".")] = db.unescapeDot(o[p])	if o.hasOwnProperty(p)
		else
			res = o
		return res

	clients: {}

	###*
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
	###
	initialize: (params, fn) ->
		logger.debug("DB initializing...")
		#clients = {}
		collections = {}
		dbnames = _.keys params?.databases
		Seq(dbnames).flatten()
			.seqEach (dbname)->
				db.clients[dbname] = new MongoClient(dbname, params.host, params.port, params.options)
				intersect = _.intersection _.keys(db.clients), _.keys(db)
				if intersect.length > 0
					fn(new Error("Conflicting database name(s): #{intersect}"))
				else
					_.extend db, db.clients
				db.clients[dbname].initialize params.databases[dbname], this
			.catch (err) ->
				fn(err)
			.seq ->
				collections = []
				for dbName in dbnames
					intersect = _.intersection _.keys(db.clients[dbName].getCollections()), _.keys(db)
					if intersect.length > 0
						fn(new Error("Conflicting collection name(s): #{intersect}"))
					else
						logger.debug("Adding collections	#{_.keys(db.clients[dbName].getCollections())} to db (from the #{dbName} db)")
					_.extend db, db.clients[dbName].getCollections()
					collections = _.union collections, _.values(db.clients[dbName].getCollections())
				this(null, collections)
			.flatten()
			.parEach (collection)->
				if params.empty
					logger.warning "Emptying collection #{collection.collectionName} (epmty=true)"
					collection.remove {}, this
				else
					this()
			.seq ->
				setInterval db.dumpPerf, 10 * 60 * 1000 # Dump stats every 10min
				logger.debug("DB initialized #{_.keys(db)}")
				fn(null)
			.catch (boo)->
				fn(boo)

	###*
	* Logs db performance & explain
	###
	perfLog: (cursor) ->
		cursor.explain (err, doc) ->
			if err then console.error('db.perf',err)
			if doc
				collectionName = cursor.collection.collectionName
				if doc.millis > 300
					logger.warn("db.perf.#{collectionName}","Latency #{doc.millis}ms: #{doc.n} records returned, #{doc.nscanned} scanned")
					logger.warn('db.perf.query', cursor.selector)
					logger.warn('db.perf.explain', doc)
				else
					logger.trace("db.perf.#{collectionName}","#{doc.nscanned} records scanned, #{doc.n} returned in #{doc.millis}ms")
					logger.trace('db.perf.query', cursor.selector)
					logger.trace('db.perf.explain', doc)
				stats = dbperf[collectionName] ?= {total: 0, min: 0, max: 0, count: 0, collection: collectionName}
				stats.min = Math.min(stats.min, doc.millis)
				stats.max = Math.max(stats.max, doc.millis)
				stats.total += doc.millis
				stats.count += 1
				agg = dbperf.aggregate ?= {total: 0, min: 0, max: 0, count: 0, collection: 'aggregate'}
				agg.min = Math.min(agg.min, doc.millis)
				agg.max = Math.max(agg.max, doc.millis)
				agg.total += doc.millis
				agg.count += 1
	dumpPerf: ->
		return # Not doing it for now
		now = new Date()
		allStats = ( _.extend(stats,{timestamp: now}) for coll, stats of dbperf )
		db.dbperf.insert allStats, (err, inserted) ->
				if err
					console.error('db.dumpPerf',err)
				else if inserted
					dbperf = {}

	history: (hrs=24, collection='aggregate', fn)->
		Seq()	.seq ->
					db.dbperf.find {collection: collection, timestamp: {$gt: (new Date(Date.now()-hrs*60*60*1000))}}, this
				.seq (cursor)->
					cursor.toArray(fn)
				.catch (err)->
					fn(err)


module.exports = db
