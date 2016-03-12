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

logger		= console
dbperf = {}


MongoClient = (dbname, host, port, options) ->
	collections = {}
	if not dbname then throw new Error 'A database name must be provided to create a new db client'
	logger.log "Initializing MongoDb server #{host}:#{port} with options", options
	server = new mongodb.Server(host, port, options)
	db = new mongodb.Db(dbname, server, {w: 1})
	logger.log "Created client for the '#{dbname}' db."

	shortCollectionName = (raw) ->
		raw.split('.').pop()
	addCollections = (cols, indexesDef) ->
		logger.log util.format("indexesDef at %j", indexesDef)
		for col in cols
			name = col.collectionName
			if name.substr(0,6) isnt 'system'
				if collections[name] is undefined
					collections[name] = col
					logger.log "OK looking at indexes for collection #{name}"
					if indexesDef[name] isnt undefined
						for indexDef in indexesDef[name]
							ensureIndexCallback = (name)->
								return (err, indexName)->
									if err
										logger.error("ensureIndex",err)
									else
										logger.log util.format("Collection #{name} has index named #{indexName}")
							
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
				logger.log("MongoClient.init: existing collections '#{existing}'")
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


	###
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
	*		options:
	*			auto_reconnect: true
	* etc..
	*
	* (For definition of options, see See https://mongodb.github.io/node-mongodb-native/api-generated/server.html?highlight=server for options)
	* 
	###
class DB
	configure: (params, lgger, fn) ->
		if fn is undefined and lgger?.log is undefined
			fn = lgger
		else if lgger isnt undefined
			logger = lgger

		@_host = params.host
		@_port = params.port
		@_options = params.options
		@databases = {}
		logger.log("DB initializing...")
		databases = @databases
		db = @
		dbnames = _.keys params?.databases
		Seq(dbnames).flatten()
			.seqEach (dbname)->
				db.addDatabase dbname, params.databases[dbname], this
			.seq ->
				fn?()
			.catch (err) ->
				fn?(err)
	
	### 

	* signature: dbname, [collectionsDef], [fn]
	* collectionsDef is a plain object, e.g
	* 	collection1: [<list of indexes>]
	* 	collection1: [<list of indexes>]
	* 	etc..
	* 
	
	###
	
	addDatabase: (dbname, collectionsDef, fn)->
		if fn is undefined
			unless _.isArray collectionsDef
				fn = collectionsDef
		db = @
		databases = @databases
		Seq().seq ->
			databases[dbname] = new MongoClient(dbname, db._host, db._port, db._options)
			if dbname in _.keys(db)
				logger.error "Conflicting database name, db.#{dbname} shortcut is unavailable, use db.databases.#{dbname}"
			else
				db[dbname] = databases[dbname]
			databases[dbname].initialize collectionsDef, this
		.seq ->
			collectionNames = _.keys databases[dbname].getCollections()
			intersect = _.intersection collectionNames, _.keys(db)
			for collectionName in collectionNames
				if collectionName in intersect
					logger.warn "Conflicting collections name(s), db.#{collName} shortcut(s) unavailable, use db.#{dbname}.#{collName}"
				else
					logger.info "OK  db.#{collectionName} is a valid shortcut for db.databases.#{dbname}.#{collectionName}"
					db[collectionName] = databases[dbname][collectionName]
			fn?()
		.catch (boo)->
			fn?(boo)


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

	getCollectionNames: ->
		collections = []
		for name, database of @databases
			logger.info "#{database} has #{_.keys(database.getCollections())}"
			collections = collections.concat _.keys(database.getCollections())
		return collections

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
					logger.log("db.perf.#{collectionName}","#{doc.nscanned} records scanned, #{doc.n} returned in #{doc.millis}ms")
					logger.log('db.perf.query', cursor.selector)
					logger.log('db.perf.explain', doc)
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


module.exports = new DB()
