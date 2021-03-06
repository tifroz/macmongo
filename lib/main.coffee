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

logger		= console
dbperf = {}


class MongoClient
	constructor: (dbname, host, port, auth, @_options) ->
		if not dbname then throw new Error 'A database name must be provided to create a new db client'
		logger.debug "Initializing MongoDb server #{@_host}:#{@_port} with options", @_options
		if auth?.user and auth.password and auth.database
			user = encodeURIComponent auth.user
			password = encodeURIComponent auth.password
			@_connectUrl = "mongodb://#{user}:#{password}@#{host}:#{port}/#{dbname}?authSource=#{auth.database}"
			logger.debug "Created client for the '#{dbname}' db, authenticated as #{auth.user} (from db #{auth.database})."
		else
			@_connectUrl = "mongodb://#{host}:#{port}/#{dbname}"
			logger.debug "Created client for the '#{dbname}' db (no authentication info provided)."

	close: ->
		@db.close()



	shortCollectionName: (raw) ->
		raw.split('.').pop()

	addCollections: (cols, indexesDef) ->
		@collections = {}
		logger.debug util.format("indexesDef at %j", indexesDef)
		for col in cols
			name = col.collectionName
			if name.substr(0,6) isnt 'system'
				if @collections[name] is undefined
					@collections[name] = col
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
	
	getAdmin: ->
		return @db.admin()

	getCollections: ->
		return @collections
	
	initialize: (params, fn) ->
		names = _.keys params
		connectUrl = @_connectUrl
		client = @
		logger.info("MongoClient.init: initializing #{names}")
		Seq().seq ->
			Client = mongodb.MongoClient
			Client.connect connectUrl, {db: {w: 1}, server: client._options}, this
		.seq (mongoClient)->
			client.db = mongoClient.db()
			client.db.collections(this)
		.seq (cols) ->
			client.addCollections(cols, params)
			existing = _.pluck(cols, 'collectionName')
			logger.debug("MongoClient.init: existing collections '#{existing}'")
			missing = _.difference(names, existing)
			if missing.length > 0
				logger.info("MongoClient.init: missing collections #{missing}")
			this(null, missing)
		.flatten()
		.parMap (name)->
			logger.info "Creating missing collection '#{name}'"
			client.db.createCollection name, this
		.unflatten()
		.seq (cols) ->
			if cols.length > 0
				logger.info("MongoClient.init: still missing collections #{_.pluck cols, 'collectionName'}")
				client.addCollections(cols, params)
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
	* etc..
	###
class DB
	initialize: (@params, lgger, fn) ->
		if fn is undefined and lgger?.log is undefined
			fn = lgger
		else if lgger isnt undefined
			logger = lgger

		@_linkingInitiated = {}
		@databases = {}
		databases = @databases
		db = @
		params = @params
		dbnames = _.keys params?.databases
		logger.debug(util.format("MongoDB initializing databases #{dbnames} from params %j", params))
		Seq(dbnames).flatten()
			.seqEach (dbname)->
				db.addDatabase dbname, params.databases[dbname], this
			.seq ->
				fn?()
			.catch (err) ->
				fn?(err)

	close: (delay = 1000)->
		db = @
		Seq().seq ->
			setTimeout this, delay
		.seq ->
			this(null, _(db.databases).values)
		.flatten()
		.seqEach (client)->
			client.close(this)
		.catch (err)->
			if err
				logger.error err.stack

	
	### 
	* Links a database (a.k.a makes the database available via db[dbname], or db.databases[dbname]), and creates it if necessary with all specified collections and indexes
	* 
	* signature: dbname, [collectionsDef], [fn]
	* collectionsDef is a plain object, e.g
	* 	collection1: [<list of indexes>]
	* 	collection1: [<list of indexes>]
	* 	etc..
	* 
	
	###
	
	addDatabase: (dbname, collectionsDef, fn)->
		if fn is undefined
			if _.isFunction collectionsDef
				fn = collectionsDef
				collectionsDef = {}
		db = @
		Seq().seq ->
			db.linkDatabase dbname, collectionsDef, this
		.seq ->
			collections = db.databases[dbname].getCollections()
			collectionNames = _.keys collections
			intersect = _.intersection collectionNames, _.keys(db)
			for collectionName in collectionNames
				db.databases[dbname][collectionName] = collections[collectionName]
				if collectionName in intersect
					logger.warn "Conflicting collections name(s), db.#{collectionName} shortcut unavailable, use db.databases.#{dbname}.#{collectionName}"
				else
					logger.info "OK  db.#{collectionName} is a valid shortcut for db.databases.#{dbname}.#{collectionName}"
					db[collectionName] = db.databases[dbname][collectionName]
			fn?()
		.catch (boo)->
			fn?(boo)

	###
	* Links a database (a.k.a makes the database available via db[dbname] if it already exists
	###
	linkDatabaseIfExists: (dbname, fn)->
		db = @
		logger.debug "OK linkDatabaseIfExists #{dbname}"
		if db.databases[dbname]?.collections isnt undefined
			logger.debug "OK database '#{dbname}' is already linked"
			return fn?()
		else if _.keys(db.databases).length is 0 and dontCreate
			return fn?(new Error("MacMongo can't get an admin (no existing db to get an admin instance off of)"))
		else
			if @_linkingInitiated[dbname]
				setTimeout fn, 500 # linking is in progress, just wait for a bit
			else
				@_linkingInitiated[dbname] = true
				Seq().seq ->
					firstDb = db.databases[_.keys(db.databases)[0]]
					firstDb.getAdmin().listDatabases this
				.seq (dbs)->
					dbnames = _.pluck dbs.databases, "name"
					if dbname.toLowerCase() not in dbnames
						return fn?(new Error("#{dbname.toLowerCase()} not found amongst collections of #{dbname} (#{dbnames})"))
					db.linkDatabase(dbname, this)
				.seq ->
					fn?()
				.catch (boo)->
					fn?(boo)

	
	###
	* Links a database (a.k.a makes the database available via db[dbname], or db.databases[dbname]), and creates it it doesn't already exists
	###
	linkDatabase: (dbname, collectionsDef, fn)->
		if fn is undefined
			if _.isFunction collectionsDef
				fn = collectionsDef
				collectionsDef = {}
		db = @
		if db.databases[dbname] isnt undefined
			logger.debug "OK database '#{dbname}' is already linked"
			return fn?()
		Seq().seq ->

			db.databases[dbname] = new MongoClient(dbname, db.params.host, db.params.port, db.params.auth, db.params.options)
			if dbname in _.keys(db)
				logger.error "Conflicting database name, db.#{dbname} shortcut is unavailable, use db.databases.#{dbname}"
			else
				db[dbname] = db.databases[dbname]
			db.databases[dbname].initialize collectionsDef, this
		.seq ->
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

	getCollectionNames: (dbname)->
		collections = []
		if dbname is undefined
			for name, database of @databases
				logger.info "#{database} has #{_.keys(database.getCollections())}"
				collections = collections.concat _.keys(database.getCollections())
		else
			collections = _.keys(@databases[dbname].getCollections())
		return collections

	
	getLayout: ->
		layout = {}
		for dbname, database of @databases
			if layout[dbname] is undefined
				layout[dbname] = []
			for colname, collection of database.getCollections()
				layout[dbname].push colname
		return layout

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
					logger.debug("db.perf.#{collectionName}","#{doc.nscanned} records scanned, #{doc.n} returned in #{doc.millis}ms")
					logger.debug('db.perf.query', cursor.selector)
					logger.debug('db.perf.explain', doc)
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
