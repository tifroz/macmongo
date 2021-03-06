macmongo
===========

A basic helper for the mongodb native driver

## installation

```js
npm install macmongo
```

## usage

```js

var db = require("macmongo")
var logger = require("maclogger")

var config = {
	host: "127.0.0.1",
	port: 27017,
	databases: {
		exampledb: {
			examplecollection: [{indexField:1}, {indexField:1}, {multipleFields1: 1, multipleFields2: -1}]
		}
	},
	options: { // See https://mongodb.github.io/node-mongodb-native/api-generated/server.html?highlight=server for options
		auto_reconnect: true,
		poolSize: 2
	}
}


db.configure(config, logger, function(err) {
	// OK now we can use db.collections1.find({...})
	// Watch for logs in case collection names conflict across databases
	// db.addDatabase("dbname", dbdef, callback) for later addition of another database reference, for example:
	db.addDatabase("dbname", {dbname3: {collection4: [<list of indexes>]}}, function(err) {
		// OK now we can use db.collections4.find({...})
	})
})

```

## APIs

```js
var db = require("macmongo")

// Must be called first, with at least one database referenced in @params
db.initialize(params, logger, callback) 					

// Links a database (a.k.a makes the database available via db[dbname], or db.databases[dbname]), and creates it it doesn't already exists
db.linkDatabase(dbname, callback) 									

// Links a database (a.k.a makes the database available via db[dbname] if it already exists
linkDatabaseIfExists(dbname, callback) 					

// Links a database (a.k.a makes the database available via db[dbname], or db.databases[dbname]), and creates it if necessary with all specified collections and indexes
db.addDatabase(dbname, collectionsDef, callback)		

// Utility function to generate GUIDs, always useful when dealing with databases. Returns a string
db.uid() 																					

// Gets the database layout in json format ({db1:[col1, col2], db2: [col3,etc..]}) -  unlinked databases are not referenced
db.getLayout() 																		

```
