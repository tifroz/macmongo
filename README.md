itunes-node
===========

search against iTunes using nodejs

## installation

```js
npm install macmongo
```

## usage

```js
	/********************************************************************************************
	* Connects to the mongo db instance, and makes the collections available as db.collection1, db.collection 2 etc...
	* collection and indexes are created automatically if they don't already exist
	* options:
	*		host: 127.0.0.1
	* 	port: 3002
	*		databases:
	*			dbname1:
	*				collection1: [<list of indexes>]
	*				collection2: [<list of indexes>]
	*			dbname2:
	*				collection3: [<list of indexes>]
	* etc..
	********************************************************************************************/

var db = require("macmongo")
var options = {
	host: "127.0.0.1",
	port: 27017,
	databases: {
		exampledb: {
			examplecollection: [{indexField:1}, {indexField:1}, {multipleFields1: 1, multipleFields2: -1}]
		}
	}
}
db.initialize(options, function(err) {
	// Ok now we can use db.examplecollection.find({...})
})

```
