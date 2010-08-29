/**
 * @author Daniel Friesen
 * Copyright © 2009 Redwerks Systems Inc.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
{
	let m = com.mongodb, bson = org.bson.types;
	
	// WARNING: Using a WeakHashMap seams to result in a situation where once in awhile
	// two equal object ids will not be considered equal causing unexpected bugs
	// we should drop that and instead add something like ObjectId.equals and objid.indexIn
	let _oids = new java.util.WeakHashMap(); // Use a gcable map to make sure only one instance for an ObjectId exists
	function ObjectId(s) {
		if ( s && _oids.containsKey(""+s) )
			return _oids.get(""+s);
		if (!(this instanceof ObjectId))
			return new ObjectId(s);
		this._jObjectId = s ? new bson.ObjectId(s) : new bson.ObjectId();
		_oids.put(""+this._jObjectId, this);
	}
	ObjectId.prototype.equals = function(objectid) {
		var j = objectid instanceof bson.ObjectId ? objectid :
		        objectid instanceof ObjectId ? objectid._jObjectId :
		        isString(objectid) ? new bson.ObjectId(objectid) : false;
		if ( !j )
			return false;
		
		return !!this._jObjectId.equals(j);
	}
	ObjectId.prototype.toJSON = function toJSON() {
		return this.toString();
	};
	ObjectId.prototype.toString = function toString() {
		return String(this._jObjectId.toString());
	};
	ObjectId.prototype.toSource = function toSource() {
		return '(new ObjectId('+uneval(this.toString())+'))';
	};

	function Mongo(a, b) {
		if ( a )
			a = new m.DBAddress(a);
		if ( b )
			b = new m.DBAddress(b);
		this._jMongo = b ? new m.Mongo(a, b) :
			a ? new m.Mongo(a) :
			new m.Mongo();
	}
	Mongo.mongoToJS = function mongoToJS(dbobject) {
		//if ( dbobject instanceof m.DBUndefined )
		//	return undefined;
		if ( dbobject instanceof bson.ObjectId )
			return new ObjectId(String(dbobject.toString()));
		if ( dbobject instanceof m.BasicDBList ) {
			var arr = [];
			var iterator = dbobject.iterator();
			while(iterator.hasNext())
				arr.push(mongoToJS(iterator.next()));
			return arr;
		}
		if ( dbobject instanceof m.BasicDBObject ) {
			var obj = {};
			var keySet = dbobject.keySet();
			var iterator = keySet.iterator();
			while(iterator.hasNext()) {
				var k = iterator.next();
				obj[String(k)] = mongoToJS(dbobject.get(k));
			}
			return obj;
		}
		var Pattern = java.util.regex.Pattern;
		if ( dbobject instanceof Pattern )
			return new RegExp(String(dbobject.pattern()), String(m.Bytes.patternFlags(dbobject.flags())));
		if ( dbobject instanceof java.util.Date )
			return new Date(Number(dbobject.getTime()));
		if ( dbobject instanceof java.lang.Boolean )
			return !!dbobject.booleanValue();
		if ( dbobject instanceof java.lang.Number )
			return Number(dbobject);
		if ( dbobject instanceof java.lang.String )
			return String(dbobject);
		// @todo DBRefs
		return dbobject;
	};
	Mongo.jsToMongo = function jsToMongo(jsobject) {
		if ( jsobject instanceof java.lang.Object )
			// Don't touch stuff that's already in java land
			return jsobject;
		if ( jsobject === undefined )
			return undefined;// return new m.DBUndefined();
		if ( isArray(jsobject) ) {
			var dblist = new m.BasicDBList();
			for ( var i = 0; i < jsobject.length; i++ ) {
				dblist.put(String(i), jsToMongo(jsobject[i]));
			}
			return dblist;
		}
		if ( isFunction(jsobject) ) {
			// @todo CodeWScope? use bind?
			return new bson.Code(jsobject.toString());
		}
		if ( jsobject instanceof RegExp ) {
			var flags = [];
			if ( jsobject.global )
				flags.push('g');
			if ( jsobject.ignoreCase )
				flags.push('i');
			if ( jsobject.multiline )
				flags.push('m');
			return java.util.regex.Pattern.compile(jsobject.source, m.Bytes.patternFlags(flags.join('')));
		}
		if ( jsobject instanceof Date )
			return new java.util.Date(jsobject.getTime());
		if ( jsobject instanceof ObjectId )	
			return jsobject._jObjectId;
		if ( jsobject instanceof Object ) {
			var dbobject = new m.BasicDBObject();
			Object.keys(jsobject).forEach(function(key) {
				// I was thinking of using Object.getOwnPropertyNames, but I suppose we'll ignore non-enumerable keys
				if ( jsobject[key] !== undefined )
					dbobject.put(key, jsToMongo(jsobject[key]));
			});
			return dbobject;
		}
		// numbers, strings, and booleans don't need special treatment
		return jsobject;
	};
	Object.merge(Mongo.prototype, {
		getDB: function getDB(name) {
			return new DB(this, name);
		},
		getDatabaseNames: function getDatabaseNames() {
			var stringList = this._jMongo.getDatabaseNames();
			var iterator = stringList.iterator();
			var names = [];
			while(iterator.hasNext()) {
				names.push(String(iterator.next()));
			}
			return names;
		}
	});
	
	function DB(mongo, name) {
		this._mongo = mongo;
		this._jDB = this._mongo._jMongo.getDB(name);
	}
	Object.merge(DB.prototype, {
		auth: function auth(username, password) {
			return !!this._jDB.authenticate(username, password);
		},
		getMongo: function getMongo() {
			return this._mongo;
		},
		getSisterDB: function getSisterDB(name) {
			return this.getMongo().getDB(name);
		},
		getName: function getName() {
			return String(this._jDB.getName());
		},
		preloadCollections: function(/*...names*/) {
			// We don't have a catchall here so we'll just make collections preloadable
			Array.forEach(arguments, function(name) {
				if ( name in this )
					return;
				this[name] = this.getCollection(name);
			}, this);
		},
		getCollection: function getCollection(cname) {
			return new DBCollection(this, cname);
		},
		runCommand: function runCommand(cmdObj) {
			if ( isString(cmdObj) ) {
				var obj = {};
				obj[cmdObj] = 1;
				cmdObj = obj;
			}
			return Mongo.mongoToJS( this._jDB.command( Mongo.jsToMongo( cmdObj ) ) );
		},
		addUser: function addUser(username, password) {
			// 
		},
		removeUser: function removeUser(username) {
			// 
		},
		createCollection: function createCollection(name, options) {
			options = Mongo.jsToMongo(options);
			return new DBCollection(this, this._jDB.createCollection(name, options));
		},
		getReplicationInfo: function getReplicationInfo() {
			// 
		},
		getProfilingLevel: function getProfilingLevel() {
			// 
		},
		setProfilingLevel: function setProfilingLevel(level) {
			// 0=off 1=slow 2=all
		},
		cloneDatabase: function cloneDatabase(fromhost) {
			
		},
		copyDatabase: function copyDatabase(fromdb, todb, fromhost) {
			
		},
		shutdownServer: function shutdownServer() {
			
		},
		dropDatabase: function dropDatabase() {
			this._jDB.dropDatabase();
		},
		repairDatabase: function repairDatabase() {
			
		},
		eval: function eval(func /*, ...args*/) {
			func = func.toString();
			var args = [func];
			Array.slice(arguments, 1).forEach(function(arg) {
				args.push( Mongo.jsToMongo(arg) );
			});
			
			return Mongo.mongoToJS( this._jDB.eval.apply( this._jDB, args ) );
		},
		getLastError: function getLastError() {
			return Mongo.mongoToJS( this._jDB.getLastError() );
		},
		getPrevError: function getPrevError() {
			return Mongo.mongoToJS( this._jDB.getPreviousError() );
		},
		resetError: function resetError() {
			this._jDB.resetError();
		},
		getCollectionNames: function getCollectionNames() {
			var stringSet = this._jDB.getCollectionNames();
			var iterator = stringSet.iterator();
			var names = [];
			while(iterator.hasNext()) {
				names.push(String(iterator.next()));
			}
			return names;
		},
		group: function group(/*ns, key[, keyf], cond, reduce, initial*/) {
			
		}
	});
	
	function DBCollection(db, cname) {
		this._db = db;
		this._jDBCollection = cname instanceof m.DBCollection ? cname : this._db._jDB.getCollection(cname);
	}
	Object.merge(DBCollection.prototype, {
		getDB: function getDB() {
			return this._db;
		},
		getName: function getName() {
			return String(this._jDBCollection.getName());
		},
		findOne: function findOne(q, fields) {
			if ( q )
				q = Mongo.jsToMongo(q);
			if ( fields )
				fields = Mongo.jsToMongo(fields);
			var doc = fields ? this._jDBCollection.findOne(q, fields) :
				q ? this._jDBCollection.findOne(q) :
				this._jDBCollection.findOne();
			return Mongo.mongoToJS(doc);
		},
		find: function find(q, fields) {
			if ( q )
				q = Mongo.jsToMongo(q);
			if ( fields )
				fields = Mongo.jsToMongo(fields);
			var jCursor = fields ? this._jDBCollection.find(q, fields) :
				q ? this._jDBCollection.find(q) :
				this._jDBCollection.find();
			return new DBCursor(jCursor);
		},
		count: function count(q, fields) {
			if ( q )
				q = Mongo.jsToMongo(q);
			if ( fields )
				fields = Mongo.jsToMongo(fields);
			var count = fields ? this._jDBCollection.getCount(q, fields) :
				q ? this._jDBCollection.getCount(q) :
				this._jDBCollection.getCount();
			return Number(count);
		},
		insert: function insert(obj) {
			obj = Mongo.jsToMongo(obj);
			this._jDBCollection.insert(obj);
			return Mongo.mongoToJS(obj.get("_id"));
		},
		update: function update(q, obj, /*options*/upsert, multi) {
			var hasId = "_id" in obj;
			q = Mongo.jsToMongo(q);
			obj = Mongo.jsToMongo(obj);
			/*if ( typeof options === "boolean" )
				options = { upsert: options };
			options = options || {};
			this._jDBCollection.update(q, obj, !!options.upsert, options.ids !== false);*/
			if ( isObject(upsert) ) {
				multi = upsert.multi;
				upsert = upsert.upsert;
			}
			this._jDBCollection.update(q, obj, !!upsert, !!multi);
		},
		save: function save(obj) {
			obj = Mongo.jsToMongo(obj);
			this._jDBCollection.save(obj);
			return Mongo.mongoToJS(obj.get("_id"));
		},
		remove: function(q) {
			q = Mongo.jsToMongo(q);
			this._jDBCollection.remove(q);
		},
		ensureIndex: function ensureIndex(keys, options) {
			keys = Mongo.jsToMongo(keys);
			
			if ( options && options.name )
				this._jDBCollection.ensureIndex(keys, options.name, !!options && !!options.unique);
			else
				this._jDBCollection.ensureIndex(keys, false, !!options && !!options.unique);
		},
		dropIndexes: function dropIndexes() {
			this._jDBCollection.dropIndexes();
		},
		getIndexes: function getIndexes() {
			
		},
		drop: function drop() {
			this._jDBCollection.drop();
		},
		validate: function validate() {
			
		},
		stats: function stats() {
			
		},
		dataSize: function dataSize() {
			
		},
		totalIndexSize: function totalIndexSize() {
			
		},
		findAndModify: function findAndModify(options) {
			var o = { findandmodify: this.getName() };
			Object.merge(o, options);
			return this._db.runCommand(o).value;
		},
		mapReduce: function mapReduce(map, reduce, options) {
			var o = { mapreduce: this.getName() };
			options.map = map;
			options.reduce = reduce;
			Object.merge(o, options);
			return this._db.runCommand(o);
		}
	});
	
	function DBCursor(jDBCursor) {
		this.jDBCursor = jDBCursor;
	};
	Object.merge(DBCursor.prototype, {
		copy: function copy() {
			return new DBCursor(this.jDBCursor.copy());
		},
		count: function count() {
			return Number(this.jDBCursor.count());
		},
		/*get current() {
			return Number(this.jDBCursor.curr());
		},
		get length() {
			return Number(this.jDBCursor.length());
		},*/
		forEach: function forEach(callback, thisp) {
			var cursor = this.jDBCursor;
			
			while( !!cursor.hasNext() ) {
				var doc = Mongo.mongoToJS(cursor.next());
				callback.call(thisp||(function(){return this;})(), doc);
			}
			
		},
		map: function map(callback, thisp) {
			var arr = [];
			this.forEach(function(doc) {
				arr.push(callback.call(this, doc));
			}, thisp);
			return arr;
		},
		toArray: function() {
			return this.map(function(doc) { return doc; });
		},
		limit: function limit(max) {
			return new DBCursor(this.jDBCursor.limit(max));
		},
		skip: function skip(num) {
			return new DBCursor(this.jDBCursor.skip(num));
		},
		sort: function sort(orderBy) {
			orderBy = Mongo.jsToMongo(orderBy);
			return new DBCursor(this.jDBCursor.sort(orderBy));
		},
		hint: function hint(index) {
			if ( !isString(index) )
				index = Mongo.jsToMongo(index);
			return new DBCursor(this.jDBCursor.hint(index));
		}
	});
	
}
