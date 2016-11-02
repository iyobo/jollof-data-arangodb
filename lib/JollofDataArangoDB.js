/**
 * Created by iyobo on 2016-10-30.
 */
const arangojs = require('arangojs');
const log = require('jollof').log;
const co = require('co');
const _ = require('lodash');

/**
 * An adapter class for
 */
class JollofDataArangoDB {
	constructor( options ) {
		try {

			this._connectionOptions = options;
			this._collections = [];

			this._db = arangojs(options);

		} catch (err) {
			log.error(err.stack);
		}
	}

	/**
	 * This is run once on boot.
	 *
	 * @param schema
	 */
	* configureCollection( schema ) {
		//Ensure collection is created
		const that = this;
		// co(function*() {

		//Apparently in arango3, this can no longer be done
		// const dbinfo = yield that._db.createDatabase(that._connectionOptions.databaseName);
		// yield that._db.useDatabase(that._connectionOptions.databaseName);

		const collection = that._db.collection(schema.name);

		//Collection create gets it's own try-catch because we don't particularly care for a 'duplicate name' error
		try {
			yield collection.create();
		} catch (err) {
			//No need to throw an error if collection already exists
			log.debug(err.message);
		}

		//Ensure Indexes
		try {
			_.each(schema.indexes, function*( index ) {
				switch (index.type) {
					case 'geo':
						switch (index.subtype) {
							case 'point':
								yield collection.createGeoIndex(index.fields, index.opts || {});
								break;
						}
						break;
					case 'list':
						yield collection.createSkipList(index.fields, index.opts || {});
						break;
					case 'persistent':
						yield collection.createPersistentIndex(index.fields, index.opts || {});
						break;
					case 'hash':
						yield collection.createHashIndex(index.fields, index.opts || {});
						break;
					case 'fullText':
						let field = '';
						if (Array.isArray(index.fields))
							yield collection.createFulltextIndex(index.fields[ 0 ], index.opts || {});
						else if (typeof index.fields === 'string')
							yield collection.createFulltextIndex(index.fields, index.opts || {});

						break;

				}
			});

			that._collections.push(schema.name);
			log.debug(schema.name + ' model successfully using ArangoDB on DB: ' + that._connectionOptions.databaseName);
		} catch (err) {
			log.error(`Error while configuring Indexes for ${schema.name} collection:`, err.stack);
		}
		// });
		return true;
	}

	/**
	 * Return an array of id field names (e.g. _id, id, _key, _rev, etc) used by
	 * the DB this adapter supports.
	 *
	 * Jollof models do not store id fields and data in the same internal object, but keeps it
	 * seperate in order to support databases that expect Id fields be passed to them
	 * as meta seperate from the data (e.g. ArangoDB).
	 *
	 * Jollof models will use the first item in the array as the primary id field name.
	 * The complete list of Id fields are used to configure the Jollof Model's accessor object.
	 * e.g So it knows modelInstance._key is a valid reference that should be forwarded to modelInstance._ids._key
	 * @returns {string[]}
	 */
	getIdFields() {
		return [ '_id', '_key', '_rev' ]
	}

	* get( collectionName, id ) {
		return 'Got ' + schema.name;
	}

	* find( collectionName, criteria, changes ) {
		return 'List ' + schema.name;
	}

	* update( collectionName, criteria, newValues, opts ) {
		return yield this._db.collection(collectionName).updateByExample(criteria, newValues, opts);
	}

	* delete( collectionName, id, opts ) {

		return yield this._db.collection(collectionName).remove(id, opts);
	}

	/**
	 * persists the model's current state.
	 *
	 * Each adapter is responsible for deciding whether that should be a create or an update according
	 * to how it's database handles either.
	 *
	 * Each adapter is also responsible for
	 * @param model
	 */
	* save( model ) {

		//First determine if this came from the db by checking meta id fields. If it dd, replace. else save
		// let res;
		model._ids = yield this._db.collection(model._collectionName).save(model._data);

		//After a save, adapter is responsible for setting the model's meta and changind it's data as it sees fit

		// return _.assign(res, data);

	}
}

//Export a singleton of this adapter
let adapterSingleton;
module.exports = ( options )=> {
	if (!adapterSingleton)
		adapterSingleton = new JollofDataArangoDB(options);

	return adapterSingleton;
}