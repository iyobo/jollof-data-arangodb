/**
 * Created by iyobo on 2016-10-30.
 */
const arangojs = require('arangojs');
const log = require('jollof').log;
const co = require('co');
const _ = require('lodash');
const aql = arangojs.aql;

/**
 * A Jollof Data Adapter for ArangoDB 3+
 */
class JollofDataArangoDB {

	/**
	 * Keep constructor free of async calls.
	 * Only use to set initial values like connection strings and similar sync commands.
	 * @param options
	 */
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
	 * Jollof will run this on boot. Place all async or 'talking to database' logic here.
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
			// log.debug(err.message);
		}

		//Ensure Indexes
		try {

			if (Array.isArray(schema.indexes)) {
				yield schema.indexes.map(function*( index ) {
					switch (index.type) {
						case 'geo':
							switch (index.subtype) {
								case 'point':
									return yield collection.createGeoIndex(index.fields, index.opts || {});
									break;
							}
							break;
						case 'list':
							return yield collection.createSkipList(index.fields, index.opts || {});
							break;
						case 'persistent':
							return yield collection.createPersistentIndex(index.fields, index.opts || {});
							break;
						case 'hash':
							return yield collection.createHashIndex(index.fields, index.opts || {});
							break;
						case 'fullText':
							let field = '';
							if (Array.isArray(index.fields))
								return yield collection.createFulltextIndex(index.fields[ 0 ], index.opts || {});
							else if (typeof index.fields === 'string')
								return yield collection.createFulltextIndex(index.fields, index.opts || {});

							break;

					}
				})
			}

			that._collections.push(schema.name);
			// log.debug(schema.name + ' model successfully using ArangoDB on DB: ' + that._connectionOptions.databaseName);
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
	 * The complete list of Id fields are used to configure the Jollof Model's accessor object.
	 * e.g So it knows modelInstance._key is a valid reference that should be forwarded to modelInstance._ids._key
	 * @returns {string[]}
	 */
	get idFields() {
		return [ '_id', '_key', '_rev' ]
	}

	/**
	 * This is the primary Id field name to be used in all models under this adapter.
	 * @returns {string}
	 */
	get idField() {
		return '_key';
	}

	_processCriteria( criteria ) {
		return criteria;
	}

	/**
	 * Get a single item by it's id
	 * @param collectionName
	 * @param id
	 * @returns {string}
	 */
	* findById( collectionName, id, params ) {
		try {
			const q = {};
			q[ this.idField ] = id;
			const cursor = yield this._db.collection(collectionName).byExample(q);
			return cursor.next();
		} catch (err) {
			log.error(err.stack);
			throw err;
		}
	}

	/**
	 *
	 * @param collectionName
	 * @param criteria
	 * @param params
	 * @returns {*}
	 */
	* findOne( collectionName, criteria, params ) {
		const cursor = yield this._db.collection(collectionName).byExample(this._processCriteria(criteria), params)
		return cursor.next();
	}

	/**
	 *
	 * @param collectionName
	 * @param criteria
	 * @param params
	 * @returns {*}
	 */
	* find( collectionName, criteria, params ) {
		const cursor = yield this._db.collection(collectionName).byExample(this._processCriteria(criteria), params)
		return cursor.all();
	}

	/**
	 *
	 * @param collectionName
	 * @param criteria
	 * @param params
	 * @returns {*}
	 */
	* findQL( collectionName, criteria, params ) {

		let pageQL = '';
		let sortQL='';
		let criteriaQL = '';

		const criteriaVars={}

		if (params.paging) {
			const skip = params.paging.limit * (params.paging.page - 1);
			pageQL = `LIMIT ${skip}, ${params.paging.limit}`;
		}
		if (params.sorting) {
			let sortBy = params.sorting.sort;
			if (sortBy === 'id')
				sortBy = this.idField;
			sortQL = `SORT row.${sortBy} ${params.sorting.order}`;
		}
		if (criteria) {
			const criteriaList = []
			_.each(criteria, (v,k)=>{
				criteriaList.push(`${k} == @${k}`);
				criteriaVars[k]==v;
			});

			criteriaQL = criteriaList.join(' && ')
		}

		let q = `
		FOR row IN ${collectionName}
			${criteriaQL}
			${sortQL}
			${pageQL}
			RETURN row
		`;

		let countQ = `
		FOR row IN ${collectionName}
			COLLECT WITH COUNT INTO length
    		RETURN length
		`;

		const queryObj = {
			query: q,
			bindVars: criteriaVars
		}

		const countQueryObj = {
			query: countQ,
			bindVars: criteriaVars
		}

		// console.log(q);
		var cursor = yield this._db.query(q, criteriaVars);
		var cursorCount = yield this._db.query(countQ, criteriaVars);

		// const cursor = yield this._db.collection(collectionName).byExample(this._processCriteria(criteria), curatedParams)
		return {
			items: yield cursor.all(),
			count: yield cursorCount.next()
		}
	}

	/**
	 *
	 * @param collectionName
	 * @param criteria
	 * @param newValues
	 * @param params
	 * @returns {*}
	 */
	* update( collectionName, criteria, newValues, params ) {
		const res = yield this._db.collection(collectionName).updateByExample(this._processCriteria(criteria), newValues, params);
		return res;
	}

	/**
	 *
	 * @param collectionName
	 * @param criteria
	 * @param newValues
	 * @param params
	 * @returns {*}
	 */
	* create( collectionName, data, params ) {
		const res = yield this._db.collection(collectionName).save(data);
		return res;
	}


	/**
	 *
	 * @param collectionName
	 * @param criteria
	 * @param params
	 * @returns {*}
	 */
	* remove( collectionName, criteria, params ) {
		const res = yield this._db.collection(collectionName).removeByExample(this._processCriteria(criteria), params);
		return res;
	}


	/**
	 * persists the model's current state.
	 *
	 * Each adapter is responsible for deciding whether it should be a create or an update depending on
	 * how it's database handles either.
	 *
	 * Each adapter is also responsible for setting the appropriate new values in the model.
	 * In this case, all that gets set is the _ids as this DB prefers keeping _data and _ids seperate.
	 *
	 * @param model
	 */
	* saveModel( model ) {

		if (model.isPersisted()) {
			const res = yield this._db.collection(model._collectionName).update(model._ids, model._data);
			return res;
		}
		else {
			model._ids = yield this._db.collection(model._collectionName).save(model._data);
			return model._ids;
		}
	}

	* removeModel( model ) {
		const cursor = yield this._db.collection(model._collectionName).remove(model._id, {});
		return cursor.all();
	}

	* runQuery( collectionName, queryFunc, params ) {
		let q = queryFunc(params);
		const cursor = yield this._db.query(q.query)

		return q.type === 'GET_ONE' ? cursor.next() : cursor.all();
	}
}

//Export a singleton of this adapter
let adapterSingleton;
module.exports = ( options )=> {
	if (!adapterSingleton)
		adapterSingleton = new JollofDataArangoDB(options);

	return adapterSingleton;
}