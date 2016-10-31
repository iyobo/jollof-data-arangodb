/**
 * Created by iyobo on 2016-10-30.
 */
const arangojs = require('arangojs');
const log = require('jollof').log;
const co = require('co');


class JollofDataArangoDB {
	constructor( options ) {
		try {

			this._connectionOptions = options;

			this._db = arangojs(options);

			this._collections = [];
		} catch (err) {
			log.error(err.stack);
		}
	}

	configureCollection( schema ) {
		//Ensure collection is created
		const that = this;
		co(function*() {

			//Apparently in arango3, this can no longer be done
			// const dbinfo = yield that._db.createDatabase(that._connectionOptions.databaseName);
			// yield that._db.useDatabase(that._connectionOptions.databaseName);

			const collection = that._db.collection(schema.name);

			//Collection create gets it's own try-catch because we don't particularly care for a 'duplicate name' error
			try {
				yield collection.create();
			}catch(err){
				//No need to throw an error if collection already exists
			}

			try {
				//Ensure Indexes
				for (let key in schema.indexes) {
					const index = schema.indexes[ key ];
					// log.debug('Creating Index:',index);
					switch (key) {
						case 'geo':
							yield collection.createGeoIndex(index.point.fields, index.point.opts||{});
							break;
						case 'list':
							yield collection.createSkipList(index.fields, index.opts||{});
							break;
						case 'persistent':
							yield collection.createSkipList(index.fields, index.opts||{});
							break;
						case 'hash':
							yield collection.createHashIndex(index.fields, index.opts||{});
							break;
						case 'fullText':
							let field = '';
							if(Array.isArray(index.fields))
								yield collection.createFulltextIndex(index.fields[0], index.opts||{});
							else if(typeof index.fields === 'string')
								yield collection.createFulltextIndex(index.fields, index.opts||{});

							break;

					}
				}

				that._collections.push(schema.name);
				log.debug('ArangoDB adapter connected to DB: ' + that._connectionOptions.databaseName);
			} catch (err) {
				log.error(`Error while configuring Indexes for ${schema.name} collection:`,err.stack);
			}
		});

	}

	* get( collection, id ) {
		return 'Got ' + schema.name;
	}

	* list( collection, criteria, changes ) {
		return 'List ' + schema.name;
	}

	* update( collection, id ) {
		return "Hohoho";
	}

	* updateAll( collection, id ) {
		return "Hohoho";
	}

	* delete( collection, id ) {
		return "Hohoho";
	}

	* processIndexes( collection, indexes ) {

	}

	* save( collection, data ) {

	}

}
let adapterSingleton;
module.exports = ( options )=> {
	if (!adapterSingleton)
		adapterSingleton = new JollofDataArangoDB(options);

	return adapterSingleton;
}