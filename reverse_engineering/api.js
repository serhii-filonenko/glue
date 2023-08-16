'use strict';
const logHelper = require('./logHelper');
const fs = require('fs');
const antlr4 = require('antlr4');
const HiveLexer = require('./parser/HiveLexer.js');
const HiveParser = require('./parser/HiveParser.js');
const hqlToCollectionsVisitor = require('./hqlToCollectionsVisitor.js');
const commandsService = require('./commandsService');
const ExprErrorListener = require('./antlrErrorListener');
const { adaptJsonSchema } = require('./adaptJsonSchema');
const schemaHelper = require("./schemaHelper");
const connectionHelper = require("./helpers/connectionHelper");
const { setDependencies, dependencies } = require('./appDependencies');

module.exports = {
	async connect(connectionInfo) {
		const connection = await connectionHelper.connect(connectionInfo);

		return connection;
	},

	disconnect(connectionInfo, cb) {
		connectionHelper.close();

		cb();
	},

	async testConnection(connectionInfo, logger, cb, app) {
		setDependencies(app);
		logInfo('Test connection', connectionInfo, logger);

		const connection = await this.connect(connectionInfo);
		const instance = connectionHelper.createInstance(connection, dependencies.lodash);

		try {
			await instance.getDatabases();
			cb();
		} catch (err) {
			logger.log('error', { message: err.message, stack: err.stack, error: err }, 'Connection failed');
			cb(err);
		}
	},

	async getDbCollectionsNames(connectionInfo, logger, cb, app) {
		setDependencies(app);

		logInfo('Retrieving databases and tables information', connectionInfo, logger);

		try {
			const connection = await this.connect(connectionInfo);
			const instance = connectionHelper.createInstance(connection, dependencies.lodash);
			const { databaseList, isFullyUploaded } = await instance.getDatabases();
			const dbsCollections = databaseList.map(async db => {
				const dbCollections = await instance.getTables(db.Name);
				return {
					dbName: db.Name,
					dbCollections,
					isEmpty: dbCollections.length === 0
				};
			});

			const result = await Promise.all(dbsCollections);

			if (isFullyUploaded) {
				return cb(null, result);
			}

			const loadMore = {
				dbName: 'Load more',
				loadMore: true,
			};

			cb(null, [ ...result, loadMore ]);
		} catch(err) {
			logger.log(
				'error',
				{ message: err.message, stack: err.stack, error: err },
				'Retrieving databases and tables information'
			);
			cb(err);
		}

	},

	async getDbCollectionsData(data, logger, cb, app) {
		setDependencies(app);
		logger.log('info', data, 'Retrieving schema', data.hiddenKeys);
		
		const { collectionData } = data;
		const databases = collectionData.dataBaseNames;
		const tables = collectionData.collections;


		try {
			const connection = await this.connect(data);
			const instance = connectionHelper.createInstance(connection, dependencies.lodash);

			const tablesDataPromise = databases.map(async dbName => {
				const dbDescription = await instance.getDatabaseDescription(dbName);
				const selectedDBTables = tables[dbName] || [];
				const dbTables = selectedDBTables.map(async tableName => {
					logger.progress({
						message: 'Getting table data',
						containerName: dbName,
						entityName: tableName
					});

					const tableData = await instance.getTable(dbName, tableName);
					const jsonSchema = getColumnsSchema([ ...tableData.columns, ...tableData.partitionKeys ])

					return {
						dbName,
						collectionName: tableData.name,
						bucketInfo: {
							description: dbDescription
						},
						entityLevel: {
							...tableData.entityLevelData,
							storedAsTable: 'input/output format',
						},
						documents: [],
						validation: {
							jsonSchema,
						}
					}
				});
				return await Promise.all(dbTables);
			});

			const tablesData = await Promise.all(tablesDataPromise);
			const flatTablesData = tablesData.reduce((acc, val) => acc.concat(val), []);
			cb(null, flatTablesData);
		} catch(err) {
			logger.log(
				'error',
				{ message: err.message, stack: err.stack, error: err },
				'Retrieving databases and tables information'
			);
			cb({ message: err.message, stack: err.stack });
		}
	},

	reFromFile: async (data, logger, callback, app) => {
		try {
			setDependencies(app);
			const _ = dependencies.lodash;
			const input = await handleFileData(data.filePath);
			const chars = new antlr4.InputStream(input);
			const lexer = new HiveLexer.HiveLexer(chars);

			const tokens = new antlr4.CommonTokenStream(lexer);
			const parser = new HiveParser.HiveParser(tokens);
			parser.removeErrorListeners();
			parser.addErrorListener(new ExprErrorListener());

			const tree = parser.statements();

			const hqlToCollectionsGenerator = new hqlToCollectionsVisitor();

			const commands = tree.accept(hqlToCollectionsGenerator);
			const { result, info, relationships } = commandsService.convertCommandsToReDocs(
                _.flatten(commands).filter(Boolean),
                input
            );
			callback(null, result, info, relationships, 'multipleSchema');
		} catch(err) {
			const { error, title, name } = err;
			const handledError = handleErrorObject(error || err, title || name);
			logger.log('error', handledError, title);
			callback(handledError);
		}
	},

	adaptJsonSchema,
};

const handleFileData = filePath => {
	return new Promise((resolve, reject) => {

		fs.readFile(filePath, 'utf-8', (err, content) => {
			if(err) {
				reject(err);
			} else {
				resolve(content);
			}
		});
	});
};

const handleErrorObject = (error, title) => {
	const errorProperties = Object.getOwnPropertyNames(error).reduce((accumulator, key) => ({ ...accumulator, [key]: error[key] }), {});

	return { title , ...errorProperties };
};

const getColumnsSchema = (columns) => {
	return columns.reduce((acc, item) => {
		const sanitizedTypeString = item.type.replace(/\s/g, '');
		let columnSchema = schemaHelper.getJsonSchema(sanitizedTypeString);
		schemaHelper.setProperty(item.name, columnSchema, acc);
		return acc;
	}, {});
};

const logInfo = (step, connectionInfo, logger) => {
	logger.clear();
	logger.log('info', logHelper.getSystemInfo(connectionInfo.appVersion), step);
	logger.log('info', connectionInfo, 'connectionInfo', connectionInfo.hiddenKeys);
};

