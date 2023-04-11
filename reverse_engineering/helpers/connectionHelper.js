const aws = require('aws-sdk');
const fs = require('fs');
const https = require('https');
const {mapTableData} = require("./tablePropertiesHelper");

let connection;
let databaseLoadContinuationToken;

const MAX_RESULTS = 100;

const readCertificateFile = path => {
    if (!path) {
        return Promise.resolve('');
    }

    return new Promise(resolve => {
        fs.readFile(path, 'utf8', (err, data) => {
            if (err) {
                resolve('');
            }
            resolve(data);
        });
    });
};
const getSslOptions = async connectionInfo => {
    switch (connectionInfo.sslType) {
        case 'Server validation': {
            const certAuthority = await readCertificateFile(connectionInfo.certAuthorityPath);
            return {
                ssl: true,
                ca: [certAuthority],
            };
        }
        case 'Server and client validation': {
            const certAuthority = await readCertificateFile(connectionInfo.certAuthorityPath);
            const key = await readCertificateFile(connectionInfo.clientPrivateKey);
            const cert = await readCertificateFile(connectionInfo.clientCert);
            return {
                ssl: true,
                ca: [certAuthority],
                key: [key],
                cert: [cert],
                passphrase: connectionInfo.clientKeyPassword,
            };
        }
        default:
            return { ssl: false };
    }
};

const createConnection = async (connectionInfo) => {
    const { accessKeyId, secretAccessKey, region, sessionToken } = connectionInfo;
    const sslOptions = await getSslOptions(connectionInfo);
    const httpOptions = sslOptions.ssl ? {
        httpOptions: {
            agent: new https.Agent({
                rejectUnauthorized: true,
                ...sslOptions
            })},
        ...sslOptions
    } : {};

    aws.config.update({ accessKeyId, secretAccessKey, region, sessionToken, ...httpOptions });

    return new aws.Glue();
};
const connect = async (connectionInfo) => {
    if (connection) {
        return connection;
    }

    connection = await createConnection(connectionInfo);

    return connection;
};

const close = () => {
    if (connection) {
        connection = null;
    }
};

const createInstance = (connection, _) => {
    const getDatabases = async () => {
        const dbsData = await connection.getDatabases({ MaxResults: MAX_RESULTS, NextToken: databaseLoadContinuationToken }).promise();

        databaseLoadContinuationToken = dbsData.NextToken ? dbsData.NextToken : null;

        return {
            databaseList: dbsData.DatabaseList,
            isFullyUploaded: !Boolean(dbsData.NextToken),
        };
    };

    const getDatabaseDescription = async (dbName) => {
        const db = await connection.getDatabase({ Name: dbName }).promise();
        return db.Database.Description;
    };

    const getTableList = async (dbName, nextToken) => {
        const tableListResponse = await connection.getTables({ DatabaseName: dbName, ...(nextToken && { NextToken: nextToken}) }).promise();

        let nextTableList = [];
        if (tableListResponse.NextToken) {
            nextTableList = await getTableList(dbName, tableListResponse.NextToken);
        }

        return [ ...tableListResponse.TableList, ...nextTableList ];
    };

    const getTables = async (dbName) => {
        const dbCollectionsData = await getTableList(dbName);
        return dbCollectionsData.map(({ Name }) => Name);
    };

    const getTable = async (dbName, tableName) => {
        const rawTableData = await connection
            .getTable({ DatabaseName: dbName, Name: tableName })
            .promise();

        return mapTableData(rawTableData, _);
    };

    return {
        getDatabaseDescription,
        getDatabases,
        getTables,
        getTable,
    };
}

module.exports = {
    createInstance,
    connect,
    close,
};
