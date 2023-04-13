const mapSortColumns = (items) => {
    return items.map(item => ({
        name: item.Column,
        type: item.SortOrder === 1 ? 'ascending' : 'descending'
    }));
}

const getSerDeLibrary = (data = {}) => {
    return data.SerializationLibrary;
}

const mapSerDePaths = (_, data = {}) => {
    return _.get(data, 'Parameters.paths', '').split(',');
}

const mapSerDeParameters = (parameters = {}) => {
    return Object.entries(parameters).reduce((acc, [key, value]) => {
        if (key !== 'paths') {
            acc.push({ serDeKey: key, serDeValue: value });
            return acc;
        }
        return acc;
    }, []);
}

const getClassification = (parameters = {}) => {
    if (parameters.classification) {
        switch (parameters.classification.toLowerCase()) {
            case 'avro':
                return 'Avro';
            case 'csv':
                return 'CSV';
            case 'json':
                return 'JSON';
            case 'xml':
                return 'XML';
            case 'parquet':
                return 'Parquet';
            case 'orc':
                return 'ORC';
        }
    }
    return {};
}

const mapTableProperties = (parameters = {}) => {
    return Object.entries(parameters).reduce((acc, [key, value]) => {
        if (key === 'classification') {
            return acc;
        }
        return acc.concat({
            tablePropKey: key,
            tablePropValue: value
        });
    }, []);
}

const getNumBuckets = (numBuckets) => {
    return numBuckets < 1 ? undefined : numBuckets;
}

const mapTableData = (tableData, _) => {
    const partitionKeys = tableData.Table.PartitionKeys || [];

    return {
        name: tableData.Table.Name,
        entityLevelData: {
            description: tableData.Table.Description,
            externalTable: tableData.Table.TableType === 'EXTERNAL_TABLE',
            tableProperties: mapTableProperties(tableData.Table.Parameters),
            compositePartitionKey: partitionKeys.map(item => item.Name),
            compositeClusteringKey: tableData.Table.StorageDescriptor.BucketColumns,
            sortedByKey: mapSortColumns(tableData.Table.StorageDescriptor.SortColumns),
            compressed: tableData.Table.StorageDescriptor.Compressed,
            location: tableData.Table.StorageDescriptor.Location,
            numBuckets: getNumBuckets(tableData.Table.StorageDescriptor.NumberOfBuckets),
            StoredAsSubDirectories: tableData.Table.StorageDescriptor.StoredAsSubDirectories,
            inputFormatClassname: tableData.Table.StorageDescriptor.InputFormat,
            outputFormatClassname: tableData.Table.StorageDescriptor.OutputFormat,
            serDeLibrary: getSerDeLibrary(tableData.Table.StorageDescriptor.SerdeInfo),
            parameterPaths: mapSerDePaths(_, tableData.Table.StorageDescriptor.SerdeInfo),
            serDeParameters: mapSerDeParameters(tableData.Table.StorageDescriptor.SerdeInfo.Parameters),
            classification: getClassification(tableData.Table.Parameters),
        },
        partitionKeys: tableData.Table.PartitionKeys || [],
        columns: tableData.Table.StorageDescriptor.Columns.map(({ Type, Name }) => ({ name: Name, type: Type })),
    };
}

module.exports = {
    mapTableData,
};