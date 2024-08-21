const { TextDataReaderSettings, TextFileDataSource } = Spotfire.Dxp.Data.Import;
const { DataType } = Spotfire.Dxp.Data;
const { MemoryStream, StreamWriter, SeekOrigin } = System.IO;


// Python<->Spotfire DataTypes https://docs.tibco.com/pub/sf-pysrv/1.12.3/doc/html/TIB_sf-pysrv_install/pyinstall/topics/spotfire_and_python_data_type_mapping_.html
function inferDataType(
    value: any
): Spotfire.Dxp.Data.DataType {
    const type = typeof value;
    switch (type) {
        case "string":
            return DataType.String;
        case "number":
            // due to the way JavaScript handles numbers, values greater than MAX_SAFE_INTEGER
            //  may not be represented accurately due to limitations of floating-point precision
            //  in JavaScript.
            // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number/MAX_SAFE_INTEGER
            if (Number.isInteger(value)) {
                return value > Number.MAX_SAFE_INTEGER ? DataType.LongInteger : DataType.Integer;
            } else {
                return value > Number.MAX_SAFE_INTEGER ? DataType.SingleReal : DataType.Real;
            }
        case "boolean":
            return DataType.Boolean;
        case "object":
            if (value === null) return DataType.Undefined;
            if (value instanceof Date) return DataType.DateTime;
            return DataType.String;
        default:
            return DataType.Undefined;
    }
}

function translateDataType(
    type: string,
): Spotfire.Dxp.Data.DataType {
    switch (type.toLowerCase()) {
        case "string":
            return DataType.String;
        case "integer":
            return DataType.Integer;
        case "real":
            return DataType.Real;
        case "boolean":
            return DataType.Boolean;
        case "datetime":
            return DataType.DateTime;
        case "binary":
            return DataType.Binary;
        default:
            return DataType.Undefined;
    }
}

/*
* creates a new DataTable or replaces an existing one with a "fresh" copy of sourceTable
*
* @param document - Spotfire Document context
* @param tableName - the name of the table to create
* @param sourceTable - the DataTable to copy from
* @returns the new table
*/
export function createOrReplaceDataTable(
    document: Spotfire.Dxp.Application.Document,
    tableName: string,
    sourceTable: Spotfire.Dxp.Data.DataSource
): Spotfire.Dxp.Data.DataTable {
    if (document.Data.Tables.Contains(tableName)) {
        // safe to use ! here since we know the table exists
        const existingTable = document.Data.Tables.Item.get(tableName)!;
        document.Data.Tables.Remove(existingTable);
    }
    const newTable = document.Data.Tables.Add(tableName, sourceTable);
    return newTable;
}

export function createDataSourceFromCsv(
    csv: string,
    delimiter: string = ",",
    hasHeaderRow: boolean = true,
    hasTypeRow: boolean = false,
): Spotfire.Dxp.Data.DataSource {
    
    const memoryStream = new MemoryStream();
    const streamWriter = new StreamWriter(memoryStream);

    streamWriter.Write(csv);
    streamWriter.Flush();
    memoryStream.Seek(0, SeekOrigin.Begin);

    const readerSettings = new TextDataReaderSettings();
    readerSettings.Separator = delimiter;
    
    if (hasHeaderRow)
        readerSettings.AddColumnNameRow(0);

    const rows = csv.split("\n").map(row => row.split(delimiter));
    const columnCount = rows[0].length;

    // infer data type for each column based on the first row that is not a header
    for (let col = 0; col < columnCount; col++) {
        if (hasTypeRow) {
            const type = translateDataType(rows[1][col]);
            readerSettings.SetDataType(col, type);
        } else {
            const sampleValue = rows[hasHeaderRow ? 1 : 0][col];
            const inferredType = inferDataType(sampleValue);
            readerSettings.SetDataType(col, inferredType);
        }
    }

    const dataSource = new TextFileDataSource(memoryStream, readerSettings);

    return dataSource;
}
