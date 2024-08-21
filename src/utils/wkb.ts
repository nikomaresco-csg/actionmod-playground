const { AddCalculatedColumnTransformation } = Spotfire.Dxp.Data.Transformations;
const { DataColumnSignature, AddColumnsSettings, JoinType, DataFlowBuilder, 
    DataSourcePromptMode, DataType, BinaryLargeObject } = Spotfire.Dxp.Data;
const { DataTableDataSource } = Spotfire.Dxp.Data.Import;
const { MemoryStream, StreamWriter, SeekOrigin } = System.IO;

import { Buffer } from "buffer";

/*
* Parse a WKT string into an array of [x, y] coordinate pairs
*
* @param wkt - the WKT string
* @returns an array of [x, y] coordinate pairs
*/
function parseWKT(
    wkt: string
): number[][] {
    // validate wkt string
    const matches = wkt.match(/\(\(([^)]+)\)\)/);
    if (!matches) throw new Error("Invalid WKT format");

    // parse coordinates and return as array of [x, y] pairs
    return matches[1].split(',').map(pair => {
        const [x, y] = pair.trim().split(' ').map(Number);
        return [x, y];
    });
}

/*
* Write a 32-bit unsigned integer to a buffer in little-endian format
*
* @param value - the value to write
* @param buffer - the buffer to write to
* @param offset - the offset in the buffer to write to
* @returns void
*/
function writeUInt32LE(
    value: number,
    buffer: Buffer,
    offset: number
): void {
    buffer.writeUInt32LE(value, offset);
}

/*
* Write a double to a buffer in little-endian format
*
* @param value - the value to write
* @param buffer - the buffer to write to
* @param offset - the offset in the buffer to write to
* @returns void
*/
function writeDoubleLE(
    value: number,
    buffer: Buffer,
    offset: number
): void {
    buffer.writeDoubleLE(value, offset);
}

/*
* Convert a Buffer to a Spotfire BinaryLargeObject
*
* @param buffer - the Buffer to convert
* @returns a Spotfire BinaryLargeObject
*/
export function bufferToBlo(
    buffer: Buffer
): Spotfire.Dxp.Data.BinaryLargeObject {
    const stream = new MemoryStream();
    const writer = new StreamWriter(stream);
    writer.Write(buffer);
    writer.Flush();
    stream.Seek(0, SeekOrigin.Begin);
    return BinaryLargeObject.Create(stream);
}

/*
* Convert a WKT string to a WKB buffer
*
* @param wkt - the WKT string
* @returns a Buffer containing the WKB representation of the WKT string
*/
export function wktToWkb(
    wkt: string
): Buffer {
    const coordinates = parseWKT(wkt);
    const numPoints = coordinates.length;

    // wkb header contains 1 byte for byte order plus 4 bytes for geometry type
    const headerSize = 1 + 4;
    // points contain 2 doubles of 8 bytes each
    const pointSize = 2 * 8;
    // plus 4 bytes for numPoints
    const bufferSize = 4 + headerSize + (numPoints * pointSize); 

    const buffer = Buffer.alloc(bufferSize);

    let offset = 0;

    // write byte order: little-endian (1)
    buffer.writeUInt8(1, offset);
    offset += 1;

    // write geometry type: polygon (3)
    writeUInt32LE(3, buffer, offset);
    offset += 4;

    // // write number of rings (1 for a simple polygon)
    // writeUInt32LE(1, buffer, offset);
    // offset += 4;

    // number of points
    writeUInt32LE(numPoints, buffer, offset);
    offset += 4;

    // write points
    // coordinates.forEach(([x, y]) => {
    //     writeDoubleLE(x, buffer, offset);
    //     writeDoubleLE(y, buffer, offset + 8);
    //     offset += 16;
    // });
    for (const point of coordinates) {
        if (offset + 16 > buffer.length) {
            throw new RangeError("Index out of range");
        }
        buffer.writeDoubleLE(point[0], offset);
        offset += 8;
        buffer.writeDoubleLE(point[1], offset);
        offset += 8;
    }

    return buffer;
}

/*
* Create required Calculated Columns from a WKB column for geospatial analysis
*
* @param importContext - the import context
* @param dataTable - the DataTable containing the WKB column
* @param wkbColumnName - the name of the WKB column
* @returns a tuple containing the DataFlow and DataRowReader
*/
export function createWkbSupportingColumns(
    importContext: Spotfire.Dxp.Data.Import.ImportContext,
    dataTable: Spotfire.Dxp.Data.DataTable,
    wkbColumnName: string
): [Spotfire.Dxp.Data.DataFlow, Spotfire.Dxp.Data.DataRowReader] {

    // the expressions used to build the WKB columns
    const wkbExpressions = [
        "WKBEnvelopeXCenter",
        "WKBEnvelopeYCenter",
        "WKBEnvelopeXMin",
        "WKBEnvelopeXMax",
        "WKBEvelopeYMin",
        "WKBEnvelopeYMax",
    ]

    const dataTableDataSource = new DataTableDataSource(dataTable);
    const builder = new DataFlowBuilder(dataTableDataSource, importContext);

    for (const expression of wkbExpressions) {
        const columnName = `${wkbColumnName}_${expression}`;
        const calculatedColumnTransformation = new AddCalculatedColumnTransformation(   
            columnName,
            `${expression}(${wkbColumnName})`
        );
        builder.AddTransformation(calculatedColumnTransformation);
    }

    const flow = builder.Build();
    const reader = builder.Execute(DataSourcePromptMode.None);

    return [flow, reader];
}