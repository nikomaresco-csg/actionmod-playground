import { createDataSourceFromCsv, createOrReplaceDataTable } from "../utils/data";
import { wktToWkb, bufferToBlo, createWkbSupportingColumns } from "../utils/wkb";

function generateCircleWKT(centerX: number, centerY: number, radius: number, numSides: number = 32): string {
    let points = [];
    const angleStep = 2 * Math.PI / numSides;

    for (let i = 0; i <= numSides; i++) {
        const angle = angleStep * i;
        const x = centerX + radius * Math.cos(angle);
        const y = centerY + radius * Math.sin(angle);
        points.push(`${x} ${y}`);
    }

    // WKT format for a polygon is: POLYGON ((x1 y1, x2 y2, ..., xn yn, x1 y1))
    return `POLYGON ((${points.join(', ')}))`;
}

function generateSquareWKT(lowerLeftX: number, lowerLeftY: number, sideLength: number): string {
    const upperRightX = lowerLeftX + sideLength;
    const upperRightY = lowerLeftY + sideLength;

    // WKT format for a polygon is: POLYGON ((x1 y1, x2 y2, x3 y3, x4 y4, x1 y1))
    // For a square, x1 y1 is the lower left corner, x2 y2 is the lower right corner,
    // x3 y3 is the upper right corner, x4 y4 is the upper left corner, and we repeat x1 y1 to close the polygon.
    return `POLYGON ((${lowerLeftX} ${lowerLeftY}, ${upperRightX} ${lowerLeftY}, ${upperRightX} ${upperRightY}, ${lowerLeftX} ${upperRightY}, ${lowerLeftX} ${lowerLeftY}))`;
}

/**
 * The entry point of a script. This function will be passed the parameters
 * specified in the manifest.
 */
function createTestTable({
    document,
    application,
}: CreateTestTableParameters) {

    // generate a csv with an id column and a random square WKT column
    const csv = "id;x;y;geometry\nInteger;Real;Real;Binary\n" +
        Array.from({ length: 10 }, (_, i) => {
            const lowerLeftX = Math.random() * 100;
            const lowerLeftY = Math.random() * 100;
            const sideLength = Math.random() * 10;
            const wkt = generateSquareWKT(lowerLeftX, lowerLeftY, sideLength);
            const wkb = wktToWkb(wkt).toString("hex");
            return `${i};${lowerLeftX};${lowerLeftY};${wkb}`;
        }).join("\n");

    console.log("csv:");
    console.log(csv);

    const dataSource = createDataSourceFromCsv(csv, ";", true, true);
    createOrReplaceDataTable(document, "Squares", dataSource);
}

// Registers the entry point such that Spotfire can invoke it in cases whereinstall 
// the name has been minified or moved into a local scope.
RegisterEntryPoint(createTestTable);
