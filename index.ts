const HEADER = 0x75;

enum TagType {
  ROW_PK = 0x1,
  ROW_ATTR = 0x2,
  CELL = 0x3,
  CELL_NAME = 0x4,
  CELL_VALUE = 0x5,
  CELL_OP = 0x6,
  CELL_TS = 0x7,
  DELETE_ROW_MARKER = 0x8,
  ROW_CHECKSUM = 0x9,
  CELL_CHECKSUM = 0x0a,
}

enum VariantType {
  INTEGER = 0x0,
  DOUBLE = 0x1,
  BOOLEAN = 0x2,
  STRING = 0x3,
  NULL = 0x6,
  BLOB = 0x7,
  INF_MIN = 0x9,
  INF_MAX = 0xa,
  AUTO_INCREMENT = 0xb,
}

enum CellOp {
  DeleteAllVersions = 0x01,
  DeleteOneVersion = 0x03,
  Increment = 0x4,
}

type PlainBufferCell = {
  name: string;
  type?: VariantType;
  value?: bigint | number | boolean | string | Uint8Array;
  op?: CellOp;
  ts?: number; // number is enough for timestamp, no need for bigint
};

type PlainBufferRow = {
  primaryKey: PlainBufferCell[];
  attributes: PlainBufferCell[];
  deleteMarker?: boolean;
};

const allowedPrimaryKeyTypes = new Set([
  VariantType.INF_MIN,
  VariantType.INF_MAX,
  VariantType.AUTO_INCREMENT,
  VariantType.INTEGER,
  VariantType.STRING,
  VariantType.BLOB,
]);

const allowedAttributeTypes = new Set([
  VariantType.INTEGER,
  VariantType.DOUBLE,
  VariantType.BOOLEAN,
  VariantType.STRING,
  VariantType.BLOB,
]);

// Initialize CRC8 table
const crc8Table = new Uint8Array(256);
for (let i = 0; i < 256; i++) {
  let x = i;
  for (let j = 8; j > 0; j--) {
    if ((x & 0x80) !== 0) {
      x = (x << 1) ^ 0x07;
    } else {
      x = x << 1;
    }
  }
  crc8Table[i] = x & 0xff;
}

// CRC8 calculation function
function crc8(crc: number, data: Uint8Array): number {
  for (const byte of data) {
    crc = crc8Table[(crc ^ byte) & 0xff] as number;
  }
  return crc;
}

// functions for writing data to DataView
function writeByte(writer: DataView, offset: number, value: number): number {
  writer.setUint8(offset, value);
  return offset + 1;
}

function writeRawLittleEndian32(
  writer: DataView,
  offset: number,
  value: number
): number {
  writer.setInt32(offset, value, true);
  return offset + 4;
}

function writeRawLittleEndian64(
  writer: DataView,
  offset: number,
  value: bigint
): number {
  writer.setBigInt64(offset, value, true);
  return offset + 8;
}

function writeDouble(writer: DataView, offset: number, value: number): number {
  writer.setFloat64(offset, value, true);
  return offset + 8;
}

function writeBytes(
  writer: DataView,
  offset: number,
  value: Uint8Array
): number {
  for (let i = 0; i < value.length; i++) {
    writer.setUint8(offset + i, value[i] as number);
  }
  return offset + value.length;
}

function calculateBufferLength(rows: PlainBufferRow[]): number {
  let length = 4; // header
  for (const row of rows) {
    if (row.primaryKey.length > 0) {
      length += 1; // tag for primary key
      for (const pk of row.primaryKey) {
        length += calculateCellLength(pk);
      }
    }
    if (row.attributes && row.attributes.length > 0) {
      length += 1; // tag for attributes
      for (const attr of row.attributes) {
        length += calculateCellLength(attr);
      }
    }
    if (row.deleteMarker) {
      length += 1; // tag for delete marker
    }
    length += 2; // row checksum
  }
  return length;
}

function calculateCellLength(cell: PlainBufferCell): number {
  let length = 1; // tag for cell
  length += 1; // tag for cell name
  length += 4; // length of cell name
  length += new TextEncoder().encode(cell.name).length; // cell name
  if (cell.type !== undefined) {
    length += calculateCellValueLength(cell.type, cell.value); // cell value
  }
  if (cell.op !== undefined) {
    length += 1; // tag for cell type
    length += 1; // cell type
  }
  if (cell.ts !== undefined) {
    length += 1; // tag for cell timestamp
    length += 8; // cell timestamp
  }
  length += 2; // cell checksum
  return length;
}

function calculateCellValueLength(
  type: PlainBufferCell["type"],
  value: PlainBufferCell["value"]
): number {
  // CELL_VALUE tag(1) + length_prefix(4) + (type_tag + value_data)
  // type! is safe here because this function is only called when type !== undefined
  return 1 + 4 + calculateValueLength(type!, value);
}

// encoding function
function encodePlainBuffer(rows: PlainBufferRow[]): ArrayBuffer {
  const length = calculateBufferLength(rows);
  const buffer = new ArrayBuffer(length);
  const writer = new DataView(buffer);
  let offset = 0;

  // Write header
  offset = writeRawLittleEndian32(writer, offset, HEADER);

  for (const row of rows) {
    offset = encodeRow(writer, offset, row);
  }

  return buffer;
}

function encodeRow(
  writer: DataView,
  offset: number,
  row: PlainBufferRow
): number {
  let checksum = 0;

  // Write primary key
  offset = writeByte(writer, offset, TagType.ROW_PK);
  for (const pk of row.primaryKey) {
    if (pk.type && !allowedPrimaryKeyTypes.has(pk.type)) {
      throw new Error(`Invalid primary key type: ${pk.type}`);
    }
    const c = encodeCell(writer, offset, pk);
    offset = c.newOffset;
    checksum = crc8(checksum, new Uint8Array([c.checksum]));
  }

  // Write attributes
  if (row.attributes && row.attributes.length > 0) {
    offset = writeByte(writer, offset, TagType.ROW_ATTR);
    for (const attr of row.attributes) {
      if (attr.type && !allowedAttributeTypes.has(attr.type)) {
        throw new Error(`Invalid attribute type: ${attr.type}`);
      }
      const c = encodeCell(writer, offset, attr);
      offset = c.newOffset;
      checksum = crc8(checksum, new Uint8Array([c.checksum]));
    }
  }

  // Write delete marker
  if (row.deleteMarker) {
    offset = writeByte(writer, offset, TagType.DELETE_ROW_MARKER);
    checksum = crc8(checksum, new Uint8Array([1]));
  } else {
    checksum = crc8(checksum, new Uint8Array([0]));
  }

  // Write checksum
  offset = writeByte(writer, offset, TagType.ROW_CHECKSUM);
  offset = writeByte(writer, offset, checksum);

  return offset;
}

function encodeCell(
  writer: DataView,
  offset: number,
  cell: PlainBufferCell
): { newOffset: number; checksum: number } {
  offset = writeByte(writer, offset, TagType.CELL);
  let checksum = 0;

  // Write column name
  offset = writeByte(writer, offset, TagType.CELL_NAME);
  const nameBytes = new TextEncoder().encode(cell.name);
  offset = writeRawLittleEndian32(writer, offset, nameBytes.length);
  offset = writeBytes(writer, offset, nameBytes);
  checksum = crc8(checksum, nameBytes);

  // Write column value
  if (cell.type !== undefined) {
    const vstart = offset;
    offset = encodeCellValue(writer, offset, cell.type, cell.value);
    checksum = crc8(
      checksum,
      new Uint8Array(writer.buffer.slice(vstart + 5, offset))
    );
  }

  // Write column op (if any)
  if (cell.op !== undefined) {
    offset = writeByte(writer, offset, TagType.CELL_OP);
    offset = writeByte(writer, offset, cell.op);
  }

  // Write timestamp (if any)
  if (cell.ts !== undefined) {
    offset = writeByte(writer, offset, TagType.CELL_TS);
    offset = writeRawLittleEndian64(writer, offset, BigInt(cell.ts));
    checksum = crc8(
      checksum,
      new Uint8Array(writer.buffer.slice(offset - 8, offset))
    );
  }

  // op is after ts in checksum calculation
  if (cell.op !== undefined) {
    checksum = crc8(checksum, new Uint8Array([cell.op]));
  }

  // Write checksum
  offset = writeByte(writer, offset, TagType.CELL_CHECKSUM);
  offset = writeByte(writer, offset, checksum);

  return { newOffset: offset, checksum };
}

function encodeCellValue(
  writer: DataView,
  offset: number,
  type: PlainBufferCell["type"],
  value: PlainBufferCell["value"]
): number {
  // Write CELL_VALUE tag
  offset = writeByte(writer, offset, TagType.CELL_VALUE);

  // Calculate and write outer length (type_tag + value_data)
  // type! is safe here because this function is only called when type !== undefined
  const valueLength = calculateValueLength(type!, value);
  offset = writeRawLittleEndian32(writer, offset, valueLength);

  // Reuse core logic: write type tag + value data
  offset = writeRawValue(writer, offset, type!, value);

  return offset;
}

// decoding functions
function decodePlainBuffer(buffer: ArrayBuffer): PlainBufferRow[] {
  const reader = new DataView(buffer);
  let offset = 0;

  // read header
  const header = reader.getInt32(offset, true);
  offset += 4;
  if (header !== HEADER) {
    throw new Error("Invalid PlainBuffer header");
  }

  const rows: PlainBufferRow[] = [];

  while (offset < buffer.byteLength) {
    const { row, newOffset } = decodeRow(reader, offset);
    rows.push(row);
    offset = newOffset;
  }

  return rows;
}

function decodeRow(
  reader: DataView,
  offset: number
): { row: PlainBufferRow; newOffset: number } {
  const row: PlainBufferRow = { primaryKey: [], attributes: [] };
  let checksum = 0;
  while (true) {
    const tag = reader.getUint8(offset);
    offset += 1;

    switch (tag) {
      case TagType.ROW_PK:
        while (reader.getUint8(offset) === TagType.CELL) {
          const { cell, newOffset, cellChecksum } = decodeCell(reader, offset);
          checksum = crc8(checksum, new Uint8Array([cellChecksum]));
          row.primaryKey.push(cell);
          offset = newOffset;
        }
        break;
      case TagType.ROW_ATTR:
        while (reader.getUint8(offset) === TagType.CELL) {
          const { cell, newOffset, cellChecksum } = decodeCell(reader, offset);
          checksum = crc8(checksum, new Uint8Array([cellChecksum]));
          row.attributes.push(cell);
          offset = newOffset;
        }
        break;
      case TagType.DELETE_ROW_MARKER:
        row.deleteMarker = true;
        checksum = crc8(checksum, new Uint8Array([1]));
        break;
      case TagType.ROW_CHECKSUM: {
        if (!row.deleteMarker) {
          checksum = crc8(checksum, new Uint8Array([0]));
        }
        const _checksum = reader.getUint8(offset);
        offset += 1;
        if (_checksum !== checksum) {
          throw new Error("Row checksum mismatch");
        }
        return { row, newOffset: offset };
      }
      default:
        throw new Error(`Unexpected tag in row: ${tag}`);
    }
  }
}

function decodeCell(
  reader: DataView,
  offset: number
): { cell: PlainBufferCell; newOffset: number; cellChecksum: number } {
  const cell = {} as PlainBufferCell;
  offset += 1; // Skip CELL tag
  let checksum = 0;

  while (true) {
    const tag = reader.getUint8(offset);
    offset += 1;
    let bytes: Uint8Array;

    switch (tag) {
      case TagType.CELL_NAME: {
        const nameLength = reader.getInt32(offset, true);
        offset += 4;
        bytes = new Uint8Array(
          reader.buffer.slice(offset, offset + nameLength)
        );
        offset += nameLength;
        cell.name = new TextDecoder().decode(bytes);
        checksum = crc8(checksum, bytes);
        break;
      }
      case TagType.CELL_VALUE: {
        const valueLength = reader.getInt32(offset, true);
        offset += 4;
        bytes = new Uint8Array(
          reader.buffer.slice(offset, offset + valueLength)
        );
        offset += valueLength;
        checksum = crc8(checksum, bytes);
        const { type, value } = decodeCellValue(bytes);
        cell.type = type;
        cell.value = value;
        break;
      }
      case TagType.CELL_OP:
        cell.op = reader.getUint8(offset);
        offset += 1;
        break;
      case TagType.CELL_TS:
        cell.ts = Number(reader.getBigInt64(offset, true));
        bytes = new Uint8Array(reader.buffer.slice(offset, offset + 8));
        offset += 8;
        checksum = crc8(checksum, bytes);
        break;
      case TagType.CELL_CHECKSUM: {
        // op is after ts in checksum calculation
        if (cell.op !== undefined) {
          checksum = crc8(checksum, new Uint8Array([cell.op]));
        }
        const _checksum = reader.getUint8(offset);
        offset += 1;
        if (_checksum !== checksum) {
          throw new Error("Cell checksum mismatch");
        }
        return { cell, newOffset: offset, cellChecksum: checksum };
      }
      default:
        throw new Error(`Unexpected tag in cell: ${tag}`);
    }
  }
}

function decodeCellValue(bytes: Uint8Array): {
  type: PlainBufferCell["type"];
  value: PlainBufferCell["value"];
} {
  if (bytes.length === 0) {
    return { type: VariantType.NULL, value: undefined };
  }

  const reader = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let offset = 0;

  const type = reader.getUint8(offset);
  offset += 1;

  switch (type) {
    case VariantType.INF_MIN:
      return { type, value: undefined };
    case VariantType.INF_MAX:
      return { type, value: undefined };
    case VariantType.AUTO_INCREMENT:
      return { type, value: undefined };
    case VariantType.STRING: {
      const strLength = reader.getInt32(offset, true);
      offset += 4;
      const value = new TextDecoder().decode(
        new Uint8Array(reader.buffer.slice(offset, offset + strLength))
      );
      offset += strLength;
      return { type, value };
    }
    case VariantType.INTEGER: {
      const intValue = reader.getBigInt64(offset, true);
      offset += 8;
      return { type, value: intValue };
    }
    case VariantType.DOUBLE: {
      const doubleValue = reader.getFloat64(offset, true);
      offset += 8;
      return { type, value: doubleValue };
    }
    case VariantType.BOOLEAN: {
      const boolValue = reader.getUint8(offset) !== 0;
      offset += 1;
      return { type, value: boolValue };
    }
    case VariantType.BLOB: {
      const blobLength = reader.getInt32(offset, true);
      offset += 4;
      const blobValue = new Uint8Array(
        reader.buffer.slice(offset, offset + blobLength)
      );
      offset += blobLength;
      return { type, value: blobValue };
    }
    default:
      throw new Error(`Unsupported variant type: ${type}`);
  }
}

// Type inference for encodeColumnValue
function inferVariantType(value: unknown): {
  type: VariantType;
  processedValue: PlainBufferCell["value"];
} {
  if (value === null || value === undefined) {
    return { type: VariantType.NULL, processedValue: undefined };
  }
  if (typeof value === "bigint") {
    return { type: VariantType.INTEGER, processedValue: value };
  }
  if (typeof value === "number") {
    return { type: VariantType.DOUBLE, processedValue: value };
  }
  if (typeof value === "boolean") {
    return { type: VariantType.BOOLEAN, processedValue: value };
  }
  if (typeof value === "string") {
    return { type: VariantType.STRING, processedValue: value };
  }
  if (value instanceof Uint8Array) {
    return { type: VariantType.BLOB, processedValue: value };
  }
  throw new Error(`Unsupported value type: ${typeof value}`);
}

// Calculate the byte length of a value for encodeColumnValue
function calculateValueLength(
  type: VariantType,
  value: PlainBufferCell["value"]
): number {
  switch (type) {
    case VariantType.NULL:
    case VariantType.INF_MIN:
    case VariantType.INF_MAX:
    case VariantType.AUTO_INCREMENT:
      return 1; // Only type tag
    case VariantType.INTEGER:
    case VariantType.DOUBLE:
      return 9; // type(1) + value(8)
    case VariantType.BOOLEAN:
      return 2; // type(1) + value(1)
    case VariantType.STRING:
      return 5 + new TextEncoder().encode(value as string).length; // type(1) + length(4) + bytes
    case VariantType.BLOB:
      return 5 + (value as Uint8Array).length; // type(1) + length(4) + bytes
    default:
      throw new Error(`Unknown variant type: ${type}`);
  }
}

// Write raw value (type tag + value data) without CELL_VALUE tag and outer length prefix
function writeRawValue(
  writer: DataView,
  offset: number,
  type: VariantType,
  value: PlainBufferCell["value"]
): number {
  // Write type tag
  offset = writeByte(writer, offset, type);

  // Write value based on type
  switch (type) {
    case VariantType.NULL:
    case VariantType.INF_MIN:
    case VariantType.INF_MAX:
    case VariantType.AUTO_INCREMENT:
      // Only type tag, no value
      break;
    case VariantType.INTEGER:
      offset = writeRawLittleEndian64(writer, offset, value as bigint);
      break;
    case VariantType.DOUBLE:
      offset = writeDouble(writer, offset, value as number);
      break;
    case VariantType.BOOLEAN:
      offset = writeByte(writer, offset, value ? 1 : 0);
      break;
    case VariantType.STRING: {
      const bytes = new TextEncoder().encode(value as string);
      offset = writeRawLittleEndian32(writer, offset, bytes.length);
      offset = writeBytes(writer, offset, bytes);
      break;
    }
    case VariantType.BLOB:
      offset = writeRawLittleEndian32(
        writer,
        offset,
        (value as Uint8Array).length
      );
      offset = writeBytes(writer, offset, value as Uint8Array);
      break;
  }

  return offset;
}

/**
 * Encode a single column value to PlainBuffer format.
 * This is similar to Alibaba's TableStore.PlainBufferBuilder.serializeColumnValue().
 *
 * Type inference rules (following Alibaba's official implementation):
 * - bigint → INTEGER (VT_INTEGER: 0x0)
 * - number → DOUBLE (VT_DOUBLE: 0x1)
 * - boolean → BOOLEAN (VT_BOOLEAN: 0x2)
 * - string → STRING (VT_STRING: 0x3)
 * - Uint8Array → BLOB (VT_BLOB: 0x7)
 * - null/undefined → NULL (VT_NULL: 0x6)
 *
 * @param value - The value to encode
 * @returns Uint8Array containing the encoded value
 *
 * @example
 * ```typescript
 * encodeColumnValue("hello")
 * // Returns: Uint8Array [0x03, 0x05, 0x00, 0x00, 0x00, 0x68, 0x65, 0x6c, 0x6c, 0x6f]
 * // Hex: 030500000068656c6c6f
 * ```
 */
function encodeColumnValue(value: unknown): Uint8Array {
  const { type, processedValue } = inferVariantType(value);
  const length = calculateValueLength(type, processedValue);

  const buffer = new ArrayBuffer(length);
  const writer = new DataView(buffer);

  writeRawValue(writer, 0, type, processedValue);

  return new Uint8Array(buffer);
}

export {
  encodePlainBuffer,
  decodePlainBuffer,
  encodeColumnValue,
  TagType,
  VariantType,
  type PlainBufferRow,
  type PlainBufferCell,
  CellOp,
};
