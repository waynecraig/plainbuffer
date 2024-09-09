const HEADER = 0x75;

// Tag types
const TAG_ROW_PK = 0x1;
const TAG_ROW_DATA = 0x2;
const TAG_CELL = 0x3;
const TAG_CELL_NAME = 0x4;
const TAG_CELL_VALUE = 0x5;
const TAG_CELL_TYPE = 0x6;
const TAG_CELL_TIMESTAMP = 0x7;
const TAG_DELETE_ROW_MARKER = 0x8;
const TAG_ROW_CHECKSUM = 0x9;
const TAG_CELL_CHECKSUM = 0x0a;
const TAG_EXTENSION = 0x0b;
const TAG_SEQ_INFO = 0x0c;
const TAG_SEQ_INFO_EPOCH = 0x0d;
const TAG_SEQ_INFO_TS = 0x0e;
const TAG_SEQ_INFO_ROW_INDEX = 0x0f;

// Cell op types
const DELETE_ALL_VERSION = 0x1;
const DELETE_ONE_VERSION = 0x3;
const INCREMENT = 0x4;

// Variant types
const VT_INTEGER = 0x0;
const VT_DOUBLE = 0x1;
const VT_BOOLEAN = 0x2;
const VT_STRING = 0x3;
const VT_NULL = 0x6;
const VT_BLOB = 0x7;
const VT_INF_MIN = 0x9;
const VT_INF_MAX = 0xa;
const VT_AUTO_INCREMENT = 0xb;

const LITTLE_ENDIAN_32_SIZE = 4;
const LITTLE_ENDIAN_64_SIZE = 8;

const spaceSize = 256;
const crc8Table = new Uint8Array(spaceSize);

// Initialize CRC8 table
(function initCRC8Table() {
  for (let i = 0; i < spaceSize; i++) {
    let x = i;
    for (let j = 8; j > 0; j--) {
      if ((x & 0x80) !== 0) {
        x = ((x << 1) ^ 0x07) & 0xff;
      } else {
        x = (x << 1) & 0xff;
      }
    }
    crc8Table[i] = x;
  }
})();

function crc8Byte(crc: number, input: number): number {
  return crc8Table[(crc ^ input) & 0xff];
}

function crc8Int32(crc: number, input: number): number {
  for (let i = 0; i < 4; i++) {
    crc = crc8Byte(crc, input & 0xff);
    input >>= 8;
  }
  return crc;
}

function crc8Int64(crc: number, input: bigint): number {
  for (let i = 0; i < 8; i++) {
    crc = crc8Byte(crc, Number(input & 0xffn));
    input >>= 8n;
  }
  return crc;
}

function crc8Bytes(crc: number, input: Uint8Array): number {
  for (let i = 0; i < input.length; i++) {
    crc = crc8Byte(crc, input[i]);
  }
  return crc;
}

class PlainBufferOutputStream {
  private buffer: number[] = [];

  writeRawByte(value: number): void {
    this.buffer.push(value & 0xff);
  }

  writeRawLittleEndian32(value: number): void {
    this.writeRawByte(value & 0xff);
    this.writeRawByte((value >> 8) & 0xff);
    this.writeRawByte((value >> 16) & 0xff);
    this.writeRawByte((value >> 24) & 0xff);
  }

  writeRawLittleEndian64(value: bigint): void {
    for (let i = 0; i < 8; i++) {
      this.writeRawByte(Number(value & 0xffn));
      value >>= 8n;
    }
  }

  writeDouble(value: number): void {
    const buffer = new ArrayBuffer(8);
    new Float64Array(buffer)[0] = value;
    const int64 = new BigInt64Array(buffer)[0];
    this.writeRawLittleEndian64(int64);
  }

  writeBoolean(value: boolean): void {
    this.writeRawByte(value ? 1 : 0);
  }

  writeBytes(value: Uint8Array): void {
    this.buffer.push(...value);
  }

  writeHeader(): void {
    this.writeRawLittleEndian32(HEADER);
  }

  writeTag(tag: number): void {
    this.writeRawByte(tag);
  }

  writeCellName(name: Uint8Array): void {
    this.writeTag(TAG_CELL_NAME);
    this.writeRawLittleEndian32(name.length);
    this.writeBytes(name);
  }

  toUint8Array(): Uint8Array {
    return new Uint8Array(this.buffer);
  }
}

class PlainBufferInputStream {
  private buffer: Uint8Array;
  private position: number = 0;

  constructor(buffer: Uint8Array) {
    this.buffer = buffer;
  }

  readRawByte(): number {
    if (this.position >= this.buffer.length) {
      throw new Error("Unexpected end of input");
    }
    return this.buffer[this.position++];
  }

  readTag(): number {
    return this.readRawByte();
  }

  readRawLittleEndian64(): bigint {
    if (this.position + 8 > this.buffer.length) {
      throw new Error("Unexpected end of input");
    }
    let result = 0n;
    for (let i = 0; i < 8; i++) {
      result |= BigInt(this.buffer[this.position++]) << BigInt(i * 8);
    }
    return result;
  }

  readRawLittleEndian32(): number {
    if (this.position + 4 > this.buffer.length) {
      throw new Error("Unexpected end of input");
    }
    let result = 0;
    for (let i = 0; i < 4; i++) {
      result |= this.buffer[this.position++] << (i * 8);
    }
    return result;
  }

  readBoolean(): boolean {
    return this.readRawByte() !== 0;
  }

  readBytes(size: number): Uint8Array {
    if (this.position + size > this.buffer.length) {
      throw new Error("Unexpected end of input");
    }
    const result = this.buffer.slice(this.position, this.position + size);
    this.position += size;
    return result;
  }

  rewindPosition(): void {
    this.position--;
  }

  getPosition(): number {
    return this.position;
  }
}

enum ColumnType {
  INTEGER,
  DOUBLE,
  BOOLEAN,
  STRING,
  BINARY,
}

class ColumnValue {
  type: ColumnType;
  value: number | boolean | string | Uint8Array;

  constructor(type: ColumnType, value: number | boolean | string | Uint8Array) {
    this.type = type;
    this.value = value;
  }

  writeCellValue(output: PlainBufferOutputStream): void {
    output.writeTag(TAG_CELL_VALUE);
    switch (this.type) {
      case ColumnType.INTEGER:
        output.writeRawLittleEndian32(VT_INTEGER);
        output.writeRawLittleEndian64(BigInt(this.value as number));
        break;
      case ColumnType.DOUBLE:
        output.writeRawLittleEndian32(VT_DOUBLE);
        output.writeDouble(this.value as number);
        break;
      case ColumnType.BOOLEAN:
        output.writeRawLittleEndian32(VT_BOOLEAN);
        output.writeBoolean(this.value as boolean);
        break;
      case ColumnType.STRING:
        output.writeRawLittleEndian32(VT_STRING);
        const strBytes = new TextEncoder().encode(this.value as string);
        output.writeRawLittleEndian32(strBytes.length);
        output.writeBytes(strBytes);
        break;
      case ColumnType.BINARY:
        output.writeRawLittleEndian32(VT_BLOB);
        const binBytes = this.value as Uint8Array;
        output.writeRawLittleEndian32(binBytes.length);
        output.writeBytes(binBytes);
        break;
    }
  }

  getCheckSum(crc: number): number {
    switch (this.type) {
      case ColumnType.INTEGER:
        return crc8Int64(crc, BigInt(this.value as number));
      case ColumnType.DOUBLE:
        const buffer = new ArrayBuffer(8);
        new Float64Array(buffer)[0] = this.value as number;
        const int64 = new BigInt64Array(buffer)[0];
        return crc8Int64(crc, int64);
      case ColumnType.BOOLEAN:
        return crc8Byte(crc, (this.value as boolean) ? 1 : 0);
      case ColumnType.STRING:
        return crc8Bytes(crc, new TextEncoder().encode(this.value as string));
      case ColumnType.BINARY:
        return crc8Bytes(crc, this.value as Uint8Array);
    }
  }
}

class PlainBufferCell {
  cellName: Uint8Array;
  cellValue: ColumnValue | null;
  cellTimestamp: bigint;
  cellType: number;
  ignoreValue: boolean;
  hasCellTimestamp: boolean;
  hasCellType: boolean;
  isInfMax: boolean;

  constructor(cellName: Uint8Array) {
    this.cellName = cellName;
    this.cellValue = null;
    this.cellTimestamp = 0n;
    this.cellType = 0;
    this.ignoreValue = false;
    this.hasCellTimestamp = false;
    this.hasCellType = false;
    this.isInfMax = false;
  }

  writeCell(output: PlainBufferOutputStream): void {
    output.writeTag(TAG_CELL);
    output.writeCellName(this.cellName);
    if (!this.ignoreValue && this.cellValue) {
      this.cellValue.writeCellValue(output);
    }

    if (this.hasCellType) {
      output.writeTag(TAG_CELL_TYPE);
      output.writeRawByte(this.cellType);
    }

    if (this.hasCellTimestamp) {
      output.writeTag(TAG_CELL_TIMESTAMP);
      output.writeRawLittleEndian64(this.cellTimestamp);
    }

    output.writeTag(TAG_CELL_CHECKSUM);
    output.writeRawByte(this.getCheckSum(0));
  }

  getCheckSum(crc: number): number {
    crc = crc8Bytes(crc, this.cellName);
    if (!this.ignoreValue && this.cellValue) {
      crc = this.cellValue.getCheckSum(crc);
    }

    if (this.hasCellTimestamp) {
      crc = crc8Int64(crc, this.cellTimestamp);
    }
    if (this.hasCellType) {
      crc = crc8Byte(crc, this.cellType);
    }
    return crc;
  }
}

class PlainBufferRow {
  primaryKey: PlainBufferCell[];
  cells: PlainBufferCell[];
  hasDeleteMarker: boolean;
  extension: RecordSequenceInfo | null;

  constructor() {
    this.primaryKey = [];
    this.cells = [];
    this.hasDeleteMarker = false;
    this.extension = null;
  }

  writeRow(output: PlainBufferOutputStream): void {
    output.writeTag(TAG_ROW_PK);
    for (const pk of this.primaryKey) {
      pk.writeCell(output);
    }

    if (this.cells.length > 0) {
      output.writeTag(TAG_ROW_DATA);
      for (const cell of this.cells) {
        cell.writeCell(output);
      }
    }

    output.writeTag(TAG_ROW_CHECKSUM);
    output.writeRawByte(this.getCheckSum(0));
  }

  writeRowWithHeader(output: PlainBufferOutputStream): void {
    output.writeHeader();
    this.writeRow(output);
  }

  getCheckSum(crc: number): number {
    for (const cell of this.primaryKey) {
      crc = crc8Byte(crc, cell.getCheckSum(0));
    }

    for (const cell of this.cells) {
      crc = crc8Byte(crc, cell.getCheckSum(0));
    }

    crc = crc8Byte(crc, this.hasDeleteMarker ? 1 : 0);

    return crc;
  }
}

class RecordSequenceInfo {
  epoch: number;
  timestamp: bigint;
  rowIndex: number;

  constructor(epoch: number, timestamp: bigint, rowIndex: number) {
    this.epoch = epoch;
    this.timestamp = timestamp;
    this.rowIndex = rowIndex;
  }
}

function readCellValue(
  input: PlainBufferInputStream
): [ColumnValue | null, boolean] {
  input.readRawLittleEndian32(); // Skip size
  const tp = input.readRawByte();
  switch (tp) {
    case VT_INTEGER:
      return [
        new ColumnValue(
          ColumnType.INTEGER,
          Number(input.readRawLittleEndian64())
        ),
        false,
      ];
    case VT_DOUBLE:
      const buffer = new ArrayBuffer(8);
      new BigInt64Array(buffer)[0] = input.readRawLittleEndian64();
      return [
        new ColumnValue(ColumnType.DOUBLE, new Float64Array(buffer)[0]),
        false,
      ];
    case VT_BOOLEAN:
      return [new ColumnValue(ColumnType.BOOLEAN, input.readBoolean()), false];
    case VT_STRING:
      const strBytes = input.readBytes(input.readRawLittleEndian32());
      return [
        new ColumnValue(ColumnType.STRING, new TextDecoder().decode(strBytes)),
        false,
      ];
    case VT_BLOB:
      return [
        new ColumnValue(
          ColumnType.BINARY,
          input.readBytes(input.readRawLittleEndian32())
        ),
        false,
      ];
    case VT_INF_MAX:
      return [null, true];
    default:
      throw new Error("Unknown value type");
  }
}

function readCell(input: PlainBufferInputStream): PlainBufferCell {
  const cell = new PlainBufferCell(new Uint8Array(0));
  let tag = input.readTag();
  if (tag !== TAG_CELL_NAME) {
    throw new Error("Invalid tag");
  }

  cell.cellName = input.readBytes(input.readRawLittleEndian32());
  tag = input.readTag();

  if (tag === TAG_CELL_VALUE) {
    [cell.cellValue, cell.isInfMax] = readCellValue(input);
    tag = input.readTag();
  }
  if (tag === TAG_CELL_TYPE) {
    cell.cellType = input.readRawByte();
    cell.hasCellType = true;
    tag = input.readTag();
  }

  if (tag === TAG_CELL_TIMESTAMP) {
    cell.cellTimestamp = input.readRawLittleEndian64();
    cell.hasCellTimestamp = true;
    tag = input.readTag();
  }

  if (tag === TAG_CELL_CHECKSUM) {
    input.readRawByte(); // Skip checksum
  } else {
    throw new Error("No checksum");
  }

  return cell;
}

function readRowPk(input: PlainBufferInputStream): PlainBufferCell[] {
  const primaryKeyColumns: PlainBufferCell[] = [];

  let tag = input.readTag();
  while (tag === TAG_CELL) {
    primaryKeyColumns.push(readCell(input));
    tag = input.readTag();
  }

  input.rewindPosition(); // Rewind one byte

  return primaryKeyColumns;
}

function readRowData(input: PlainBufferInputStream): PlainBufferCell[] {
  const columns: PlainBufferCell[] = [];

  let tag = input.readTag();
  while (tag === TAG_CELL) {
    columns.push(readCell(input));
    tag = input.readTag();
  }

  input.rewindPosition(); // Rewind one byte

  return columns;
}

function readRowExtension(input: PlainBufferInputStream): RecordSequenceInfo {
  input.readRawLittleEndian32(); // Skip size
  let tag = input.readTag();
  if (tag !== TAG_SEQ_INFO) {
    throw new Error("Invalid tag");
  }

  input.readRawLittleEndian32(); // Skip size
  tag = input.readTag();
  if (tag !== TAG_SEQ_INFO_EPOCH) {
    throw new Error("Invalid tag");
  }
  const epoch = input.readRawLittleEndian32();

  tag = input.readTag();
  if (tag !== TAG_SEQ_INFO_TS) {
    throw new Error("Invalid tag");
  }
  const ts = input.readRawLittleEndian64();

  tag = input.readTag();
  if (tag !== TAG_SEQ_INFO_ROW_INDEX) {
    throw new Error("Invalid tag");
  }
  const rowIndex = input.readRawLittleEndian32();

  return new RecordSequenceInfo(epoch, ts, rowIndex);
}

function readRow(input: PlainBufferInputStream): PlainBufferRow {
  const row = new PlainBufferRow();
  let tag = input.readTag();
  if (tag === TAG_ROW_PK) {
    row.primaryKey = readRowPk(input);
    tag = input.readTag();
  }

  if (tag === TAG_ROW_DATA) {
    row.cells = readRowData(input);
    tag = input.readTag();
  }

  if (tag === TAG_DELETE_ROW_MARKER) {
    row.hasDeleteMarker = true;
    tag = input.readTag();
  }

  if (tag === TAG_EXTENSION) {
    row.extension = readRowExtension(input);
    tag = input.readTag();
  }

  if (tag === TAG_ROW_CHECKSUM) {
    input.readRawByte(); // Skip checksum
  } else {
    throw new Error("No checksum");
  }
  return row;
}

function readRowsWithHeader(buffer: Uint8Array): PlainBufferRow[] {
  const input = new PlainBufferInputStream(buffer);

  if (input.readRawLittleEndian32() !== HEADER) {
    throw new Error("Invalid header from plain buffer");
  }

  const rows: PlainBufferRow[] = [];

  while (input.getPosition() < buffer.length) {
    rows.push(readRow(input));
  }

  return rows;
}

export {
  PlainBufferOutputStream,
  PlainBufferInputStream,
  PlainBufferRow,
  PlainBufferCell,
  readRowsWithHeader,
  ColumnValue,
  ColumnType,
};

// Example usage:
// const buffer = new Uint8Array([...]);  // Your input buffer
// const rows = readRowsWithHeader(buffer);
// console.log(rows);

// For writing:
// const output = new PlainBufferOutputStream();
// const row = new PlainBufferRow();
// // ... populate row ...
// row.writeRowWithHeader(output);
// const result = output.toUint8Array();
