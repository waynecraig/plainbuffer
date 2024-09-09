enum Tag {
  Header = 0x75,
  PK = 0x01,
  Attr = 0x02,
  Cell = 0x03,
  CellName = 0x04,
  CellValue = 0x05,
  CellOp = 0x06,
  CellTs = 0x07,
  DeleteMarker = 0x08,
  RowChecksum = 0x09,
  CellChecksum = 0x0a,
}

enum ValueType {
  Integer = 0x0,
  Double = 0x1,
  Boolean = 0x2,
  String = 0x3,
  Null = 0x6,
  Blob = 0x7,
  InfMin = 0x9,
  InfMax = 0xa,
  AutoIncrement = 0xb,
}

enum CellOp {
  DeleteAllVersion = 0x01,
  DeleteOneVersion = 0x03,
}

interface Cell {
  name: string;
  value: any;
  type: ValueType;
  timestamp?: number;
  op?: CellOp;
}

interface Row {
  primaryKey: Cell[];
  attributes: Cell[];
  deleteMarker?: boolean;
}

function crc8(crc: number, data: number | Uint8Array): number {
  if (typeof data === "number") {
    crc ^= data;
  } else {
    for (let i = 0; i < data.length; i++) {
      crc ^= data[i];
    }
  }
  for (let i = 0; i < 8; i++) {
    if (crc & 0x80) {
      crc = ((crc << 1) ^ 0x07) & 0xff;
    } else {
      crc = (crc << 1) & 0xff;
    }
  }
  return crc;
}

function getValueChecksum(crc: number, value: any, type: ValueType): number {
  switch (type) {
    case ValueType.Integer:
      return crc8(crc, new Uint8Array(new BigInt64Array([value]).buffer));
    case ValueType.Double:
      return crc8(crc, new Uint8Array(new Float64Array([value]).buffer));
    case ValueType.Boolean:
      return crc8(crc, value ? 1 : 0);
    case ValueType.String:
      return crc8(crc, new TextEncoder().encode(value));
    case ValueType.Null:
      return crc;
    case ValueType.Blob:
      return crc8(crc, value);
    case ValueType.InfMin:
    case ValueType.InfMax:
    case ValueType.AutoIncrement:
      return crc;
    default:
      throw new Error(`Unsupported value type: ${type}`);
  }
}

function getCellChecksum(crc: number, cell: Cell): number {
  if (cell.name) {
    crc = crc8(crc, new TextEncoder().encode(cell.name));
  }

  if (cell.value !== undefined) {
    crc = getValueChecksum(crc, cell.value, cell.type);
  }

  if (cell.timestamp !== undefined) {
    crc = crc8(
      crc,
      new Uint8Array(new BigInt64Array([BigInt(cell.timestamp)]).buffer)
    );
  }

  if (cell.op !== undefined) {
    crc = crc8(crc, cell.op);
  }

  return crc;
}

function getRowChecksum(row: Row): number {
  let crc = 0;

  for (const cell of row.primaryKey) {
    crc = crc8(crc, getCellChecksum(0, cell));
  }

  for (const cell of row.attributes) {
    crc = crc8(crc, getCellChecksum(0, cell));
  }

  crc = crc8(crc, row.deleteMarker ? 1 : 0);

  return crc;
}

function encodePlainBuffer(rows: Row[]): Buffer {
  let buffer = Buffer.alloc(1024);
  let offset = 0;

  function ensureCapacity(additionalBytes: number) {
    if (offset + additionalBytes > buffer.length) {
      const newBuffer = Buffer.alloc(offset + additionalBytes + 1024);
      buffer.copy(newBuffer);
      buffer = newBuffer;
    }
  }

  function writeTag(tag: Tag) {
    ensureCapacity(4);
    buffer.writeUInt8(tag, offset);
    offset += 1;
    if (tag === Tag.Header) {
      offset += 3; // Padding
    }
  }

  function writeFormattedValue(type: ValueType, data: Buffer) {
    ensureCapacity(9 + data.length);
    buffer.writeUInt32LE(5 + data.length, offset);
    offset += 4;
    buffer.writeUInt8(type, offset);
    offset += 1;
    buffer.writeUInt32LE(data.length, offset);
    offset += 4;
    data.copy(buffer, offset);
    offset += data.length;
  }

  function encodeCellName(name: string) {
    const data = Buffer.from(name);
    writeTag(Tag.CellName);
    ensureCapacity(4 + data.length);
    buffer.writeUInt32LE(data.length, offset);
    offset += 4;
    data.copy(buffer, offset);
    offset += data.length;
  }

  function encodeCellValue(value: any, type: ValueType) {
    writeTag(Tag.CellValue);
    let data: Buffer;
    switch (type) {
      case ValueType.Integer:
        data = Buffer.alloc(8);
        data.writeBigInt64BE(BigInt(value));
        break;
      case ValueType.Double:
        data = Buffer.alloc(8);
        data.writeDoubleBE(value);
        break;
      case ValueType.Boolean:
        data = Buffer.from([value ? 1 : 0]);
        break;
      case ValueType.String:
        data = Buffer.from(value);
        break;
      // Add other types as needed
      default:
        throw new Error(`Unsupported value type: ${type}`);
    }
    writeFormattedValue(type, data);
  }

  function encodeCellOp(op: CellOp) {
    writeTag(Tag.CellOp);
    ensureCapacity(1);
    buffer.writeUInt8(op, offset);
    offset += 1;
  }

  function encodeCellTs(timestamp: number) {
    writeTag(Tag.CellTs);
    ensureCapacity(8);
    buffer.writeBigInt64BE(BigInt(timestamp), offset);
    offset += 8;
  }

  function encodeCell(cell: Cell) {
    writeTag(Tag.Cell);
    encodeCellName(cell.name);
    encodeCellValue(cell.value, cell.type);
    if (cell.op) {
      encodeCellOp(cell.op);
    }
    if (cell.timestamp) {
      encodeCellTs(cell.timestamp);
    }
    const checksum = getCellChecksum(0, cell);
    writeTag(Tag.CellChecksum);
    ensureCapacity(1);
    buffer.writeUInt8(checksum, offset);
    offset += 1;
  }

  function encodeCells(cells: Cell[], cellType: Tag) {
    writeTag(cellType);
    cells.forEach(encodeCell);
  }

  function encodeRow(row: Row) {
    encodeCells(row.primaryKey, Tag.PK);
    encodeCells(row.attributes, Tag.Attr);
    if (row.deleteMarker) {
      writeTag(Tag.DeleteMarker);
    }
    const checksum = getRowChecksum(row);
    writeTag(Tag.RowChecksum);
    ensureCapacity(1);
    buffer.writeUInt8(checksum, offset);
    offset += 1;
  }

  writeTag(Tag.Header);
  rows.forEach(encodeRow);

  return buffer.subarray(0, offset);
}

function decodePlainBuffer(buffer: Buffer): Row[] {
  let offset = 0;
  const rows: Row[] = [];

  function readTag(): Tag {
    const tag = buffer.readUInt8(offset++) as Tag;
    if (tag === Tag.Header) {
      offset += 3; // Skip 3 bytes of padding
    }
    return tag;
  }

  function readFormattedValue(): { type: ValueType; value: Buffer } {
    const type = buffer.readUInt8(offset++) as ValueType;
    const length = buffer.readUInt32LE(offset);
    offset += 4;
    const value = buffer.subarray(offset, offset + length);
    offset += length;
    return { type, value };
  }

  function decodeCellName(): string {
    const length = buffer.readUInt32LE(offset);
    offset += 4;
    const value = buffer.subarray(offset, offset + length);
    offset += length;
    return value.toString();
  }

  function decodeCellValue(): { value: any; type: ValueType } {
    const { type, value } = readFormattedValue();
    switch (type) {
      case ValueType.Integer:
        return { value: value.readBigInt64BE(), type };
      case ValueType.Double:
        return { value: value.readDoubleBE(), type };
      case ValueType.Boolean:
        return { value: value[0] !== 0, type };
      case ValueType.String:
        return { value: value.toString(), type };
      // Add other types as needed
      default:
        return { value, type };
    }
  }

  function decodeCellOp(): CellOp {
    return buffer.readUInt8(offset++) as CellOp;
  }

  function decodeCellTs(): number {
    const timestamp = Number(buffer.readBigInt64BE(offset));
    offset += 8;
    return timestamp;
  }

  function decodeCell(): Cell {
    const cell: Cell = {
      name: "",
      value: null,
      type: ValueType.Null,
    };

    while (offset < buffer.length) {
      const tag = readTag();
      switch (tag) {
        case Tag.CellName:
          cell.name = decodeCellName();
          break;
        case Tag.CellValue:
          const { value, type } = decodeCellValue();
          cell.value = value;
          cell.type = type;
          break;
        case Tag.CellOp:
          cell.op = decodeCellOp();
          break;
        case Tag.CellTs:
          cell.timestamp = decodeCellTs();
          break;
        case Tag.CellChecksum:
          offset += 1; // Skip checksum for now
          return cell;
        default:
          throw new Error(`Unexpected tag in cell: ${tag}`);
      }
    }

    throw new Error("Unexpected end of buffer");
  }

  function decodeCells(): Cell[] {
    const cells: Cell[] = [];
    while (readTag() === Tag.Cell) {
      cells.push(decodeCell());
    }
    offset -= 1; // Rewind to the non-Cell tag
    return cells;
  }

  function decodeRow(): Row {
    const row: Row = {
      primaryKey: [],
      attributes: [],
    };

    while (offset < buffer.length) {
      const tag = readTag();
      switch (tag) {
        case Tag.PK:
          row.primaryKey = decodeCells();
          break;
        case Tag.Attr:
          row.attributes = decodeCells();
          break;
        case Tag.DeleteMarker:
          row.deleteMarker = true;
          break;
        case Tag.RowChecksum:
          offset += 1; // Skip checksum for now
          return row;
        default:
          throw new Error(`Unexpected tag in row: ${tag}`);
      }
    }

    throw new Error("Unexpected end of buffer");
  }

  if (readTag() !== Tag.Header) {
    throw new Error("Invalid PlainBuffer format");
  }

  while (offset < buffer.length) {
    rows.push(decodeRow());
  }

  return rows;
}

export { encodePlainBuffer, decodePlainBuffer, Row, Cell, ValueType, CellOp };
