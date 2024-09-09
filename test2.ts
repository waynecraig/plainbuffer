import {
  ColumnType,
  ColumnValue,
  PlainBufferCell,
  PlainBufferOutputStream,
  PlainBufferRow,
} from "./plainbuffer2";

const out = new PlainBufferOutputStream();
const row = new PlainBufferRow();
const pk1 = new PlainBufferCell(Buffer.from("org_id"));
pk1.cellValue = new ColumnValue(ColumnType.STRING, "o1");
row.primaryKey.push(pk1);
const pk2 = new PlainBufferCell(Buffer.from("checkin_id"));
pk2.cellValue = new ColumnValue(ColumnType.STRING, "c1");
row.primaryKey.push(pk2);

row.writeRowWithHeader(out);
console.log(Buffer.from(out.toUint8Array()).toString("hex"));
