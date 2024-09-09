import { encodePlainBuffer, decodePlainBuffer, Row, Cell, ValueType } from "./plainbuffer";

const row: Row = {
  primaryKey: [
    { name: "org_id", value: "o1", type: ValueType.String },
    { name: "checkin_id", value: "c1", type: ValueType.String },
  ],
  attributes: [],
};


const buf = encodePlainBuffer([row]);
console.log(buf.toString("hex"));

const rows = decodePlainBuffer(buf);
console.log(rows);