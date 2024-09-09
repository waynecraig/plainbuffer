import { decodePlainBuffer, encodePlainBuffer } from ".";

// 使用示例
const row = {
  primaryKey: [
    { name: "id", value: 1 },
    { name: "name", value: "John Doe" },
  ],
  attributes: [
    { name: "age", value: 30 },
    { name: "active", value: true },
    { name: "data", value: new Uint8Array([1, 2, 3, 4, 5]) },
  ],
};

const encodedBuffer = encodePlainBuffer([row]);
console.log("Encoded buffer:", new Uint8Array(encodedBuffer));

const decodedRow = decodePlainBuffer(encodedBuffer);
console.log("Decoded row:", decodedRow);

const row2 = {
  primaryKey: [
    { name: "org_id", value: "o1" },
    { name: "checkin_id", value: "c1" },
  ],
  attributes: [],
};

const encodedBuffer2 = encodePlainBuffer([row2]);
console.log(Buffer.from(encodedBuffer2).toString("hex"));
