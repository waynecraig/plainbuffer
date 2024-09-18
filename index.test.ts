import {
  decodePlainBuffer,
  encodePlainBuffer,
  PlainBufferRow,
  VariantType,
} from ".";

const testEncodeDecode = (data: PlainBufferRow[], hex: string) => {
  const newHex = Buffer.from(encodePlainBuffer(data)).toString("hex");
  expect(newHex).toBe(hex);

  const b = Buffer.from(hex, "hex");
  const newData = decodePlainBuffer(
    b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength)
  );
  expect(newData).toEqual(data);
}

test("1 pk, 0 attr", () => {
  const data: PlainBufferRow[] = [
    {
      primaryKey: [{ name: "id", value: 1n, type: VariantType.INTEGER }],
      attributes: [],
    },
  ];
  const hex = "7500000001030402000000696405090000000001000000000000000a0a0982";

  testEncodeDecode(data, hex);
});

test("1 pk, 1 attr", () => {
  const data: PlainBufferRow[] = [
    {
      primaryKey: [{ name: "id", value: 1n, type: VariantType.INTEGER }],
      attributes: [
        { name: "name", value: "name", type: VariantType.STRING },
      ],
    },
  ];
  const hex = "7500000001030402000000696405090000000001000000000000000a0a020304040000006e616d65050900000003040000006e616d650af409c7";

  testEncodeDecode(data, hex);
});

test("2 pks, 2 attr", () => {
  const data: PlainBufferRow[] = [
    {
      primaryKey: [
        { name: "name", value: "name", type: VariantType.STRING },
        { name: "id", type: VariantType.AUTO_INCREMENT },
      ],
      attributes: [
        { name: "score", value: 65.8, type: VariantType.DOUBLE },
        { name: "pass", value: true, type: VariantType.BOOLEAN },
      ],
    },
  ];
  const hex =
    "75000000010304040000006e616d65050900000003040000006e616d650af4030402000000696405010000000b0a6f0203040500000073636f726505090000000133333333337350400ac303040400000070617373050200000002010ae20958"

  testEncodeDecode(data, hex);
});

test("3 pks, 0 attr", () => {
  const data: PlainBufferRow[] = [
    {
      primaryKey: [
        { name: "name", value: "name", type: VariantType.STRING },
        {
          name: "buf",
          value: new Uint8Array([1, 2, 3]),
          type: VariantType.BLOB,
        },
        { name: "index", type: VariantType.INF_MIN },
      ],
      attributes: [],
    },
  ];
  const hex = "75000000010304040000006e616d65050900000003040000006e616d650af4030403000000627566050800000007030000000102030a6b030405000000696e6465780501000000090ae80999"

  testEncodeDecode(data, hex);
});

test("4 pks, 0 attr", () => {
});
