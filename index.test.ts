import {
  CellOp,
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
};

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
      attributes: [{ name: "name", value: "name", type: VariantType.STRING }],
    },
  ];
  const hex =
    "7500000001030402000000696405090000000001000000000000000a0a020304040000006e616d65050900000003040000006e616d650af409c7";

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
    "75000000010304040000006e616d65050900000003040000006e616d650af4030402000000696405010000000b0a6f0203040500000073636f726505090000000133333333337350400ac303040400000070617373050200000002010ae20958";

  testEncodeDecode(data, hex);
});

test("3 pks, 2 attr", () => {
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
      attributes: [
        { name: "level", value: 3n, type: VariantType.INTEGER },
        {
          name: "payload",
          value: new Uint8Array(Buffer.from("Hello")),
          type: VariantType.BLOB,
        },
      ],
    },
  ];
  const hex =
    "75000000010304040000006e616d65050900000003040000006e616d650af4030403000000627566050800000007030000000102030a6b030405000000696e6465780501000000090ae8020304050000006c6576656c05090000000003000000000000000a230304070000007061796c6f6164050a000000070500000048656c6c6f0a77096b";

  testEncodeDecode(data, hex);
});

test("4 pks, 4 attr", () => {
  const data: PlainBufferRow[] = [
    {
      primaryKey: [
        { name: "area", value: "a1", type: VariantType.STRING },
        { name: "index", value: 2n, type: VariantType.INTEGER },
        {
          name: "buf",
          value: new Uint8Array([1, 2, 3]),
          type: VariantType.BLOB,
        },
        { name: "id", type: VariantType.INF_MAX },
      ],
      attributes: [
        { name: "score", value: 65.8, type: VariantType.DOUBLE },
        { name: "pass", value: true, type: VariantType.BOOLEAN },
        { name: "level", value: 3n, type: VariantType.INTEGER },
        {
          name: "remark",
          value: "Hello",
          type: VariantType.STRING,
          ts: 1728143341651n,
        },
      ],
    },
  ];
  const hex =
    "7500000001030404000000617265610507000000030200000061310a6b030405000000696e64657805090000000002000000000000000a0b030403000000627566050800000007030000000102030a6b030402000000696405010000000a0a680203040500000073636f726505090000000133333333337350400ac303040400000070617373050200000002010ae20304050000006c6576656c05090000000003000000000000000a2303040600000072656d61726b050a000000030500000048656c6c6f0753b85e5d920100000aab095a";

  testEncodeDecode(data, hex);
});

test("2 rows", () => {
  const data: PlainBufferRow[] = [
    {
      primaryKey: [
        { name: "name", value: "name", type: VariantType.STRING },
        { name: "id", value: 1n, type: VariantType.INTEGER },
      ],
      attributes: [
        { name: "score", value: 65.8, type: VariantType.DOUBLE },
        { name: "pass", value: true, type: VariantType.BOOLEAN },
      ],
    },
    {
      primaryKey: [
        { name: "name", value: "name2", type: VariantType.STRING },
        { name: "id", value: 2n, type: VariantType.INTEGER },
      ],
      attributes: [
        { name: "score", value: 48, type: VariantType.DOUBLE },
        { name: "pass", value: false, type: VariantType.BOOLEAN },
      ],
    },
  ];
  const hex =
    "75000000010304040000006e616d65050900000003040000006e616d650af4030402000000696405090000000001000000000000000a0a0203040500000073636f726505090000000133333333337350400ac303040400000070617373050200000002010ae20943010304040000006e616d65050a00000003050000006e616d65320a25030402000000696405090000000002000000000000000a3f0203040500000073636f726505090000000100000000000048400a2303040400000070617373050200000002000ae5090e";

  testEncodeDecode(data, hex);
});

test("update put", () => {
  const data: PlainBufferRow[] = [
    {
      primaryKey: [
        { name: "area", value: "a1", type: VariantType.STRING },
        { name: "id", value: 2n, type: VariantType.INTEGER },
      ],
      attributes: [{ name: "score", value: 85.6, type: VariantType.DOUBLE }],
    },
  ];
  const hex =
    "7500000001030404000000617265610507000000030200000061310a6b030402000000696405090000000002000000000000000a3f0203040500000073636f726505090000000166666666666655400aa809c9";

  testEncodeDecode(data, hex);
});

test("update delete", () => {
  const data: PlainBufferRow[] = [
    {
      primaryKey: [
        { name: "area", value: "a1", type: VariantType.STRING },
        { name: "id", value: 2n, type: VariantType.INTEGER },
      ],
      attributes: [{ name: "score", ts: 1234n, op: CellOp.DeleteOneVersion }],
    },
  ];
  const hex =
    "7500000001030404000000617265610507000000030200000061310a6b030402000000696405090000000002000000000000000a3f0203040500000073636f7265060307d2040000000000000a7209f1";

  testEncodeDecode(data, hex);
});

test("update delete all", () => {
  const data: PlainBufferRow[] = [
    {
      primaryKey: [
        { name: "area", value: "a1", type: VariantType.STRING },
        { name: "id", value: 20000n, type: VariantType.INTEGER },
      ],
      attributes: [{ name: "pass", op: CellOp.DeleteAllVersions }],
    },
  ];
  const hex =
    "7500000001030404000000617265610507000000030200000061310a6b0304020000006964050900000000204e0000000000000a14020304040000007061737306010a9d09de";

  testEncodeDecode(data, hex);
});

test("update delete row", () => {
  const data: PlainBufferRow[] = [
    {
      primaryKey: [
        { name: "area", value: "a1", type: VariantType.STRING },
        { name: "id", value: 2n, type: VariantType.INTEGER },
      ],
      attributes: [],
      deleteMarker: true,
    },
  ];
  const hex =
    "7500000001030404000000617265610507000000030200000061310a6b030402000000696405090000000002000000000000000a3f080914";

  testEncodeDecode(data, hex);
});
