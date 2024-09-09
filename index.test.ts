import {
  decodePlainBuffer,
  encodePlainBuffer,
  PlainBufferRow,
  VariantType,
} from ".";

test("encodePlainBuffer", () => {
  const row: PlainBufferRow = {
    primaryKey: [
      { name: "org_id", value: "o1", type: VariantType.STRING },
      { name: "checkin_id", value: "c1", type: VariantType.STRING },
    ],
    attributes: [],
  };

  const encodedBuffer = encodePlainBuffer([row]);
  const result = Buffer.from(encodedBuffer).toString("hex");
  expect(result).toBe(
    "75000000010304060000006f72675f6964050700000003020000006f310abe03040a000000636865636b696e5f69640507000000030200000063310aa009de"
  );
});

test("decodePlainBuffer", () => {
  const buf = Buffer.from(
    "75000000010304060000006f72675f6964050700000003020000006f310abe03040a000000636865636b696e5f69640507000000030200000063310aa009de",
    "hex"
  );

  const rows = decodePlainBuffer(
    buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength)
  );
  expect(rows).toEqual([
    {
      primaryKey: [
        { name: "org_id", value: "o1", type: VariantType.STRING },
        { name: "checkin_id", value: "c1", type: VariantType.STRING },
      ],
      attributes: [],
    },
  ]);
});

test("1 pk, 0 attr", () => {
  const data: PlainBufferRow[] = [
    {
      primaryKey: [{ name: "id", value: 1n, type: VariantType.INTEGER }],
      attributes: [],
    },
  ];
  const hex = "7500000001030402000000696405090000000001000000000000000a0a0982";
  const buf = new Uint8Array(
    (hex.match(/[\da-f]{2}/gi) as string[]).map(function (h) {
      return parseInt(h, 16);
    })
  ).buffer;
  expect(encodePlainBuffer(data)).toEqual(buf);
  expect(decodePlainBuffer(buf)).toEqual(data);
});
