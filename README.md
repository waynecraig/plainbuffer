# PlainBuffer

阿里云表格存储 API 协议中的 PlainBuffer 的 TypeScript 实现。

## 官方文档

https://help.aliyun.com/zh/tablestore/developer-reference/plainbuffer

## 参考实现：

1. 官方 JavaScript SDK: https://github.com/aliyun/aliyun-tablestore-nodejs-sdk

2. 官方 Go SDK： https://github.com/aliyun/aliyun-tablestore-go-sdk

## 使用示例

编码:

```TypeScript
import {
  CellOp,
  encodePlainBuffer,
  PlainBufferRow,
  VariantType,
} from "plainbuffer";

const data: PlainBufferRow[] = [
  {
    primaryKey: [
      { name: "pk1", type: VariantType.STRING, value: "iampk" },
      { name: "pk2", type: VariantType.INTEGER, value: 100n },
    ],
    attributes: [
      { name: "column1", type: VariantType.STRING, value: "bad", ts: 1001n },
      { name: "column2", type: VariantType.INTEGER, value: 128n, ts: 1002n },
      { name: "column3", type: VariantType.DOUBLE, value: 34.2, ts: 1003n },
      { name: "column4", op: CellOp.DeleteAllVersions },
    ],
  },
];

const encoded = encodePlainBuffer(data);

console.log(Buffer.from(encoded).toString("hex"))
```

解码：

```TypeScript
import { decodePlainBuffer } from "plainbuffer";

const hex =
  "75000000010304040000006e616d65050900000003040000006e616d650af4030402000000696405010000000b0a6f0203040500000073636f726505090000000133333333337350400ac303040400000070617373050200000002010ae20958";

const buf = new Uint8Array(hex.match(/../g)!.map((h) => parseInt(h, 16)))
  .buffer;

const decoded = decodePlainBuffer(buf);

console.log(JSON.stringify(decoded, null, 4));
/*
[
  {
      "primaryKey": [
          {
              "name": "name",
              "type": 3,         // VariantType.STRING
              "value": "name"
          },
          {
              "name": "id",
              "type": 11         // VariantType.AUTO_INCREMENT
          }
      ],
      "attributes": [
          {
              "name": "score",
              "type": 1,         // VariantType.DOUBLE
              "value": 65.8
          },
          {
              "name": "pass",
              "type": 2,         // VariantType.BOOLEAN
              "value": true
          }
      ]
  }
]
*/
```

## 关于 BigInt 的处理

PlainBuffer 中的 integer 类型是 64 位的，本项目中使用 JavaScript 中的 BigInt 类型来表示，详见 https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/BigInt

为了支持 BigInt，使用 TypeScript 时，target 需要设置为 es2020 以上。
