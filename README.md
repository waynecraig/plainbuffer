# PlainBuffer

阿里云表格存储 API 协议中的 PlainBuffer 的 TypeScript 实现。

## 官方文档

https://help.aliyun.com/zh/tablestore/developer-reference/plainbuffer

## 参考实现：

1. 官方 JavaScript SDK: https://github.com/aliyun/aliyun-tablestore-nodejs-sdk

2. 官方 Go SDK： https://github.com/aliyun/aliyun-tablestore-go-sdk

## 使用示例

```TypeScript
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
