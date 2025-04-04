enum TypeTag {
  Number = 1,
  BigInt,
  Date,
  Uint8Array,
  Array,
  Object,
}

export type Serializable =
  | number
  | bigint
  | Date
  | Uint8Array
  | Serializable[]
  | { [key: string]: Serializable };

export class Serializer {
  private view: DataView;
  private offset = 0;

  constructor(private buffer: SharedArrayBuffer) {
    this.view = new DataView(buffer);
  }

  private writeUint8(value: number) {
    this.view.setUint8(this.offset++, value);
  }

  private writeUint8Array(value: Uint8Array) {
    this.view.setUint32(this.offset, value.length);
    this.offset += 4;
    for (const v of value) this.writeUint8(v);
  }

  private writeFloat64(value: number) {
    this.view.setFloat64(this.offset, value, true);
    this.offset += 8;
  }

  private writeBigInt(value: bigint) {
    this.view.setBigInt64(this.offset, value);
    this.offset += 8;
  }

  private writeString(str: string) {
    this.writeUint8(str.length);
    for (let i = 0; i < str.length; i++)
      this.view.setUint8(this.offset++, str.charCodeAt(i));
  }

  private write(data: Serializable) {
    if (typeof data === "number") {
      this.writeUint8(TypeTag.Number);
      this.writeFloat64(data);
    } else if (typeof data === "bigint") {
      this.writeUint8(TypeTag.BigInt);
      this.writeBigInt(data);
    } else if (Array.isArray(data)) {
      this.writeUint8(TypeTag.Array);
      this.writeUint8(data.length);
      for (const item of data) this.write(item);
    } else if (data instanceof Uint8Array) {
      this.writeUint8(TypeTag.Uint8Array);
      this.writeUint8Array(data);
    } else if (data instanceof Date) {
      this.writeUint8(TypeTag.Date);
      this.writeFloat64(data.getTime());
    } else if (typeof data === "object" && data !== null) {
      this.writeUint8(TypeTag.Object);
      const keys = Object.keys(data);
      this.writeUint8(keys.length);
      for (const key of keys) {
        this.writeString(key);
        this.write(data[key]);
      }
    } else {
      throw new Error(`Unsupported data type: ${JSON.stringify(data)}`);
    }
  }

  serialize(data: Serializable) {
    this.offset = 0;
    this.write(data);
  }

  deserialize(): Serializable {
    this.offset = 0;
    return this.read();
  }

  private readUint8() {
    return this.view.getUint8(this.offset++);
  }

  private readUint8Array() {
    const length = this.view.getUint32(this.offset);
    this.offset += 4;
    const value = new Uint8Array(this.buffer, this.offset, length);
    this.offset += length;
    return value;
  }

  private readFloat64() {
    const value = this.view.getFloat64(this.offset, true);
    this.offset += 8;
    return value;
  }

  private readBigInt() {
    const value = this.view.getBigInt64(this.offset);
    this.offset += 8;
    return value;
  }

  private readString() {
    const length = this.readUint8();
    let str = "";
    for (let i = 0; i < length; i++)
      str += String.fromCharCode(this.readUint8());
    return str;
  }

  private read(): Serializable {
    const type = this.readUint8();
    if (type === TypeTag.Number) return this.readFloat64();
    if (type === TypeTag.BigInt) return this.readBigInt();
    if (type === TypeTag.Array) {
      const length = this.readUint8();
      return Array.from({ length }, () => this.read());
    }

    if (type === TypeTag.Uint8Array) {
      return this.readUint8Array();
    }

    if (type === TypeTag.Date) {
      return new Date(this.readFloat64());
    }

    if (type === TypeTag.Object) {
      const length = this.readUint8();
      const obj: Record<string, Serializable> = {};
      for (let i = 0; i < length; i++) obj[this.readString()] = this.read();
      return obj;
    }
    throw new Error(`Invalid serializer tag: ${type}`);
  }
}
