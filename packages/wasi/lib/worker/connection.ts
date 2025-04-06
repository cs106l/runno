class ByteStream {
  protected closedShared: Int32Array; // Shared. Zero if stream closed
  protected outstandingShared: Int32Array; // Shared. Number of written bytes unread
  protected readIdxShared: Int32Array; // Shared. Position of next byte read
  protected writeIdxShared: Int32Array; // Shared. Position of next byte written
  protected data: Uint8Array; // Shared. Ring buffer where data is read/written

  /* Scratchpad for writing simple values to stream */
  protected scratch: Uint8Array;
  protected scratchView: DataView;

  protected get closed() {
    return Atomics.load(this.closedShared, 0) === 0;
  }

  protected get outstanding() {
    return Atomics.load(this.outstandingShared, 0);
  }

  constructor(protected buffer: SharedArrayBuffer) {
    if (buffer.byteLength <= 16) throw new Error("Buffer too small");
    this.closedShared = new Int32Array(buffer, 0, 1);
    this.outstandingShared = new Int32Array(buffer, 4, 1);
    this.readIdxShared = new Int32Array(buffer, 8, 1);
    this.writeIdxShared = new Int32Array(buffer, 12, 1);
    this.data = new Uint8Array(buffer, 16);

    const scratch = new ArrayBuffer(8);
    this.scratch = new Uint8Array(scratch);
    this.scratchView = new DataView(scratch);
  }

  writer(): Writer {
    return new Writer(this.buffer);
  }

  reader(): Reader {
    return new Reader(this.buffer);
  }
}

class Writer extends ByteStream {
  constructor(buffer: SharedArrayBuffer) {
    super(buffer);
    Atomics.store(this.closedShared, 0, 1); // Open stream for reading
    Atomics.notify(this.closedShared, 0, 1); // Wake up pending reader
  }

  private get offset() {
    return this.writeIdxShared[0];
  }

  private set offset(value: number) {
    this.writeIdxShared[0] = value;
  }

  async write(src: Uint8Array, begin?: number, end?: number) {
    src = src.subarray(begin, end);

    while (src.length > 0) {
      if (this.closed) throw new Error("Stream was closed");

      if (this.outstanding === this.data.length) {
        // Buffer is fully filled, let's drain it completely
        Atomics.notify(this.outstandingShared, 0, 1);

        // Naive wait. Could be made better w/ exponential backoff
        // or by using Atomics.waitAsync once it's implemented by more browsers
        while (this.outstanding > 0) await Promise.resolve();
      }

      // Write as much data as we can into the buffer

      const chunkLen = Math.min(
        src.length,
        this.data.length - this.outstanding
      );

      const before = src.subarray(0, this.data.length - this.offset);
      const after = src.subarray(before.length, chunkLen);

      this.data.set(before, this.offset);
      this.data.set(after, 0);

      this.offset = (this.offset + chunkLen) % this.data.length;
      Atomics.add(this.outstandingShared, 0, chunkLen);
      src = src.subarray(chunkLen);
    }
  }

  close() {
    Atomics.store(this.closedShared, 0, 1); // Close the connection
    Atomics.notify(this.outstandingShared, 0, 1); // Wake up reader to start reading
  }

  writeUint8(value: number) {
    this.scratchView.setUint8(0, value);
    return this.write(this.scratch, 0, 1);
  }

  writeInt32(value: number) {
    this.scratchView.setInt32(0, value);
    return this.write(this.scratch, 0, 4);
  }

  writeInt64(value: bigint) {
    this.scratchView.setBigInt64(0, value);
    return this.write(this.scratch, 0, 8);
  }
}

class Reader extends ByteStream {
  private get offset() {
    return this.readIdxShared[0];
  }

  private set offset(value: number) {
    this.readIdxShared[0] = value;
  }

  read(dst: Uint8Array, begin?: number, end?: number): number {
    // If stream is closed initially, wait for writer to open it
    Atomics.wait(this.closedShared, 0, 0);

    dst = dst.subarray(begin, end);
    let read = 0;
    while (dst.length > 0) {
      // If connection has been closed and there's nothing left to read, exit
      if (this.closed && this.outstanding === 0) return read;

      // Block until writer has written data
      Atomics.wait(this.outstandingShared, 0, 0);

      const chunkLen = Math.min(dst.length, this.outstanding);

      const before = this.data.subarray(this.offset, this.offset + chunkLen);
      const after = this.data.subarray(0, chunkLen - before.length);

      dst.set(before);
      dst.set(after, before.length);

      this.offset = (this.offset + chunkLen) % this.data.length;
      Atomics.sub(this.outstandingShared, 0, chunkLen);
      dst = dst.subarray(chunkLen);
      read += chunkLen;
    }

    return read;
  }

  readUint8(): number {
    if (this.read(this.scratch, 0, 1) !== 1)
      throw new Error("Not enough bytes for uint8");
    return this.scratchView.getUint8(0);
  }

  readInt32(): number {
    if (this.read(this.scratch, 0, 4) !== 4)
      throw new Error("Not enough bytes for int32");
    return this.scratchView.getInt32(0);
  }

  readInt64(): bigint {
    if (this.read(this.scratch, 0, 8) !== 8)
      throw new Error("Not enough bytes for int64");
    return this.scratchView.getBigInt64(0);
  }
}

enum TypeTag {
  Int32 = 1,
  Int64,
  Date,
  String,
  Uint8Array,
  Array,
  Object,
}

type TypeTagMap = {
  [TypeTag.Int32]: number;
  [TypeTag.Int64]: bigint;
  [TypeTag.Date]: Date;
  [TypeTag.String]: string;
  [TypeTag.Uint8Array]: Uint8Array;
  [TypeTag.Array]: Serializable[];
  [TypeTag.Object]: { [key: string]: Serializable };
};

export type Serializable =
  | TypeTagMap[Exclude<TypeTag, TypeTag.Array | TypeTag.Object>]
  | Serializable[]
  | { [key: string]: Serializable };

export class SerializedConnection {
  private stream: ByteStream;
  private encoder = new TextEncoder();

  constructor(public buffer: SharedArrayBuffer) {
    this.stream = new ByteStream(buffer);
  }

  async send(data: Serializable) {
    // console.log("sending: ", data);
    const writer = this.stream.writer();
    await this.writeTagged(writer, data);
    writer.close();
  }

  receive(): Serializable {
    const reader = this.stream.reader();
    const value = this.readTagged(reader);
    // console.log("receiving: ", value);
    return value;
  }

  private writeTagged(writer: Writer, data: Serializable) {
    if (data instanceof Date)
      return this.write(writer, TypeTag.Date, data, true);
    if (data instanceof Uint8Array)
      return this.write(writer, TypeTag.Uint8Array, data, true);

    switch (typeof data) {
      case "number":
        return this.write(writer, TypeTag.Int32, data, true);

      case "bigint":
        return this.write(writer, TypeTag.Int64, data, true);

      case "string":
        return this.write(writer, TypeTag.String, data, true);

      case "object":
        if (data === null) throw new Error("Cannot serialize null");
        if (Array.isArray(data))
          return this.write(writer, TypeTag.Array, data, true);
        return this.write(writer, TypeTag.Object, data, true);

      default:
        throw new Error(`Unsupported type: ${typeof data}`);
    }
  }

  private async write<Tag extends TypeTag>(
    writer: Writer,
    tag: Tag,
    data: TypeTagMap[Tag],
    tagged?: boolean
  ) {
    if (tagged) await writer.writeUint8(tag);

    switch (tag) {
      case TypeTag.Int32:
        await writer.writeInt32(data as TypeTagMap[TypeTag.Int32]);
        break;

      case TypeTag.Int64:
        await writer.writeInt64(data as TypeTagMap[TypeTag.Int64]);
        break;

      case TypeTag.Date:
        const date = data as TypeTagMap[TypeTag.Date];
        await this.write(writer, TypeTag.Int64, BigInt(date.getTime()));
        break;

      case TypeTag.String:
        const str = data as TypeTagMap[TypeTag.String];
        await this.write(writer, TypeTag.Uint8Array, this.encoder.encode(str));
        break;

      case TypeTag.Uint8Array:
        const byteArray = data as TypeTagMap[TypeTag.Uint8Array];
        await this.write(writer, TypeTag.Int32, byteArray.length);
        await writer.write(byteArray);
        break;

      case TypeTag.Array:
        const array = data as TypeTagMap[TypeTag.Array];
        await this.write(writer, TypeTag.Int32, array.length);
        for (const item of array) await this.writeTagged(writer, item);
        break;

      case TypeTag.Object:
        const entries = Object.entries(data);
        await this.write(writer, TypeTag.Int32, entries.length);
        for (const [key, value] of entries) {
          await this.write(writer, TypeTag.String, key);
          await this.writeTagged(writer, value);
        }
        break;

      default:
        throw new Error(`Unsupported tag: ${tag}`);
    }
  }

  private readTagged(reader: Reader): Serializable {
    const tag = reader.readUint8() as TypeTag;
    return this.read(reader, tag);
  }

  private read<Tag extends TypeTag>(reader: Reader, tag: Tag): TypeTagMap[Tag] {
    switch (tag) {
      case TypeTag.Int32:
        return reader.readInt32() as TypeTagMap[Tag];

      case TypeTag.Int64:
        return reader.readInt64() as TypeTagMap[Tag];

      case TypeTag.Date:
        const epoch = Number(this.read(reader, TypeTag.Int64));
        return new Date(epoch) as TypeTagMap[Tag];

      case TypeTag.String:
        const bytes = this.read(reader, TypeTag.Uint8Array);
        return new TextDecoder().decode(bytes) as TypeTagMap[Tag];

      case TypeTag.Uint8Array:
        const byteLength = this.read(reader, TypeTag.Int32);
        const byteArray = new Uint8Array(byteLength);
        if (reader.read(byteArray) != byteLength)
          throw new Error(`Uint8Array: not enough bytes read`);
        return byteArray as TypeTagMap[Tag];

      case TypeTag.Array:
        const arrLength = this.read(reader, TypeTag.Int32);
        const array: Serializable[] = [];
        for (let i = 0; i < arrLength; i++) array.push(this.readTagged(reader));
        return array as TypeTagMap[Tag];

      case TypeTag.Object:
        const objLength = this.read(reader, TypeTag.Int32);
        const obj: { [key: string]: Serializable } = {};
        for (let i = 0; i < objLength; i++) {
          const key = this.read(reader, TypeTag.String);
          obj[key] = this.readTagged(reader);
        }
        return obj as TypeTagMap[Tag];

      default:
        throw new Error(`Unsupported tag: ${tag}`);
    }
  }
}
