class ByteStream {
  protected closedShared: Int32Array; // Shared. Positive if stream closed
  protected outstandingShared: Int32Array; // Shared. Number of written bytes unread
  protected data: Uint8Array; // Shared. Ring buffer where data is read/written
  protected offset: number = 0; // Local.  Location of next byte to read/write

  protected get closed() {
    return Atomics.load(this.closedShared, 0) > 0;
  }

  protected get outstanding() {
    return Atomics.load(this.outstandingShared, 0);
  }

  constructor(protected buffer: SharedArrayBuffer) {
    if (buffer.byteLength < 16) throw new Error("Buffer too small");
    this.closedShared = new Int32Array(buffer, 0, 1);
    this.outstandingShared = new Int32Array(buffer, 4, 1);
    this.data = new Uint8Array(buffer, 8);
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
    Atomics.store(this.closedShared, 0, 0);
  }

  async write(src: Uint8Array, byteOffset?: number, length?: number) {
    if (this.closed) throw new Error("Stream closed");

    src = src.slice(byteOffset, length);
    while (src.length > 0) {
      while (this.outstanding !== 0) {
        // Wait until buffer has been completely drained by reader
        Atomics.notify(this.outstandingShared, 0, 1);
        await Promise.resolve();
      }

      // Write as much data as we can into the buffer

      const before = src.slice(0, this.data.length - this.offset);
      const after = src.slice(before.length);

      this.data.set(before, this.offset);
      this.data.set(after, 0);

      const chunkLen = Math.min(
        src.length,
        this.data.length - this.outstanding
      );
      this.offset = (this.offset + chunkLen) % this.data.length;
      Atomics.add(this.outstandingShared, 0, chunkLen);
      src = src.slice(chunkLen);
    }
  }

  close() {
    Atomics.store(this.closedShared, 0, 1); // Close the connection
    Atomics.notify(this.outstandingShared, 0, 1); // Wake up reader to start reading
  }
}

class Reader extends ByteStream {
  read(dst: Uint8Array, byteOffset?: number, length?: number): void {
    dst = dst.slice(byteOffset, length);
    while (dst.length > 0) {
      // If connection has been closed and there's nothing left to read, exit
      if (this.closed && this.outstanding === 0) return;

      // Block until writer has written data
      Atomics.wait(this.outstandingShared, 0, this.data.length);

      const chunkLen = Math.min(dst.length, this.outstanding);

      const before = dst.slice(0, this.data.length - this.offset);
      const after = dst.slice(before.length);

      before.set(this.data.slice(this.offset, this.offset + chunkLen));
      after.set(this.data.slice(0, chunkLen - before.length));

      this.offset = (this.offset + chunkLen) % this.data.length;
      Atomics.sub(this.outstandingShared, 0, chunkLen);
      dst = dst.slice(chunkLen);
    }
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
  private view: DataView;
  private atomicsView: Int32Array;

  private offset = 0;
  private encoder = new TextEncoder();

  constructor(public buffer: SharedArrayBuffer) {
    this.view = new DataView(buffer);
    this.atomicsView = new Int32Array(buffer);
  }

  async send(data: Serializable) {
    /* Wait for receiving end to finish processing message */
    while (Atomics.load(this.atomicsView, 0) !== 0) {
      await Promise.resolve();
    }

    console.log("sending: ", data);

    this.offset = 4;
    this.writeTagged(data);
    Atomics.store(this.atomicsView, 0, 1);
    Atomics.notify(this.atomicsView, 0, 1);
  }

  receive(): Serializable {
    /* Wait for data to arrive */
    Atomics.wait(this.atomicsView, 0, 0);
    this.offset = 4;
    const value = this.readTagged();

    console.log("receiving: ", value);

    Atomics.store(this.atomicsView, 0, 0);
    return value;
  }

  private writeTagged(data: Serializable) {
    if (data instanceof Date) return this.write(TypeTag.Date, data, true);
    if (data instanceof Uint8Array)
      return this.write(TypeTag.Uint8Array, data, true);

    switch (typeof data) {
      case "number":
        return this.write(TypeTag.Int32, data, true);

      case "bigint":
        return this.write(TypeTag.Int64, data, true);

      case "string":
        return this.write(TypeTag.String, data, true);

      case "object":
        if (data === null) throw new Error("Cannot serialize null");
        if (Array.isArray(data)) return this.write(TypeTag.Array, data, true);
        return this.write(TypeTag.Object, data, true);

      default:
        throw new Error(`Unsupported type: ${typeof data}`);
    }
  }

  private write<Tag extends TypeTag>(
    tag: Tag,
    data: TypeTagMap[Tag],
    tagged?: boolean
  ) {
    if (tagged) this.view.setUint8(this.offset++, tag);

    switch (tag) {
      case TypeTag.Int32:
        this.view.setInt32(this.offset, data as TypeTagMap[TypeTag.Int32]);
        this.offset += 4;
        break;

      case TypeTag.Int64:
        this.view.setBigInt64(this.offset, data as TypeTagMap[TypeTag.Int64]);
        this.offset += 8;
        break;

      case TypeTag.Date:
        const date = data as TypeTagMap[TypeTag.Date];
        this.write(TypeTag.Int64, BigInt(date.getTime()));
        break;

      case TypeTag.String:
        const str = data as TypeTagMap[TypeTag.String];
        this.write(TypeTag.Uint8Array, this.encoder.encode(str));
        break;

      case TypeTag.Uint8Array:
        const byteArray = data as TypeTagMap[TypeTag.Uint8Array];
        this.write(TypeTag.Int32, byteArray.length);
        const byteView = new Uint8Array(
          this.buffer,
          this.offset,
          byteArray.length
        );
        byteView.set(byteArray);
        this.offset += byteArray.length;
        break;

      case TypeTag.Array:
        const array = data as TypeTagMap[TypeTag.Array];
        this.write(TypeTag.Int32, array.length);
        for (const item of array) this.writeTagged(item);
        break;

      case TypeTag.Object:
        const entries = Object.entries(data);
        this.write(TypeTag.Int32, entries.length);
        for (const [key, value] of entries) {
          this.write(TypeTag.String, key);
          this.writeTagged(value);
        }
        break;

      default:
        throw new Error(`Unsupported tag: ${tag}`);
    }
  }

  private readTagged(): Serializable {
    const tag = this.view.getUint8(this.offset++) as TypeTag;
    return this.read(tag);
  }

  private read<Tag extends TypeTag>(tag: Tag): TypeTagMap[Tag] {
    switch (tag) {
      case TypeTag.Int32:
        const int32 = this.view.getInt32(this.offset);
        this.offset += 4;
        return int32 as TypeTagMap[Tag];

      case TypeTag.Int64:
        const int64 = this.view.getBigInt64(this.offset);
        this.offset += 8;
        return int64 as TypeTagMap[Tag];

      case TypeTag.Date:
        const epoch = Number(this.read(TypeTag.Int64));
        return new Date(epoch) as TypeTagMap[Tag];

      case TypeTag.String:
        const bytes = this.read(TypeTag.Uint8Array);
        return new TextDecoder().decode(bytes) as TypeTagMap[Tag];

      case TypeTag.Uint8Array:
        const byteLength = this.read(TypeTag.Int32);
        const slice = new Uint8Array(this.buffer, this.offset, byteLength);
        this.offset += byteLength;

        // Return a copy of the array to avoid sharing
        const byteArray = new Uint8Array(slice.length);
        byteArray.set(slice);
        return byteArray as TypeTagMap[Tag];

      case TypeTag.Array:
        const arrLength = this.read(TypeTag.Int32);
        const array: Serializable[] = [];
        for (let i = 0; i < arrLength; i++) array.push(this.readTagged());
        return array as TypeTagMap[Tag];

      case TypeTag.Object:
        const objLength = this.read(TypeTag.Int32);
        const obj: { [key: string]: Serializable } = {};
        for (let i = 0; i < objLength; i++) {
          const key = this.read(TypeTag.String);
          obj[key] = this.readTagged();
        }
        return obj as TypeTagMap[Tag];

      default:
        throw new Error(`Unsupported tag: ${tag}`);
    }
  }
}
