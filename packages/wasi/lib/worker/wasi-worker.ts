import { WASI } from "../wasi/wasi";
import { WASIContextOptions, WASIContext } from "../wasi/wasi-context";
import type { WASIExecutionResult, WASIFS } from "../types";
import type { SyncDrive } from "../wasi/wasi-drive";
import { SerializedConnection } from "./serializer";

class BlockingDrive implements SyncDrive {
  fs: WASIFS = {};

  private serializer: SerializedConnection;

  constructor(private buffer: SharedArrayBuffer) {
    this.serializer = new SerializedConnection(buffer);
  }

  open = this.call("open");
  close = this.call("close");
  read = this.call("read");
  pread = this.call("pread");
  write = this.call("write");
  pwrite = this.call("pwrite");
  sync = this.call("sync");
  seek = this.call("seek");
  tell = this.call("tell");
  renumber = this.call("renumber");
  unlink = this.call("unlink");
  rename = this.call("rename");
  list = this.call("list");
  stat = this.call("stat");
  pathStat = this.call("pathStat");
  setFlags = this.call("setFlags");
  getFlags = this.call("getFlags");
  setSize = this.call("setSize");
  setAccessTime = this.call("setAccessTime");
  setModificationTime = this.call("setModificationTime");
  pathSetAccessTime = this.call("pathSetAccessTime");
  pathSetModificationTime = this.call("pathSetModificationTime");
  pathCreateDir = this.call("pathCreateDir");

  private call<Name extends Functions<SyncDrive>>(name: Name) {
    return (
      ...args: Parameters<SyncDrive[Name]>
    ): ReturnType<SyncDrive[Name]> => {
      sendMessage({
        target: "host",
        type: "drive",
        name,
        args,
      });

      // Note: the assumpption here is that every SyncDrive method returns something that is serializable!
      return this.serializer.receive() as ReturnType<SyncDrive[Name]>;
    };
  }
}

type WorkerWASIContext = Partial<
  Omit<WASIContextOptions, "stdin" | "stdout" | "stderr" | "debug">
>;

type StartWorkerMessage = {
  target: "client";
  type: "start";
  binaryURL: string;
  stdinBuffer: SharedArrayBuffer;
} & WorkerWASIContext;

export type WorkerMessage = StartWorkerMessage;

type StdoutHostMessage = {
  target: "host";
  type: "stdout";
  text: string;
};

type StderrHostMessage = {
  target: "host";
  type: "stderr";
  text: string;
};

type DebugHostMessage = {
  target: "host";
  type: "debug";
  name: string;
  args: string[];
  ret: number;
  data: { [key: string]: any }[];
};

type ResultHostMessage = {
  target: "host";
  type: "result";
  result: WASIExecutionResult;
};

type CrashHostMessage = {
  target: "host";
  type: "crash";
  error: {
    message: string;
    type: string;
  };
};

type Functions<T> = {
  [K in keyof T]: T[K] extends (...args: any[]) => any ? K : never;
}[keyof T];

type DriveHostMessage<
  Name extends Functions<SyncDrive> = Functions<SyncDrive>
> = {
  target: "host";
  type: "drive";
  name: Name;
  args: Parameters<SyncDrive[Name]>;
};

export type HostMessage =
  | StdoutHostMessage
  | StderrHostMessage
  | DebugHostMessage
  | ResultHostMessage
  | CrashHostMessage
  | DriveHostMessage;

onmessage = async (ev: MessageEvent) => {
  const data = ev.data as WorkerMessage;

  switch (data.type) {
    case "start":
      try {
        const result = await start(data.binaryURL, data.stdinBuffer, data);
        sendMessage({
          target: "host",
          type: "result",
          result,
        });
      } catch (e) {
        let error;
        if (e instanceof Error) {
          error = {
            message: e.message,
            type: e.constructor.name,
          };
        } else {
          error = {
            message: `unknown error - ${e}`,
            type: "Unknown",
          };
        }
        sendMessage({
          target: "host",
          type: "crash",
          error,
        });
      }

      break;
  }
};

function sendMessage(message: HostMessage) {
  postMessage(message);
}

async function start(
  binaryURL: string,
  stdinBuffer: SharedArrayBuffer,
  context: WorkerWASIContext
) {
  return WASI.start(
    fetch(binaryURL),
    new WASIContext({
      ...context,
      stdout: sendStdout,
      stderr: sendStderr,
      stdin: (maxByteLength) => getStdin(maxByteLength, stdinBuffer),
      debug: sendDebug,
    })
  );
}

function sendStdout(out: string) {
  sendMessage({
    target: "host",
    type: "stdout",
    text: out,
  });
}

function sendStderr(err: string) {
  sendMessage({
    target: "host",
    type: "stderr",
    text: err,
  });
}

function sendDebug(
  name: string,
  args: string[],
  ret: number,
  data: { [key: string]: any }[]
) {
  // this debug data comes through as part of a message
  // we need to make sure it can be encoded by sendMessage
  data = JSON.parse(JSON.stringify(data));
  sendMessage({
    target: "host",
    type: "debug",
    name,
    args,
    ret,
    data,
  });

  // TODO: debugging WASI supports substituting a return value
  //       but it's hard to do async, so lets just always return
  //       the same value
  return ret;
}

function getStdin(
  maxByteLength: number,
  stdinBuffer: SharedArrayBuffer
): string | null {
  // Wait until the integer at the start of the buffer has a length in it
  Atomics.wait(new Int32Array(stdinBuffer), 0, 0);

  // First four bytes are a Int32 of how many bytes are in the buffer
  const view = new DataView(stdinBuffer);
  const numBytes = view.getInt32(0);
  if (numBytes < 0) {
    view.setInt32(0, 0);
    return null;
  }

  const buffer = new Uint8Array(stdinBuffer, 4, numBytes);

  // Decode the buffer into text, but only as much as was asked for
  const returnValue = new TextDecoder().decode(buffer.slice(0, maxByteLength));

  // Rewrite the buffer with the remaining bytes
  const remaining = buffer.slice(maxByteLength, buffer.length);
  view.setInt32(0, remaining.byteLength);
  buffer.set(remaining);

  return returnValue;
}
