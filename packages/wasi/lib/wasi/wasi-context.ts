import { WASIFS } from "../types";
import { DebugFn } from "./wasi";
import { SyncDrive, WASIDrive } from "./wasi-drive";

export type WASIContextOptions = {
  fs: WASIFS | SyncDrive;
  args: string[];
  env: Record<string, string>;
  stdin: (maxByteLength: number) => string | null;
  stdout: (out: string) => void;
  stderr: (err: string) => void;
  debug: DebugFn;
  isTTY: boolean;
};

function splitFSDrive(fs?: WASIContextOptions["fs"]): [WASIFS, SyncDrive] {
  if (!fs) {
    const wfs: WASIFS = {};
    return [wfs, new WASIDrive(wfs)];
  }

  // This is a hacky way of distinguishing between a WASIFS and SyncDrive
  // but it should work since a WASIFS is plain data
  if ("open" in fs && typeof fs.open === "function") {
    const drive = fs as SyncDrive;
    return [drive.fs, drive];
  }

  const wfs = fs as WASIFS;
  return [wfs, new WASIDrive(wfs)];
}

/**
 * WASIContext
 *
 * The context in which a WASI binary is executed.
 * This is used for syscalls that get args, environment, use IO and read/write
 * to files. This gives you programmattic access to the environment that the
 * WASI binary is executing in.
 *
 * Notes:
 *
 * stdin - string is passed as JavaScript string but encoded to UTF-8, if this
 *         ends up longer than maxByteLength it will be truncated.
 *
 * debug - function injected between wasi and return for debugging
 *
 */
export class WASIContext {
  fs: WASIFS;
  drive: SyncDrive;
  args: string[]; // Program args (like from a terminal program)
  env: Record<string, string>; // Environment (like a .env file)
  stdin: WASIContextOptions["stdin"];
  stdout: WASIContextOptions["stdout"];
  stderr: WASIContextOptions["stderr"];
  debug?: WASIContextOptions["debug"];
  isTTY: WASIContextOptions["isTTY"];

  constructor(options?: Partial<WASIContextOptions>) {
    [this.fs, this.drive] = splitFSDrive(options?.fs);
    this.args = options?.args ?? [];
    this.env = options?.env ?? {};

    this.stdin = options?.stdin ?? (() => null);
    this.stdout = options?.stdout ?? (() => {});
    this.stderr = options?.stderr ?? (() => {});
    this.debug = options?.debug;
    this.isTTY = !!options?.isTTY;
  }
}
