import { Filelike } from "@foxglove/rosbag2";

export class BlobReader implements Filelike {
  private blob_: Blob;
  private size_: number;

  constructor(blob: Blob) {
    this.blob_ = blob;
    this.size_ = blob.size;
  }

  read(offset?: number, length?: number): Promise<Uint8Array> {
    return new Promise((resolve, reject) => {
      offset ??= 0;
      length ??= Math.max(0, this.size_ - offset);

      const reader = new FileReader();
      reader.onload = function () {
        reader.onload = null;
        reader.onerror = null;
        resolve(new Uint8Array(reader.result as ArrayBuffer));
      };
      reader.onerror = function () {
        reader.onload = null;
        reader.onerror = null;
        reject(reader.error ?? new Error(`Unknown FileReader error`));
      };
      reader.readAsArrayBuffer(this.blob_.slice(offset, offset + length));
    });
  }

  readAsText(): Promise<string> {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = function () {
        reader.onload = null;
        reader.onerror = null;
        resolve(reader.result as string);
      };
      reader.onerror = function () {
        reader.onload = null;
        reader.onerror = null;
        reject(reader.error ?? new Error(`Unknown FileReader error`));
      };
      reader.readAsText(this.blob_, "utf8");
    });
  }

  size(): Promise<number> {
    return Promise.resolve(this.size_);
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}
