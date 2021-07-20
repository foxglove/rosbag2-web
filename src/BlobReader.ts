import { Filelike } from "@foxglove/rosbag2";

export class BlobReader implements Filelike {
  private blob_: Blob;
  private size_: number;

  constructor(blob: Blob) {
    this.blob_ = blob;
    this.size_ = blob.size;
  }

  async read(offset = 0, length = Math.max(0, this.size_ - offset)): Promise<Uint8Array> {
    return await new Promise((resolve, reject) => {
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

  async readAsText(): Promise<string> {
    return await new Promise((resolve, reject) => {
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

  async size(): Promise<number> {
    return this.size_;
  }

  async close(): Promise<void> {
    // no-op
  }
}
