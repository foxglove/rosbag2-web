import { Rosbag2 } from "@foxglove/rosbag2";

import { BlobReader } from "./BlobReader";
import { SqliteSqljs } from "./SqliteSqljs";

export async function openFileSystemDirectoryEntry(
  folder: FileSystemDirectoryEntry,
  locateSqlJsWasm?: (file: string) => string,
): Promise<Rosbag2> {
  const files = await listFilesInDirectoryEntry(folder);
  const entries = files.map((file) => ({
    relativePath: file.webkitRelativePath,
    file: new BlobReader(file),
  }));
  const bag = new Rosbag2(
    folder.fullPath,
    entries,
    (fileEntry) => new SqliteSqljs(fileEntry.file, locateSqlJsWasm),
  );
  await bag.open();
  return bag;
}

export async function openFileSystemDirectoryHandle(
  folder: FileSystemDirectoryHandle,
  locateSqlJsWasm?: (file: string) => string,
): Promise<Rosbag2> {
  const files = await listFilesInDirectoryHandle(folder);
  const entries = files.map((file) => ({
    relativePath: file.webkitRelativePath,
    file: new BlobReader(file),
  }));
  const bag = new Rosbag2(
    folder.name,
    entries,
    (fileEntry) => new SqliteSqljs(fileEntry.file, locateSqlJsWasm),
  );
  await bag.open();
  return bag;
}

// FileSystemDirectoryEntry helpers

async function listFilesInDirectoryEntry(folder: FileSystemDirectoryEntry): Promise<File[]> {
  let files: File[] = [];

  const entries = await getFolderEntries(folder);
  for (const entry of entries) {
    if (entry.isDirectory) {
      files = files.concat(await listFilesInDirectoryEntry(entry as FileSystemDirectoryEntry));
    } else if (entry.isFile) {
      files.push(await getFile(entry as FileSystemFileEntry));
    }
  }

  return files;
}

function getFolderEntries(folder: FileSystemDirectoryEntry): Promise<FileSystemEntry[]> {
  return new Promise((resolve, reject) => folder.createReader().readEntries(resolve, reject));
}

function getFile(fileEntry: FileSystemFileEntry): Promise<File> {
  return new Promise((resolve, reject) => fileEntry.file(resolve, reject));
}

// FileSystemDirectoryHandle helpers

async function listFilesInDirectoryHandle(folder: FileSystemDirectoryHandle): Promise<File[]> {
  let files: File[] = [];

  for await (const handle of folder.values()) {
    if (handle.kind === "directory") {
      files = files.concat(await listFilesInDirectoryHandle(handle));
    } else if (handle.kind === "file") {
      files.push(await handle.getFile());
    }
  }

  return files;
}
