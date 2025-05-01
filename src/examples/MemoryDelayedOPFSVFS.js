import { FacadeVFS } from "../FacadeVFS.js";
import * as VFS from "../VFS.js";

// Memory-based VFS with asynchronous OPFS persistence.
export class MemoryDelayedOPFSVFS extends FacadeVFS {
  // Map of existing files, keyed by filename.
  mapNameToFile = new Map();

  // Map of open files, keyed by id (sqlite3_file pointer).
  mapIdToFile = new Map();

  // OPFS root directory handle
  #rootDir = null;

  // OPFS file handle for the database
  #dbFileHandle = null;

  // Track if OPFS is ready
  #opfsReady = null;

  // Database filename in OPFS
  #opfsFilename = "db.sqlite";

  // Queue for pending writes to OPFS - stores regions to be written
  #writeQueue = [];

  // Flag to track if write processing is active
  #isProcessingWrites = false;

  // Buffer for loaded OPFS data (not immediately added to mapNameToFile)
  #opfsDataBuffer = null;

  static async create(name, module) {
    const vfs = new MemoryDelayedOPFSVFS(name, module);
    await vfs.isReady();
    return vfs;
  }

  constructor(name, module) {
    super(name, module);
    // Initialize OPFS
    this.#opfsReady = this.#initOpfs();
  }

  async isReady() {
    return this.#opfsReady;
  }

  async #initOpfs() {
    try {
      // Get a handle to the file in OPFS (creating if necessary)
      this.#rootDir = await navigator.storage.getDirectory();
      this.#dbFileHandle = await this.#rootDir.getFileHandle(
        this.#opfsFilename,
        { create: true }
      );

      // Load existing data from OPFS
      const file = await this.#dbFileHandle.getFile();
      const data = await file.arrayBuffer();

      // Store the raw data in our buffer rather than adding it to mapNameToFile
      this.#opfsDataBuffer = data;
      console.log(`Loaded ${data.byteLength} bytes from OPFS into buffer`);

      return true;
    } catch (e) {
      console.error(`Failed to initialize OPFS: ${e.message}`);
      return false;
    }
  }

  #isTrackedDbFile(pathname) {
    // Check if the pathname matches our OPFS filename
    return pathname === `/${this.#opfsFilename}`;
  }

  /**
   * Add a write operation to the queue
   * @param {number} offset - The offset at which to write
   * @param {number} length - The length of data to write
   */
  #queueWrite(offset, length) {
    // Add write operation to queue - we only store the range, not the data itself
    this.#writeQueue.push({
      type: "write",
      offset,
      length,
    });

    this.#triggerWrites();
  }

  /**
   * Add a truncate operation to the queue
   * @param {number} size - The new size to truncate to
   */
  #queueTruncate(size) {
    // Add truncate operation to queue
    this.#writeQueue.push({
      type: "truncate",
      size,
    });

    this.#triggerWrites();
  }

  /**
   * Add a delete operation to the queue
   */
  #queueDelete() {
    // Add delete operation to queue
    this.#writeQueue.push({
      type: "delete",
    });

    this.#triggerWrites();
  }

  #triggerWrites() {
    // Start processing if not already processing
    if (!this.#isProcessingWrites) {
      // Use setTimeout to make this truly asynchronous and non-blocking
      setTimeout(() => this.#processWriteQueue(), 0);
    }
  }
  /**
   * Process all pending operations in the queue in order without coalescing
   */
  async #processWriteQueue() {
    if (
      this.#isProcessingWrites ||
      this.#writeQueue.length === 0 ||
      !this.#dbFileHandle
    ) {
      return;
    }

    this.#isProcessingWrites = true;
    const start = performance.now();
    let processed = 0;
    console.log(`Processing ${this.#writeQueue.length} write operations`);

    try {
      // Find the in-memory database file object from the mapNameToFile
      const file = this.mapNameToFile.get(`/${this.#opfsFilename}`);
      if (!file) {
        console.error(
          `Cannot find file /${this.#opfsFilename} in mapNameToFile`
        );
        this.#isProcessingWrites = false;
        return;
      }

      // Create a writable stream - we'll use a single writable for all operations
      // to avoid the overhead of opening and closing the file multiple times
      // console.log(`Opening file for processing ${this.#writeQueue.length} operations`);
      const writable = await this.#dbFileHandle.createWritable({
        keepExistingData: true,
      });

      // Process each operation in order
      let continueProcessing = true;

      while (this.#writeQueue.length > 0 && continueProcessing) {
        processed++;
        const operation = this.#writeQueue.shift();

        if (operation.type === "write") {
          let { offset, length } = operation;
          let end = offset + length;

          // if the next writes are close enough, just combine them
          while (this.#writeQueue.length > 0 
            && this.#writeQueue[0].type === "write" 
            && this.#writeQueue[0].offset > offset
            && this.#writeQueue[0].offset <= end + 64000) {
            const nextOp = this.#writeQueue.shift();
            end = Math.max(end, nextOp.offset + nextOp.length);
          }
          length = end - offset;

          // Get the data from the in-memory file
          const dataToWrite = new Uint8Array(file.data, offset, length);

          // Seek to the position and write the data
          await writable.seek(offset);
          await writable.write(dataToWrite);

          // console.log(`Wrote ${length} bytes at offset ${offset} to OPFS`);
        } else if (operation.type === "truncate") {
          // Truncate operation
          const { size } = operation;
          await writable.truncate(size);
          // console.log(`Truncated OPFS file to ${size} bytes`);
        } else if (operation.type === "delete") {
          // Close the current writable since we're deleting the file
          await writable.close();

          try {
            // Delete the file
            await this.#rootDir.removeEntry(this.#opfsFilename);
            this.#dbFileHandle = null;
            console.log(`Deleted OPFS file`);

            // Stop processing further operations
            continueProcessing = false;
            this.#writeQueue = [];
          } catch (e) {
            console.error(`Failed to delete OPFS file: ${e.message}`);
          }
        }
      }

      // Close the stream after all operations if we haven't already closed it
      if (continueProcessing) {
        await writable.close();
        // console.log(`Completed all operations and closed file`);
      }
    } catch (e) {
      console.error(`Failed to process operation queue: ${e.message}`);
    } finally {
      this.#isProcessingWrites = false;
      const end = performance.now();
      console.log(`Processed ${processed} operations in ${(end - start).toFixed(2)} ms`);

      // If more operations were added while processing, start again
      if (this.#writeQueue.length > 0) {
        setTimeout(() => this.#processWriteQueue(), 0);
      }
    }
  }

  /**
   * Process all pending operations and close the VFS
   */
  close() {
    // console.log("MemoryDelayedOPFSVFS.close()");

    // Close all open files
    for (const fileId of this.mapIdToFile.keys()) {
      this.jClose(fileId);
    }

    // Try to flush any pending operations
    this.#triggerWrites();
  }

  /**
   * @param {string?} filename
   * @param {number} fileId
   * @param {number} flags
   * @param {DataView} pOutFlags
   * @returns {number|Promise<number>}
   */
  jOpen(filename, fileId, flags, pOutFlags) {
    // console.log(`MemoryDelayedOPFSVFS.jOpen(${filename}, ${fileId}, ${flags})`);

    const url = new URL(
      filename || Math.random().toString(36).slice(2),
      "file://"
    );
    const pathname = url.pathname;

    let file = this.mapNameToFile.get(pathname);
    if (!file) {
      if (flags & VFS.SQLITE_OPEN_CREATE) {
        // Check if we should use the buffered OPFS data
        let initialData = new ArrayBuffer(0);
        if (this.#isTrackedDbFile(pathname) && this.#opfsDataBuffer) {
          // Use the buffered data for this file
          initialData = this.#opfsDataBuffer;
          // Clear the buffer after it's been used to prevent potential reuse issues
          this.#opfsDataBuffer = null;
        }
        file = {
          pathname,
          flags: flags,
          size: initialData.byteLength,
          data: initialData,
        };
        this.mapNameToFile.set(pathname, file);
      } else {
        return VFS.SQLITE_CANTOPEN;
      }
    }

    // Put the file in the opened files map.
    this.mapIdToFile.set(fileId, file);
    pOutFlags.setInt32(0, flags, true);
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} fileId
   * @returns {number|Promise<number>}
   */
  jClose(fileId) {
    // console.log(`MemoryDelayedOPFSVFS.jClose(${fileId})`);
    const file = this.mapIdToFile.get(fileId);
    this.mapIdToFile.delete(fileId);

    if (file.flags & VFS.SQLITE_OPEN_DELETEONCLOSE) {
      this.mapNameToFile.delete(file.pathname);

      // Attempt to delete from OPFS if this is our database file
      if (this.#isTrackedDbFile(file.pathname)) {
        this.#queueDelete();
      }
    }

    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} fileId
   * @param {Uint8Array} pData
   * @param {number} iOffset
   * @returns {number|Promise<number>}
   */
  jRead(fileId, pData, iOffset) {
    // console.log(`MemoryDelayedOPFSVFS.jRead(${fileId}, ${pData.byteLength}, ${iOffset})`);
    const file = this.mapIdToFile.get(fileId);

    // Clip the requested read to the file boundary.
    const bgn = Math.min(iOffset, file.size);
    const end = Math.min(iOffset + pData.byteLength, file.size);
    const nBytes = end - bgn;

    if (nBytes) {
      pData.set(new Uint8Array(file.data, bgn, nBytes));
    }

    if (nBytes < pData.byteLength) {
      // Zero unused area of read buffer.
      pData.fill(0, nBytes);
      return VFS.SQLITE_IOERR_SHORT_READ;
    }
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} fileId
   * @param {Uint8Array} pData
   * @param {number} iOffset
   * @returns {number|Promise<number>}
   */
  jWrite(fileId, pData, iOffset) {
    // console.log(`MemoryDelayedOPFSVFS.jWrite(${fileId}, ${pData.byteLength}, ${iOffset})`);
    const file = this.mapIdToFile.get(fileId);
    if (iOffset + pData.byteLength > file.data.byteLength) {
      // Resize the ArrayBuffer to hold more data.
      const newSize = Math.max(
        iOffset + pData.byteLength,
        2 * file.data.byteLength
      );
      const data = new ArrayBuffer(newSize);
      new Uint8Array(data).set(new Uint8Array(file.data, 0, file.size));
      file.data = data;
    }

    // Copy data.
    new Uint8Array(file.data, iOffset, pData.byteLength).set(pData);
    file.size = Math.max(file.size, iOffset + pData.byteLength);

    // If this is our database file, queue only the changed page for writing to OPFS
    if (this.#isTrackedDbFile(file.pathname)) {
      // Queue just the offset and length - the actual data is already in memory
      this.#queueWrite(iOffset, pData.byteLength);
    }

    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} fileId
   * @param {number} iSize
   * @returns {number|Promise<number>}
   */
  jTruncate(fileId, iSize) {
    // console.log(`Truncating file ${fileId} to ${iSize} bytes`);
    const file = this.mapIdToFile.get(fileId);

    // For simplicity we don't make the ArrayBuffer smaller.
    file.size = Math.min(file.size, iSize);

    // If this is our database file, queue the truncation operation
    if (this.#isTrackedDbFile(file.pathname)) {
      this.#queueTruncate(file.size);
    }

    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} fileId
   * @param {DataView} pSize64
   * @returns {number|Promise<number>}
   */
  jFileSize(fileId, pSize64) {
    // console.log(`MemoryDelayedOPFSVFS.jFileSize(${fileId})`);
    const file = this.mapIdToFile.get(fileId);

    pSize64.setBigInt64(0, BigInt(file.size), true);
    return VFS.SQLITE_OK;
  }

  /**
   * @param {string} name
   * @param {number} syncDir
   * @returns {number|Promise<number>}
   */
  jDelete(name, syncDir) {
    // console.log(`MemoryDelayedOPFSVFS.jDelete(${name})`);
    const url = new URL(name, "file://");
    const pathname = url.pathname;

    this.mapNameToFile.delete(pathname);

    // If this is our database file, queue a delete operation
    if (this.#isTrackedDbFile(pathname)) {
      this.#queueDelete();
    }

    return VFS.SQLITE_OK;
  }

  /**
   * @param {string} name
   * @param {number} flags
   * @param {DataView} pResOut
   * @returns {number|Promise<number>}
   */
  jAccess(name, flags, pResOut) {
    // console.log(`MemoryDelayedOPFSVFS.jAccess(${name}, ${flags})`);
    const url = new URL(name, "file://");
    const pathname = url.pathname;

    // First check if the file exists in our in-memory map
    const fileExists = this.mapNameToFile.has(pathname);

    // If not found in memory and this is our database file, check if we have a buffered version
    const isBufferedDb =
      !fileExists &&
      pathname === `/${this.#opfsFilename}` &&
      this.#opfsDataBuffer !== null;

    // A file exists if it's either in memory or in our buffer
    pResOut.setInt32(0, fileExists || isBufferedDb ? 1 : 0, true);
    return VFS.SQLITE_OK;
  }
}
