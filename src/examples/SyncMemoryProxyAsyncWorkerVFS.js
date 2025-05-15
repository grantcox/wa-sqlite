import { FacadeVFS } from "../FacadeVFS.js";
import * as VFS from "../VFS.js";

/**
 * @typedef {Object} MappedFile
 * @property {string} pathname
 * @property {number} flags
 * @property {number} size
 * @property {ArrayBuffer} data
 */

/**
 * @typedef {Object} VFSConfig
 * @property {string} encryptionPassword
 * @property {string} dbName
 * @property {Worker | () => Worker} worker
 * @property {number} syncLatencyMsec
 */

/**
 * @typedef {Object} PendingWriteOperation
 * @property {'write' | 'truncate' | 'delete'} type
 * @property {number} [offset]
 * @property {number} [size]
 */

// Memory-based VFS with asynchronous persistence via Web Worker
export class SyncMemoryProxyAsyncWorkerVFS extends FacadeVFS {
  // Map of SQLite files, keyed by filename.
  /** @type {Map<string, MappedFile>} */ mapNameToFile = new Map();

  // Map of SQLite files, keyed by id (sqlite3_file pointer).
  /** @type {Map<number, MappedFile>} */ mapIdToFile = new Map();

  // Worker for handling persistence operations
  /** @type {Worker} */ #worker = null;
  #workerSupportsWrites = true;

  // Track if VFS (and worker) is ready
  /** @type {Promise<boolean>} */ #vfsReady = null;

  // This is the name of the SQLite file we are persisting via worker
  #dbName = "db.sqlite";

  // Buffer for initial data loaded by the worker
  /** @type {ArrayBuffer} */ #initialData = null;

  #writePushCadenceMsec = 25;

  // Array of pending operations to track what has changed
  /** @type {Array<PendingWriteOperation>} */ #pendingWrites = [];

  // Interval ID for the periodic write sender
  #writeIntervalId = null;

  /**
   * @param {string} name
   * @param {*} module
   * @param {VFSConfig} config - Optional encryption key and configuration
   * @returns {Promise<SyncMemoryProxyAsyncWorkerVFS>}
   */
  static async create(name, module, config = {}) {
    const vfs = new SyncMemoryProxyAsyncWorkerVFS(name, module, config);
    await vfs.isReady();
    return vfs;
  }

  get isReadOnly() {
    return !this.#workerSupportsWrites;
  }

  /**
   * @param {string} name
   * @param {*} module
   * @param {VFSConfig} config
   */
  constructor(name, module, config) {
    super(name, module);
    this.#dbName = config.dbName ?? "db.sqlite";
    this.#writePushCadenceMsec = config.syncLatencyMsec ?? 25;
    this.#worker = (config.worker instanceof Worker) ? config.worker : config.worker();
    this.#vfsReady = this.init(config);
  }

  async isReady() {
    return this.#vfsReady;
  }

  /**
   * @param {VFSConfig} config
   */
  async init(config) {
    try {
      // Set up message handler for worker
      const initPromise = new Promise((resolve, reject) => {
        const messageHandler = (event) => {
          const msg = event.data;
          if (msg.type === 'initComplete') {
            // Store the initial data buffer for later use when opening the SQLite database
            this.#initialData = msg.fileData.buffer;
            this.#workerSupportsWrites = msg.writesEnabled;
            this.#worker.removeEventListener('message', messageHandler);
            
            // Set up permanent message handler for ongoing communication
            this.#worker.addEventListener('message', this.#handleWorkerMessage.bind(this));
            
            // Start the interval for sending pending writes
            if (this.#workerSupportsWrites) {
              this.#startWriteInterval();
            }
            
            resolve(true);
          } else if (msg.type === 'error') {
            reject(new Error(msg.message));
          }
        };
        
        this.#worker.addEventListener('message', messageHandler);
      });

      // Initialize the worker
      this.#worker.postMessage({
        type: 'init',
        config: {
          encryptionPassword: config.encryptionPassword,
          dbName: this.#dbName,
        }
      });

      return await initPromise;
    } catch (e) {
      console.error("SyncMemoryProxyAsyncWorkerVFS | Failed to initialize worker:", e);
      return false;
    }
  }

  exportDatabase() {
    // return a copy of the current SQLite database file
    const file = this.mapNameToFile.get(`/${this.#dbName}`);
    if (file && file.data) {
      // create a new ArrayBuffer to hold the copied data
      const newBuffer = new ArrayBuffer(file.data.byteLength);
      const sourceView = new Uint8Array(file.data);
      const newView = new Uint8Array(newBuffer);
      
      // Copy all data from original to new buffer
      newView.set(sourceView);
      
      // Return this copied buffer
      return newBuffer;
    }
    return null;
  }

  /**
   * Handle messages from the worker
   * @param {MessageEvent} event - The message event
   */
  #handleWorkerMessage(event) {
    // No message handling needed in this implementation
  }

  /**
   * Start the interval for sending pending writes
   */
  #startWriteInterval() {
    // Clear any existing interval
    if (this.#writeIntervalId) {
      clearInterval(this.#writeIntervalId);
    }
    
    // Set up a new interval to regularly send pending writes
    this.#writeIntervalId = setInterval(() => {
      this.#sendPendingWrites();
    }, this.#writePushCadenceMsec);
  }

  /**
   * Send any pending operations to the worker
   */
  #sendPendingWrites() {
    if (this.#pendingWrites.length === 0 || !this.#workerSupportsWrites) {
      return;
    }

    // Atomically claim the operations to send
    const operations = this.#pendingWrites.splice(0);
    
    // Get the current state of the database file
    const file = this.mapNameToFile.get(`/${this.#dbName}`);
    if (!file || !file.data) {
      return;
    }
    
    // Create a copy of the database to send to the worker
    const dbCopy = new Uint8Array(file.data.byteLength);
    dbCopy.set(new Uint8Array(file.data, 0, file.size));
    
    // Send the entire database along with the operations log
    this.#worker.postMessage({
      type: 'writes',
      operations,
      databaseState: dbCopy
    }, [dbCopy.buffer]);
  }

  /**
   * Queue a write operation to be sent to the worker
   * @param {number} offset - The offset at which the write occurred
   * @param {Uint8Array} data - The data that was written (only used for length)
   */
  #queueWrite(offset, data) {
    if (!this.#workerSupportsWrites) {
      return;
    }
    // Only record the offset and size, no data copying
    this.#pendingWrites.push({
      type: 'write',
      offset,
      size: data.byteLength
    });
  }

  /**
   * Queue a truncate operation
   * @param {number} size - The new size to truncate to
   */
  #queueTruncate(size) {
    if (!this.#workerSupportsWrites) {
      return;
    }
    // Add to pending operations array
    this.#pendingWrites.push({
      type: 'truncate',
      size
    });
  }

  /**
   * Queue a delete operation
   */
  #queueDelete() {
    if (!this.#workerSupportsWrites) {
      return;
    }
    // Add to pending operations array
    this.#pendingWrites.push({
      type: 'delete'
    });
  }

  /**
   * Check if the SQLite file is the one we're are persisting
   * @param {string} pathname - The pathname to check
   * @returns {boolean} - Whether this is our tracked DB file
   */
  #isTrackedDbFile(pathname) {
    return pathname === `/${this.#dbName}`;
  }

  /**
   * Process all pending operations and close the VFS
   */
  close() {
    // Cancel the write interval
    if (this.#writeIntervalId) {
      clearInterval(this.#writeIntervalId);
      this.#writeIntervalId = null;
    }

    // Close all open files
    for (const fileId of this.mapIdToFile.keys()) {
      this.jClose(fileId);
    }

    // Force send any pending writes
    this.#sendPendingWrites();

    // Terminate the worker after a short delay to allow pending operations to complete
    setTimeout(() => {
      if (this.#worker) {
        this.#worker.terminate();
        this.#worker = null;
      }
    }, 100);
  }

  /**
   * @param {string?} filename
   * @param {number} fileId
   * @param {number} flags
   * @param {DataView} pOutFlags
   * @returns {number}
   */
  jOpen(filename, fileId, flags, pOutFlags) {
    const url = new URL(filename || Math.random().toString(36).slice(2), "file://");
    const pathname = url.pathname;

    let file = this.mapNameToFile.get(pathname);
    if (!file) {
      if (flags & VFS.SQLITE_OPEN_CREATE) {
        // Check if we should use the buffered OPFS data
        let initialData = new ArrayBuffer(0);
        if (this.#isTrackedDbFile(pathname) && this.#initialData) {
          // Use the buffered data for this file
          initialData = this.#initialData;
          // Clear the buffer after it's been used to prevent potential reuse issues
          this.#initialData = null;
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
   * @returns {number}
   */
  jClose(fileId) {
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
   * @returns {number}
   */
  jRead(fileId, pData, iOffset) {
    const file = this.mapIdToFile.get(fileId);

    // Clip the requested read to the file boundary.
    const bgn = Math.min(iOffset, file.size);
    const end = Math.min(iOffset + pData.byteLength, file.size);
    const nBytes = end - bgn;

    if (nBytes) {
      // Read data from memory
      const data = new Uint8Array(file.data, bgn, nBytes);
      pData.set(data);
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
   * @returns {number}
   */
  jWrite(fileId, pData, iOffset) {
    const file = this.mapIdToFile.get(fileId);
    if (iOffset + pData.byteLength > file.data.byteLength) {
      // Resize the ArrayBuffer to hold more data.
      const newSize = Math.max(iOffset + pData.byteLength, 2 * file.data.byteLength);
      const data = new ArrayBuffer(newSize);
      new Uint8Array(data).set(new Uint8Array(file.data, 0, file.size));
      file.data = data;
    }

    // Copy data to in-memory storage
    new Uint8Array(file.data, iOffset, pData.byteLength).set(pData);
    file.size = Math.max(file.size, iOffset + pData.byteLength);

    // If this is our database file, queue the write operation
    if (this.#isTrackedDbFile(file.pathname)) {
      this.#queueWrite(iOffset, pData);
    }

    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} fileId
   * @param {number} iSize
   * @returns {number}
   */
  jTruncate(fileId, iSize) {
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
   * @returns {number}
   */
  jFileSize(fileId, pSize64) {
    const file = this.mapIdToFile.get(fileId);

    pSize64.setBigInt64(0, BigInt(file.size), true);
    return VFS.SQLITE_OK;
  }

  /**
   * @param {string} name
   * @param {number} syncDir
   * @returns {number}
   */
  jDelete(name, syncDir) {
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
   * @returns {number}
   */
  jAccess(name, flags, pResOut) {
    const url = new URL(name, "file://");
    const pathname = url.pathname;

    // First check if the file exists in our in-memory map
    const fileExists = this.mapNameToFile.has(pathname);

    // If not found in memory and this is our database file, check if we have a buffered version
    const isBufferedDb = !fileExists && this.#isTrackedDbFile(pathname) && this.#initialData !== null;

    // A file exists if it's either in memory or in our buffer
    pResOut.setInt32(0, fileExists || isBufferedDb ? 1 : 0, true);
    return VFS.SQLITE_OK;
  }
  
  /**
   * Override the base makeDataArray to not watch for WebAssembly memory resize.
   * as we always use the memory immediately.
   * @param {number} byteOffset 
   * @param {number} byteLength 
   */
  makeDataArray(byteOffset, byteLength) {
    return this._module.HEAPU8.subarray(byteOffset, byteOffset + byteLength);
  }
}
