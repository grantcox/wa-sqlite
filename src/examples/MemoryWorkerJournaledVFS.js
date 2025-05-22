import { FacadeVFS } from "../FacadeVFS.js";
import * as VFS from "../VFS.js";

/**
 * @typedef {Object} MappedFile
 * @property {string} name
 * @property {number} flags
 * @property {number} size
 * @property {ArrayBuffer} data
 */

/**
 * @typedef {Object} VFSConfig
 * @property {string} encryptionPassword
 * @property {string} dbName
 * @property {Worker | (() => Worker) | null} worker
 * @property {number} syncLatencyMsec
 */

/**
 * @typedef {Object} PendingWriteOperation
 * @property {'write' | 'truncate' | 'delete'} type
 * @property {number} [offset]
 * @property {number} [size]
 */

/**
 * @typedef {Object} FileSnapshot
 * @property {string} filename - Name of the file
 * @property {ArrayBuffer} data - Current file data at the time of the snapshot (may be partial)
 * @property {number} size - Size of the file at the time of the snapshot
 * @property {number} startOffset - Starting offset of the data in the snapshot (0 for full file)
 * @property {number} queuePosition - The position in the write queue at the time of the snapshot
 * @property {number} timestamp - Timestamp when the snapshot was created
 */

// Memory-based VFS with asynchronous persistence via Web Worker
export class MemoryWorkerJournaledVFS extends FacadeVFS {
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

  // Map of initial file data loaded by the worker
  /** @type {Map<string, ArrayBuffer>} */ #initialFiles = new Map();

  // Map of writes that have occurred since the last sync
  /** @type {Map<string, Array<PendingWriteOperation>>} */ #pendingWritesMap = new Map();

  /** @type {Map<string, number>} */ #syncCount = new Map();

  /**
   * @param {string} name
   * @param {*} module
   * @param {VFSConfig} config - Optional encryption key and configuration
   * @returns {Promise<MemoryWorkerJournaledVFS>}
   */
  static async create(name, module, config = {}) {
    const vfs = new MemoryWorkerJournaledVFS(name, module, config);
    await vfs.isReady();
    return vfs;
  }

  get isReadOnly() {
    return !this.#workerSupportsWrites;
  }

  exportDatabase() {
    // Return a copy of the current SQLite database file
    // The main database file is named according to the dbName
    const filename = `${this.#dbName}`;
    const file = this.mapNameToFile.get(filename);
    
    if (file && file.data) {
      // Create a new ArrayBuffer to hold the copied data
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

  destroyDatabase() {
    console.log("MemoryWorkerJournaledVFS | destroyDatabase");

    // delete all files
    for (const filename of this.mapNameToFile.keys()) {
      this.jDelete(filename);
      this.#syncFileToWorker(filename);
    }
    
    // Clear all maps
    this.mapNameToFile.clear();
    this.#initialFiles.clear();
    this.#pendingWritesMap.clear();
  }

  terminate() {
    if (this.#worker) {
      this.#worker.terminate();
      this.#worker = null;
    }
    this.mapNameToFile.clear();
    this.mapIdToFile.clear();
    this.#initialFiles.clear();
    this.#pendingWritesMap.clear();
  }

  /**
   * @param {string} name
   * @param {*} module
   * @param {VFSConfig} config
   */
  constructor(name, module, config) {
    super(name, module);
    this.#dbName = config.dbName ?? "db.sqlite";
    this.#worker =(config.worker instanceof Function) ? config.worker() : config.worker;
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
      if (!this.#worker) {
        console.warn("MemoryWorkerJournaledVFS | No worker provided, will use memory only");
        this.#workerSupportsWrites = false;
        return true;
      }

      // Set up message handler for worker
      const initPromise = new Promise((resolve, reject) => {
        const messageHandler = (event) => {
          const msg = event.data;
          if (msg.type === 'initComplete') {
            // Store the initial file data for later use when opening files
            if (msg.files && Array.isArray(msg.files)) {
              for (const fileInfo of msg.files) {
                // Store file data with the internal name (without prefix)
                this.#initialFiles.set(fileInfo.name, fileInfo.data.buffer);
              }
            }
            
            this.#workerSupportsWrites = msg.writesEnabled;
            this.#worker.removeEventListener('message', messageHandler);
            
            // Set up permanent message handler for ongoing communication
            this.#worker.addEventListener('message', this.#handleWorkerMessage.bind(this));
            
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
      console.error("MemoryWorkerJournaledVFS | Failed to initialize worker:", e);
      return false;
    }
  }

  /**
   * Handle messages from the worker
   * @param {MessageEvent} event - The message event
   */
  #handleWorkerMessage(event) {
    // No message handling needed in this implementation
  }

  /**
   * Queue a write operation to be sent to the worker
   * @param {string} filename - The name of the file being written
   */
  #queueWrite(filename, event) {
    if (!this.#workerSupportsWrites || !this.#isTrackedFile(filename)) {
      return;
    }
    
    // Make sure we have a queue for this file
    if (!this.#pendingWritesMap.has(filename)) {
      this.#pendingWritesMap.set(filename, []);
    }
    const queue = this.#pendingWritesMap.get(filename);
    queue.push(event);
  }

  /**
   * Check if the SQLite file is one we're tracking
   * @param {string} filename - The filename to check
   * @returns {boolean} - Whether this is a tracked file
   */
  #isTrackedFile(filename) {
    // Only persist the specific SQLite file and its journal
    return filename.startsWith(this.#dbName);
  }

  /**
   * Process all pending operations and close the VFS
   */
  close() {
    // Close all open files
    for (const fileId of this.mapIdToFile.keys()) {
      this.jClose(fileId);
    }

    for (const filename of this.mapNameToFile.keys()) {
      // Sync to ensure latest data is saved
      this.#syncFileToWorker(filename);
    }

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
    filename = filename || Math.random().toString(36).slice(2);

    let file = this.mapNameToFile.get(filename);
    if (!file) {
      let initialData = null;
      if (this.#initialFiles.has(filename)) {
        // Use the buffered data for this file
        initialData = this.#initialFiles.get(filename);
        // Remove from initialFiles after it's been used
        this.#initialFiles.delete(filename);
      } else if (flags & VFS.SQLITE_OPEN_CREATE) {
        initialData = new ArrayBuffer(0);
      }
       
      if (initialData) {
        file = {
          name: filename,
          flags: flags,
          size: initialData.byteLength,
          data: initialData,
        };
        this.mapNameToFile.set(filename, file);
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
      // Attempt to delete from OPFS if this is a tracked file
      this.#queueWrite(file.name, {
        type: 'delete',
      });
      this.#syncFileToWorker(file.name);
      this.mapNameToFile.delete(file.name);
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

    // If this is a tracked file, queue the write operation
    this.#queueWrite(file.name, {
      type: 'write',
      offset: iOffset,
      size: pData.byteLength
    });

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

    // If this is a tracked file, queue the truncation operation
    this.#queueWrite(file.name, {
      type: 'truncate',
      size: file.size
    });

    return VFS.SQLITE_OK;
  }
  
  /**
   * Creates a snapshot of the file state for syncing to persistent storage
   * @param {number} fileId 
   * @param {number} flags 
   * @returns {number|Promise<number>}
   */
  jSync(fileId, flags) {    
    const file = this.mapIdToFile.get(fileId);
    if (!file) {
      return VFS.SQLITE_OK;
    }
    
    // Only track snapshots for files we're tracking
    if (!this.#isTrackedFile(file.name)) {
      return VFS.SQLITE_OK;
    }
    
    this.#syncCount.set(file.name, (this.#syncCount.get(file.name) || 0) + 1);
    console.log(`MemoryWorkerJournaledVFS | Syncing file ${file.name} (${this.#syncCount.get(file.name)} times)`);
    
    this.#syncFileToWorker(file.name);
  
    return VFS.SQLITE_OK;
  }
  
  /**
   * Takes a snapshot of a file and its write queue position
   * Optimized to only copy the necessary range of data affected by pending writes
   * @param {string} filename - The name of the file
   */
  #syncFileToWorker(filename) {
    if (!this.#workerSupportsWrites) {
      return;
    }
    const file = this.mapNameToFile.get(filename);

    // Get the current write queue for this file
    const queue = this.#pendingWritesMap.get(filename) || [];
    const operations = queue.splice(0);
    
    if (operations.length === 0) {
      return;
    }
    
    // Calculate the range of data affected by pending writes, that we need to copy and send to the worker
    let minOffset = Number.MAX_SAFE_INTEGER;
    let maxOffset = 0;
    
    for (const op of operations) {
      if (op.type === 'write') {
        minOffset = Math.min(minOffset, op.offset);
        maxOffset = Math.max(maxOffset, (op.offset + op.size));
      } else if (op.type === 'truncate') {
        maxOffset = op.size
      } else if (op.type === 'delete') {
        // For delete, we don't need any data
        minOffset = 0;
        maxOffset = 0;
      }
    }
    
    // If no actual range was calculated, snapshot the entire file
    if (minOffset === Number.MAX_SAFE_INTEGER) {
      minOffset = 0;
      maxOffset = file.size;
    }
    const rangeSize = maxOffset - minOffset;
    
    // Create a copy of just the affected range
    const dataCopy = new ArrayBuffer(rangeSize)
    const snapshotData = new Uint8Array(dataCopy)
    snapshotData.set(
      new Uint8Array(file.data, minOffset, rangeSize)
    );
    
    console.log(`MemoryWorkerJournaledVFS | syncFileToWorker for ${filename}: Range ${minOffset}-${maxOffset} (${dataCopy.byteLength} bytes out of ${file.size})`);

    // Send the snapshot and operations to the worker
    this.#worker.postMessage({
      type: 'writes',
      operations: operations,
      filename: filename,
      databaseState: snapshotData,
      startOffset: minOffset,
      totalSize: file.size
    }, [snapshotData.buffer]);
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
   * @param {string} filename
   * @param {number} syncDir
   * @returns {number}
   */
  jDelete(filename, syncDir = null) {
    // truncate the in-memory file
    const file = this.mapNameToFile.get(filename);
    if (file) {
      file.size = 0;

      // and delete
      this.mapNameToFile.delete(filename);
    }

    // If this is a tracked file, queue a delete operation
    this.#queueWrite(filename, {
      type: 'delete',
    });

    return VFS.SQLITE_OK;
  }

  /**
   * @param {string} filename
   * @param {number} flags
   * @param {DataView} pResOut
   * @returns {number}
   */
  jAccess(filename, flags, pResOut) {
    // First check if the file exists in our in-memory map
    const fileExists = this.mapNameToFile.has(filename);

    // If not found in memory, check if we have a buffered version
    const isBuffered = !fileExists && this.#initialFiles.has(filename);

    // A file exists if it's either in memory or in our buffer
    pResOut.setInt32(0, fileExists || isBuffered ? 1 : 0, true);
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
