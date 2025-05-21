/**
 * SqliteWasmMemoryToWorkerVFS.js
 * 
 * A SQLite VFS for sqlite-wasm that uses in-memory storage with asynchronous syncing to a worker.
 * This VFS writes synchronously to memory, and asynchronously syncs database state to a worker.
 * 
 * Based on SyncMemoryProxyAsyncWorkerVFS.js and SqliteWasmMemoryVFS.js
 */
import { FacadeVFS } from "../src/FacadeVFS.js";
import * as VFS from "../src/VFS.js";

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
 * A memory-based VFS for sqlite-wasm that asynchronously persists changes to a worker
 */
export class SqliteWasmMemoryToWorkerVFS extends FacadeVFS {
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

  // SQLite3 module for wasm compatibility
  /** @type {Object} */ sqlite3 = null;
  
  /**
   * Creates a new instance of SqliteWasmMemoryToWorkerVFS
   * 
   * @param {string} name - VFS name
   * @param {Object} sqlite3 - sqlite3 module
   * @param {VFSConfig} config - Configuration options
   */
  constructor(name, sqlite3, config = {}) {
    const waModule = {
      HEAPU8: sqlite3.wasm.heap8u(),
      UTF8ToString: sqlite3.wasm.cstrToJs
    };
    super(name, waModule);
    this.sqlite3 = sqlite3;
    this.#dbName = config.dbName ?? "db.sqlite";
    this.#writePushCadenceMsec = config.syncLatencyMsec ?? 25;
    this.#worker = (config.worker instanceof Function) ? config.worker() : config.worker;
    this.#vfsReady = this.init(config);
  }

  /**
   * Create and initialize the VFS
   * 
   * @param {string} name - VFS name
   * @param {Object} sqlite3 - sqlite3 module
   * @param {VFSConfig} config - Configuration options
   * @returns {Promise<SqliteWasmMemoryToWorkerVFS>}
   */
  static async create(name, sqlite3, config = {}) {
    const vfs = new SqliteWasmMemoryToWorkerVFS(name, sqlite3, config);
    await vfs.isReady();
    return vfs;
  }

  /**
   * Wait for VFS initialization to complete
   */
  async isReady() {
    return this.#vfsReady;
  }

  /**
   * Get a readable indicator of whether the VFS is read-only
   */
  get isReadOnly() {
    return !this.#workerSupportsWrites;
  }

  /**
   * Export a copy of the current database from memory
   * 
   * @returns {ArrayBuffer|null} Database contents or null if not found
   */
  exportDatabase() {
    // Return a copy of the current SQLite database file
    const file = this.mapNameToFile.get(`/${this.#dbName}`);
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

  /**
   * Destroy the database file in memory and trigger worker deletion
   */
  destroyDatabase() {
    this.#queueDelete();
    this.#sendPendingWrites();
    this.mapNameToFile.clear();
    this.#initialData = null;
  }

  /**
   * Shut down the VFS and release resources
   */
  terminate() {
    if (this.#writeIntervalId) {
      clearInterval(this.#writeIntervalId);
      this.#writeIntervalId = null;
    }
    if (this.#worker) {
      this.#worker.terminate();
      this.#worker = null;
    }
    this.mapNameToFile.clear();
    this.mapIdToFile.clear();
    this.#initialData = null;
    this.#pendingWrites = [];
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
}



/**
 * Register this VFS with sqlite-wasm
 * 
 * @param {Object} sqlite3 - The sqlite3 object from sqlite-wasm
 * @param {Object} options - Configuration options
 * @param {string} [options.name='memory-worker'] - The name for the VFS
 * @param {string} [options.dbName='db.sqlite'] - The name of the database file
 * @param {Worker|null} [options.worker=null] - Worker for persistence, or null for memory-only
 * @param {number} [options.syncLatencyMsec=25] - How often to sync to the worker (ms)
 * @param {string} [options.encryptionPassword] - Optional encryption password
 * @returns {Object} The VFS controller
 */
export function registerVfs(sqlite3, options = {}) {
  if (!sqlite3 || !sqlite3.capi || !sqlite3.wasm) {
    throw new Error("sqlite3 argument is required and must have capi and wasm properties.");
  }
  
  const capi = sqlite3.capi;
  const wasm = sqlite3.wasm;
  
  // Extract options
  const vfsName = options.name || 'memory-worker';
  const dbName = options.dbName || 'db.sqlite';
  const worker = options.worker || null;
  const syncLatencyMsec = options.syncLatencyMsec || 25;
  const encryptionPassword = options.encryptionPassword;
  
  // Create VFS and IO Methods structures
  const memoryIoMethods = new capi.sqlite3_io_methods();
  const memoryVfs = new capi.sqlite3_vfs();
  
  // Store open files, keyed by file ID (sqlite3_file pointer)
  const openFiles = Object.create(null);
  
  // In-memory storage for files
  const fileStorage = new Map();
  let initialData = options.initialData || null;
  let pendingWrites = [];
  let writeIntervalId = null;
  let workerSupportsWrites = true;
  
  // VFS configuration
  memoryVfs.$iVersion = 2;
  memoryVfs.$szOsFile = capi.sqlite3_file.structInfo.sizeof;
  memoryVfs.$mxPathname = 1024;
  memoryVfs.$zName = wasm.allocCString(vfsName);
  
  // Set to null since we don't need dynamic library support
  memoryVfs.$xDlOpen = memoryVfs.$xDlError = memoryVfs.$xDlSym = memoryVfs.$xDlClose = null;

  // Clean up resources when disposing
  memoryVfs.addOnDispose('$zName', memoryVfs.$zName);
  memoryIoMethods.$iVersion = 1;

  // Initialize worker if provided
  if (worker) {
    worker.addEventListener('message', (event) => {
      const msg = event.data;
      if (msg.type === 'initComplete') {
        initialData = msg.fileData?.buffer || new ArrayBuffer(0);
        workerSupportsWrites = msg.writesEnabled ?? true;
        
        // Start the interval for sending pending writes
        if (workerSupportsWrites && !writeIntervalId) {
          writeIntervalId = setInterval(() => {
            sendPendingWrites();
          }, syncLatencyMsec);
        }
      }
    });
    
    // Initialize the worker
    worker.postMessage({
      type: 'init',
      config: {
        encryptionPassword: encryptionPassword,
        dbName: dbName,
      }
    });
  }
  
  // Helper to generate a random filename if none is specified
  const randomFilename = function() {
    return 'memory-' + Math.random().toString(36).slice(2);
  };
  
  // Helper to check if a file is our tracked DB file
  const isTrackedDbFile = function(pathname) {
    return pathname === dbName;
  };
  
  // Helper to send pending writes to worker
  const sendPendingWrites = function() {
    if (pendingWrites.length === 0 || !workerSupportsWrites || !worker) {
      return;
    }

    // Atomically claim the operations to send
    const operations = pendingWrites.splice(0);
    
    // Find our database file
    let dbFile = null;
    for (const file of Object.values(openFiles)) {
      if (isTrackedDbFile(file.filename)) {
        dbFile = file;
        break;
      }
    }
    
    if (!dbFile) {
      // Check fileStorage directly
      const fileData = fileStorage.get(dbName);
      if (fileData) {
        // Create a copy to send
        const dbCopy = new Uint8Array(fileData.byteLength);
        dbCopy.set(new Uint8Array(fileData));
        
        // Send to worker
        worker.postMessage({
          type: 'writes',
          operations,
          databaseState: dbCopy
        }, [dbCopy.buffer]);
      }
      return;
    }
    
    // Get file data
    const fileData = fileStorage.get(dbFile.filename);
    if (!fileData) {
      return;
    }
    
    // Create a copy to send
    const dbCopy = new Uint8Array(fileData.byteLength);
    dbCopy.set(new Uint8Array(fileData));
    
    // Send to worker
    worker.postMessage({
      type: 'writes',
      operations,
      databaseState: dbCopy
    }, [dbCopy.buffer]);
  };
  
  // Helper to queue a write operation
  const queueWrite = function(offset, size) {
    if (!workerSupportsWrites) {
      return;
    }
    pendingWrites.push({
      type: 'write',
      offset,
      size
    });
  };
  
  // Helper to queue a truncate operation
  const queueTruncate = function(size) {
    if (!workerSupportsWrites) {
      return;
    }
    pendingWrites.push({
      type: 'truncate',
      size
    });
  };
  
  // Helper to queue a delete operation
  const queueDelete = function() {
    if (!workerSupportsWrites) {
      return;
    }
    pendingWrites.push({
      type: 'delete'
    });
  };
  
  // IO Method implementations
  const ioMethods = {
    xClose: function(pFile) {
      const f = openFiles[pFile];
      if (f) {
        delete openFiles[pFile];
        if (f.sq3File) f.sq3File.dispose();
        
        if (f.flags & capi.SQLITE_OPEN_DELETEONCLOSE) {
          fileStorage.delete(f.filename);
          if (isTrackedDbFile(f.filename)) {
            queueDelete();
          }
        }
      }
      return 0;
    },
    
    xRead: function(pFile, pDest, nBytes, offset64) {
      const f = openFiles[pFile];
      if (!f) return capi.SQLITE_IOERR_READ;
      
      try {
        const fileData = fileStorage.get(f.filename);
        if (!fileData) return capi.SQLITE_IOERR_READ;
        
        const offset = Number(offset64);
        const available = Math.max(0, fileData.byteLength - offset);
        const bytesToRead = Math.min(nBytes, available);
        
        if (bytesToRead < nBytes) {
          // Fill remaining space with zeros
          wasm.heap8u().fill(0, pDest, pDest + nBytes);
        }
        
        if (bytesToRead > 0) {
          // Copy data from our storage to the destination
          const srcView = new Uint8Array(fileData, offset, bytesToRead);
          wasm.heap8u().set(srcView, pDest);
        }
        
        return bytesToRead < nBytes ? capi.SQLITE_IOERR_SHORT_READ : 0;
      } catch (e) {
        console.error('xRead error:', e);
        return capi.SQLITE_IOERR_READ;
      }
    },
    
    xWrite: function(pFile, pSrc, nBytes, offset64) {
      const f = openFiles[pFile];
      if (!f) return capi.SQLITE_IOERR_WRITE;
      
      try {
        const offset = Number(offset64);
        let fileData = fileStorage.get(f.filename);
        const requiredSize = offset + nBytes;
        
        // Resize the ArrayBuffer if needed
        if (!fileData || fileData.byteLength < requiredSize) {
          // Create a new, larger buffer
          const newSize = Math.max(requiredSize, fileData ? fileData.byteLength * 2 : 8192);
          const newBuffer = new ArrayBuffer(newSize);
          const newView = new Uint8Array(newBuffer);
          
          // Copy existing data if any
          if (fileData) {
            newView.set(new Uint8Array(fileData));
          }
          
          fileData = newBuffer;
          fileStorage.set(f.filename, fileData);
        }
        
        // Copy data from the source to our storage
        const destView = new Uint8Array(fileData, offset, nBytes);
        destView.set(wasm.heap8u().subarray(pSrc, pSrc + nBytes));
        
        // If this is our synced database file, queue the write operation
        if (isTrackedDbFile(f.filename)) {
          queueWrite(offset, nBytes);
        }
        
        return 0;
      } catch (e) {
        console.error('xWrite error:', e);
        return capi.SQLITE_IOERR_WRITE;
      }
    },
    
    xTruncate: function(pFile, size64) {
      const f = openFiles[pFile];
      if (!f) return capi.SQLITE_IOERR;
      
      try {
        const size = Number(size64);
        const fileData = fileStorage.get(f.filename);
        
        if (fileData) {
          // If requested size is smaller than current, create a smaller buffer
          if (size < fileData.byteLength) {
            const newBuffer = new ArrayBuffer(size);
            new Uint8Array(newBuffer).set(new Uint8Array(fileData, 0, size));
            fileStorage.set(f.filename, newBuffer);
            
            // If this is our synced database file, queue the truncation operation
            if (isTrackedDbFile(f.filename)) {
              queueTruncate(size);
            }
          }
          // If larger, we don't need to do anything as xWrite will handle expansion
        }
        
        return 0;
      } catch (e) {
        console.error('xTruncate error:', e);
        return capi.SQLITE_IOERR;
      }
    },
    
    xSync: function(pFile, flags) {
      // Sync with worker on demand
      if (worker && workerSupportsWrites) {
        sendPendingWrites();
      }
      return 0;
    },
    
    xFileSize: function(pFile, pSize64) {
      const f = openFiles[pFile];
      if (!f) return capi.SQLITE_IOERR;
      
      try {
        const fileData = fileStorage.get(f.filename);
        const size = fileData ? fileData.byteLength : 0;
        wasm.poke(pSize64, size, 'i64');
        return 0;
      } catch (e) {
        console.error('xFileSize error:', e);
        return capi.SQLITE_IOERR;
      }
    },
    
    xLock: function(pFile, lockType) {
      const f = openFiles[pFile];
      if (f) f.lockType = lockType;
      return 0;
    },
    
    xUnlock: function(pFile, lockType) {
      const f = openFiles[pFile];
      if (f) f.lockType = lockType;
      return 0;
    },
    
    xCheckReservedLock: function(pFile, pOut) {
      wasm.poke(pOut, 0, 'i32');
      return 0;
    },
    
    xFileControl: function(pFile, op, pArg) {
      return capi.SQLITE_NOTFOUND;
    },
    
    xDeviceCharacteristics: function(pFile) {
      // Memory storage supports atomic and sequential writes
      return capi.SQLITE_IOCAP_ATOMIC | capi.SQLITE_IOCAP_SEQUENTIAL;
    },
    
    xSectorSize: function(pFile) {
      return 512; // Standard sector size
    }
  };

  // VFS method implementations
  const vfsMethods = {
    xOpen: function(pVfs, zName, pFile, flags, pOutFlags) {
      try {
        // Parse filename from the C string
        let filename = zName ? wasm.cstrToJs(zName) : randomFilename();
        
        // Create a file handle object
        const fh = Object.create(null);
        fh.fid = pFile;
        fh.filename = filename;
        fh.flags = flags;
        fh.lockType = capi.SQLITE_LOCK_NONE;
        fh.readOnly = !(flags & capi.SQLITE_OPEN_CREATE) && !!(flags & capi.SQLITE_OPEN_READONLY);
        
        // Check for buffered initial data
        if (!fileStorage.has(filename) && isTrackedDbFile(filename) && initialData) {
          fileStorage.set(filename, initialData);
          initialData = null; // Use it only once
        }
        
        // If file doesn't exist but we're asked to create it
        if (!fileStorage.has(filename) && (flags & capi.SQLITE_OPEN_CREATE)) {
          fileStorage.set(filename, new ArrayBuffer(0));
        }
        
        // If file doesn't exist and we're not creating, return error
        if (!fileStorage.has(filename)) {
          return capi.SQLITE_CANTOPEN;
        }
        
        // Store the file handle
        openFiles[pFile] = fh;
        
        // Set up the sqlite3_file structure
        fh.sq3File = new capi.sqlite3_file(pFile);
        fh.sq3File.$pMethods = memoryIoMethods.pointer;
        
        // Update out flags if read-only
        if (fh.readOnly && pOutFlags) {
          wasm.poke(pOutFlags, capi.SQLITE_OPEN_READONLY, 'i32');
        }
        
        return 0;
      } catch (e) {
        console.error('xOpen error:', e);
        return capi.SQLITE_CANTOPEN;
      }
    },
    
    xDelete: function(pVfs, zName, syncDir) {
      try {
        const filename = wasm.cstrToJs(zName);
        const result = fileStorage.delete(filename);
        
        // If this is our database file, queue a delete operation
        if (isTrackedDbFile(filename)) {
          queueDelete();
          sendPendingWrites(); // Send immediately
        }
        
        return result ? 0 : capi.SQLITE_IOERR_DELETE;
      } catch (e) {
        console.error('xDelete error:', e);
        return capi.SQLITE_IOERR_DELETE;
      }
    },
    
    xAccess: function(pVfs, zName, flags, pOut) {
      try {
        const filename = wasm.cstrToJs(zName);
        
        // Check in memory storage first
        let exists = fileStorage.has(filename);
        
        // If not found and this is our database file, check initial data
        if (!exists && isTrackedDbFile(filename) && initialData) {
          exists = true;
        }
        
        wasm.poke(pOut, exists ? 1 : 0, 'i32');
        return 0;
      } catch (e) {
        console.error('xAccess error:', e);
        wasm.poke(pOut, 0, 'i32');
        return 0;
      }
    },
    
    xFullPathname: function(pVfs, zName, nOut, pOut) {
      try {
        // Just copy the name as-is for our simple VFS
        const i = wasm.cstrncpy(pOut, zName, nOut);
        return i < nOut ? 0 : capi.SQLITE_CANTOPEN;
      } catch (e) {
        console.error('xFullPathname error:', e);
        return capi.SQLITE_CANTOPEN;
      }
    },
    
    xCurrentTime: function(pVfs, pOut) {
      // Return Julian day with fractional part for the time of day
      wasm.poke(pOut, 2440587.5 + new Date().getTime() / 86400000, 'double');
      return 0;
    },
    
    xCurrentTimeInt64: function(pVfs, pOut) {
      // Return time in milliseconds since Julian epoch
      wasm.poke(pOut, 2440587.5 * 86400000 + new Date().getTime(), 'i64');
      return 0;
    },
    
    xRandomness: function(pVfs, nOut, pOut) {
      // Fill the output buffer with random bytes
      const heap = wasm.heap8u();
      let i = 0;
      for (; i < nOut; ++i) {
        heap[pOut + i] = (Math.random() * 255) & 0xff;
      }
      return i;
    },
    
    xSleep: function(pVfs, microseconds) {
      // No-op for now; could implement with a busy-wait
      return 0;
    },
    
    xGetLastError: function(pVfs, nOut, pOut) {
      // No error mechanism for this simple implementation
      return 0;
    }
  };

  // Register the VFS with SQLite
  sqlite3.vfs.installVfs({
    io: { struct: memoryIoMethods, methods: ioMethods },
    vfs: { struct: memoryVfs, methods: vfsMethods }
  });
  
  // Add a convenience DB constructor if OO1 API is available
  if (sqlite3.oo1) {
    const MemoryWorkerDb = function(...args) {
      const opt = sqlite3.oo1.DB.dbCtorHelper.normalizeArgs(...args);
      opt.vfs = vfsName;
      sqlite3.oo1.DB.dbCtorHelper.call(this, opt);
    };
    MemoryWorkerDb.prototype = Object.create(sqlite3.oo1.DB.prototype);
    sqlite3.oo1.MemoryWorkerDb = MemoryWorkerDb;
  }
  
  // Return the VFS controller object with utility methods
  return {
    vfs: memoryVfs,
    name: vfsName,
    
    /**
     * Clears all storage in the memory VFS
     */
    clearStorage: function() {
      fileStorage.clear();
    },
    
    /**
     * Gets file data as ArrayBuffer
     * 
     * @param {string} filename - The name of the file to retrieve
     * @returns {ArrayBuffer|undefined} - The file data or undefined if not found
     */
    getFileData: function(filename) {
      return fileStorage.get(filename);
    },
    
    /**
     * Export the database as an ArrayBuffer
     * 
     * @param {string} [filename] - Optional filename (defaults to configured dbName)
     * @returns {ArrayBuffer|null} - The database content or null if not found
     */
    exportDb: function(filename) {
      const pathname = filename || dbName;
      const buffer = fileStorage.get(pathname);
      return buffer ? buffer.slice(0) : null; // Return a copy
    },
    
    /**
     * Imports a database from an ArrayBuffer
     * 
     * @param {ArrayBuffer} buffer - The database content
     * @param {string} [filename] - Optional filename (defaults to configured dbName)
     */
    importDb: function(buffer, filename) {
      if (!(buffer instanceof ArrayBuffer)) {
        throw new Error("Data must be an ArrayBuffer");
      }
      fileStorage.set(filename || dbName, buffer.slice(0)); // Use slice to clone the buffer
    },
    
    /**
     * Shuts down the VFS and releases all resources
     */
    shutdown: function() {
      if (writeIntervalId) {
        clearInterval(writeIntervalId);
        writeIntervalId = null;
      }
      
      if (worker) {
        sendPendingWrites(); // Final sync
        setTimeout(() => {
          worker.terminate();
        }, 100);
      }
      
      fileStorage.clear();
    }
  };
}
