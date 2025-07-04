/**
 * SqliteWasmMemoryWorkerJournaledVFS.js
 * 
 * A VFS for sqlite-wasm that uses synchronous in-memory storage with asynchronous syncing to a worker.
 * This VFS supports multiple files (database + journal/WAL) with optimized partial syncing.
 * 
 * Based on MemoryWorkerJournaledVFS.js
 */

/**
 * Register this VFS with sqlite-wasm
 * 
 * @param {Object} sqlite3 - The sqlite3 object from sqlite-wasm
 * @param {string} vfsName - The name this VFS will be registered under
 * @param {Object} options - Configuration options
 * @param {string} [options.dbName='db.sqlite'] - The name of the database file
 * @param {Worker|null} [options.worker=null] - Worker for persistence, or null for memory-only
 * @param {string} [options.encryptionPassword] - Optional encryption password
 * @returns {Object} The VFS controller
 */
export function registerVfs(sqlite3, vfsName, options = {}) {
  if (!sqlite3 || !sqlite3.capi || !sqlite3.wasm) {
    throw new Error("sqlite3 argument is required and must have capi and wasm properties.");
  }
  
  const capi = sqlite3.capi;
  const wasm = sqlite3.wasm;
  
  // Extract options
  const dbName = options.dbName || 'db.sqlite';
  let worker = (options.worker instanceof Function) ? options.worker() : options.worker;
  const encryptionPassword = options.encryptionPassword;
  
  // Create VFS and IO Methods structures
  const ioMethodsStruct = new capi.sqlite3_io_methods();
  const vfsStruct = new capi.sqlite3_vfs();
  
  // Store open files, keyed by file ID (sqlite3_file pointer)
  const openFiles = Object.create(null);
  
  // In-memory storage for files
  const fileStorage = new Map();
  const initialFiles = new Map();
  const pendingWritesMap = new Map();
  const syncCount = new Map();
  const fileSizes = new Map();
  let workerSupportsWrites = true;
  let initCompletePromise = null;
  let initCompleteResolve = null;
  
  // VFS configuration
  vfsStruct.$iVersion = 2;
  vfsStruct.$szOsFile = capi.sqlite3_file.structInfo.sizeof;
  vfsStruct.$mxPathname = 1024;
  vfsStruct.$zName = wasm.allocCString(vfsName);
  
  // Set to null since we don't need dynamic library support
  vfsStruct.$xDlOpen = vfsStruct.$xDlError = vfsStruct.$xDlSym = vfsStruct.$xDlClose = null;

  // Clean up resources when disposing
  vfsStruct.addOnDispose('$zName', vfsStruct.$zName);
  ioMethodsStruct.$iVersion = 1;

  // Create the initialization promise
  initCompletePromise = new Promise((resolve) => {
    initCompleteResolve = resolve;
  });

  // Initialize worker if provided
  if (worker) {
    worker.addEventListener('message', (event) => {
      // console.log('SqliteWasmMemoryToWorkerVFS | Received message from worker:', event.data);
      const msg = event.data;
      if (msg.type === 'initComplete') {
        // Store the initial file data for later use when opening files
        if (msg.files && Array.isArray(msg.files)) {
          for (const fileInfo of msg.files) {
            // Store file data with the internal name (without prefix)
            initialFiles.set(fileInfo.name, fileInfo.data.buffer);
          }
        }
        
        workerSupportsWrites = msg.writesEnabled ?? true;
        
        // Resolve the initialization promise
        if (initCompleteResolve) {
          initCompleteResolve(true);
          initCompleteResolve = null;
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
  } else {
    // No worker provided, resolve immediately
    if (initCompleteResolve) {
      initCompleteResolve(true);
      initCompleteResolve = null;
    }
  }
  
  // Helper to generate a random filename if none is specified
  const randomFilename = function() {
    return 'memory-' + Math.random().toString(36).slice(2);
  };
  
  // Helper to check if a file is tracked (database or journal/WAL)
  const isTrackedFile = function(pathname) {
    return pathname.startsWith(dbName);
  };
  
  // Helper to sync a specific file to the worker
  const syncFileToWorker = function(filename) {
    if (!workerSupportsWrites || !worker) {
      return;
    }
    
    // Get the current write queue for this file
    const queue = pendingWritesMap.get(filename) || [];
    const operations = queue.splice(0);
    
    if (operations.length === 0) {
      return;
    }
    
    // Get the file data
    const fileData = fileStorage.get(filename);
    if (!fileData) {
      return;
    }
    
    // Calculate the range of data affected by pending writes
    let minOffset = Number.MAX_SAFE_INTEGER;
    let maxOffset = 0;
    
    for (const op of operations) {
      if (op.type === 'write') {
        minOffset = Math.min(minOffset, op.offset);
        maxOffset = Math.max(maxOffset, (op.offset + op.size));
      } else if (op.type === 'truncate') {
        maxOffset = op.size;
      } else if (op.type === 'delete') {
        // For delete, we don't need any data
        minOffset = 0;
        maxOffset = 0;
      }
    }
    
    // If no actual range was calculated, snapshot the entire file
    if (minOffset === Number.MAX_SAFE_INTEGER) {
      minOffset = 0;
      maxOffset = fileData.byteLength;
    }
    const rangeSize = maxOffset - minOffset;
    
    // Create a copy of just the affected range
    const dataCopy = new ArrayBuffer(rangeSize);
    const snapshotData = new Uint8Array(dataCopy);
    if (rangeSize > 0) {
      snapshotData.set(
        new Uint8Array(fileData, minOffset, rangeSize)
      );
    }
    
    // Get the logical file size
    const logicalSize = fileSizes.get(filename) || 0;
    
    // Send the snapshot and operations to the worker
    worker.postMessage({
      type: 'writes',
      operations: operations,
      filename: filename,
      databaseState: snapshotData,
      startOffset: minOffset,
      totalSize: logicalSize
    }, [snapshotData.buffer]);
  };
  
  // Helper to queue a write operation
  const queueWrite = function(filename, offset, size) {
    if (!workerSupportsWrites || !isTrackedFile(filename)) {
      return;
    }
    
    // Make sure we have a queue for this file
    if (!pendingWritesMap.has(filename)) {
      pendingWritesMap.set(filename, []);
    }
    const queue = pendingWritesMap.get(filename);
    queue.push({
      type: 'write',
      offset,
      size
    });
  };
  
  // Helper to queue a truncate operation
  const queueTruncate = function(filename, size) {
    if (!workerSupportsWrites || !isTrackedFile(filename)) {
      return;
    }
    
    // Make sure we have a queue for this file
    if (!pendingWritesMap.has(filename)) {
      pendingWritesMap.set(filename, []);
    }
    const queue = pendingWritesMap.get(filename);
    queue.push({
      type: 'truncate',
      size
    });
  };
  
  // Helper to queue a delete operation
  const queueDelete = function(filename) {
    if (!workerSupportsWrites || !isTrackedFile(filename)) {
      return;
    }
    
    // Make sure we have a queue for this file
    if (!pendingWritesMap.has(filename)) {
      pendingWritesMap.set(filename, []);
    }
    const queue = pendingWritesMap.get(filename);
    queue.push({
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
          fileSizes.delete(f.filename);
          if (isTrackedFile(f.filename)) {
            queueDelete(f.filename);
            syncFileToWorker(f.filename);
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
        
        // Update the logical file size (maximum of current size and end of this write)
        const currentSize = fileSizes.get(f.filename) || 0;
        const newLogicalSize = Math.max(currentSize, offset + nBytes);
        fileSizes.set(f.filename, newLogicalSize);
        
        // If this is a tracked file, queue the write operation
        if (isTrackedFile(f.filename)) {
          queueWrite(f.filename, offset, nBytes);
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
        fileSizes.set(f.filename, size);

        const fileData = fileStorage.get(f.filename);
        if (fileData) {
          // If requested size is smaller than current buffer, create a smaller buffer
          if (size < fileData.byteLength) {
            const newBuffer = new ArrayBuffer(size);
            new Uint8Array(newBuffer).set(new Uint8Array(fileData, 0, size));
            fileStorage.set(f.filename, newBuffer);
            
            // If this is a tracked file, queue the truncation operation
            if (isTrackedFile(f.filename)) {
              queueTruncate(f.filename, size);
            }
          }
          // If larger, we don't need to resize the buffer as xWrite will handle expansion
        }
        
        return 0;
      } catch (e) {
        console.error('xTruncate error:', e);
        return capi.SQLITE_IOERR;
      }
    },
    
    xSync: function(pFile, flags) {
      const f = openFiles[pFile];
      if (!f) {
        return 0;
      }
      
      // Only track sync for files we're tracking
      if (!isTrackedFile(f.filename)) {
        return 0;
      }
      
      syncCount.set(f.filename, (syncCount.get(f.filename) || 0) + 1);
      console.log(`SqliteWasmMemoryWorkerJournaledVFS | Syncing file ${f.filename} (${syncCount.get(f.filename)} times)`);
      
      // Sync with worker on demand
      if (worker && workerSupportsWrites) {
        syncFileToWorker(f.filename);
      }
      return 0;
    },
    
    xFileSize: function(pFile, pSize64) {
      const f = openFiles[pFile];
      if (!f) return capi.SQLITE_IOERR;
      
      try {
        // Return the logical file size, not the buffer size
        const size = fileSizes.get(f.filename) || 0;
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
        if (!fileStorage.has(filename) && initialFiles.has(filename)) {
          // Use the buffered data for this file
          const initialData = initialFiles.get(filename);
          fileStorage.set(filename, initialData);
          // Initialize the logical file size to the buffer size
          // TODO: The worker should send the logical size separately
          fileSizes.set(filename, initialData.byteLength);
          // Remove from initialFiles after it's been used
          initialFiles.delete(filename);
        }
        
        // If file doesn't exist but we're asked to create it
        if (!fileStorage.has(filename) && (flags & capi.SQLITE_OPEN_CREATE)) {
          fileStorage.set(filename, new ArrayBuffer(0));
          // Initialize the logical file size to 0
          fileSizes.set(filename, 0);
        }
        
        // If file doesn't exist and we're not creating, return error
        if (!fileStorage.has(filename)) {
          return capi.SQLITE_CANTOPEN;
        }
        
        // Store the file handle
        openFiles[pFile] = fh;
        
        // Set up the sqlite3_file structure
        fh.sq3File = new capi.sqlite3_file(pFile);
        fh.sq3File.$pMethods = ioMethodsStruct.pointer;
        
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
        fileSizes.delete(filename);
        
        // If this is a tracked file, queue a delete operation
        if (isTrackedFile(filename)) {
          queueDelete(filename);
          syncFileToWorker(filename);
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
        
        // First check if the file exists in our in-memory map
        const fileExists = fileStorage.has(filename);
        
        // If not found in memory, check if we have a buffered version
        const isBuffered = !fileExists && initialFiles.has(filename);
        
        // A file exists if it's either in memory or in our buffer
        wasm.poke(pOut, (fileExists || isBuffered) ? 1 : 0, 'i32');
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
    io: { struct: ioMethodsStruct, methods: ioMethods },
    vfs: { struct: vfsStruct, methods: vfsMethods }
  });
  
  // Add a convenience DB constructor if OO1 API is available
  if (sqlite3.oo1) {
    const MemoryWorkerJournaledDb = function(...args) {
      const opt = sqlite3.oo1.DB.dbCtorHelper.normalizeArgs(...args);
      opt.vfs = vfsName;
      sqlite3.oo1.DB.dbCtorHelper.call(this, opt);
    };
    MemoryWorkerJournaledDb.prototype = Object.create(sqlite3.oo1.DB.prototype);
    sqlite3.oo1.MemoryWorkerJournaledDb = MemoryWorkerJournaledDb;
  }
  
  // Return the VFS controller object with utility methods
  return {
    vfs: vfsStruct,
    name: vfsName,
    
    /**
     * Returns a promise that resolves when the worker has initialized and initial data has been loaded
     * @returns {Promise<boolean>} Promise that resolves after initComplete message is received
     */
    isReady: function() {
      return initCompletePromise;
    },
    
    /**
     * Get a readable indicator of whether the VFS is read-only
     * @returns {boolean} True if VFS is read-only, false otherwise
     */
    get isReadOnly() {
      return !workerSupportsWrites;
    },
    
    /**
     * Export a copy of the current database from memory
     * @returns {ArrayBuffer|null} Database contents or null if not found
     */
    exportDatabase: function() {
      // Return a copy of the current SQLite database file
      // The main database file is named according to the dbName
      const filename = dbName;
      const fileData = fileStorage.get(filename);
      
      if (fileData) {
        // Create a new ArrayBuffer to hold the copied data
        const newBuffer = new ArrayBuffer(fileData.byteLength);
        const sourceView = new Uint8Array(fileData);
        const newView = new Uint8Array(newBuffer);
        
        // Copy all data from original to new buffer
        newView.set(sourceView);
        
        // Return this copied buffer
        return newBuffer;
      }
      return null;
    },
    
    /**
     * Destroy the database file in memory and trigger worker deletion
     */
    destroyDatabase: function() {
      // Delete all files
      for (const filename of fileStorage.keys()) {
        if (isTrackedFile(filename)) {
          queueDelete(filename);
          syncFileToWorker(filename);
        }
      }
      
      // Clear all maps
      fileStorage.clear();
      initialFiles.clear();
      pendingWritesMap.clear();
      fileSizes.clear();
    },
    
    /**
     * Shut down the VFS and release resources
     */
    terminate: function() {
      if (worker) {
        worker.terminate();
        worker = null;
      }
      fileStorage.clear();
      for (const fileId of Object.keys(openFiles)) {
        delete openFiles[fileId];
      }
      initialFiles.clear();
      pendingWritesMap.clear();
      fileSizes.clear();
    },
    
    /**
     * Process all pending operations and close the VFS
     */
    close: function() {
      // Close all open files
      for (const fileId of Object.keys(openFiles)) {
        ioMethods.xClose(fileId);
      }

      // Sync all tracked files
      for (const filename of fileStorage.keys()) {
        if (isTrackedFile(filename)) {
          syncFileToWorker(filename);
        }
      }

      // Terminate the worker after a short delay to allow pending operations to complete
      setTimeout(() => {
        if (worker) {
          worker.terminate();
          worker = null;
        }
      }, 100);
    },
    
    /**
     * Clears all storage in the memory VFS
     */
    clearStorage: function() {
      fileStorage.clear();
      fileSizes.clear();
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
      const fname = filename || dbName;
      fileStorage.set(fname, buffer.slice(0)); // Use slice to clone the buffer
      fileSizes.set(fname, buffer.byteLength);
    },
    
    /**
     * Shuts down the VFS and releases all resources
     */
    shutdown: function() {      
      if (worker) {
        worker.terminate();
      }
      
      fileStorage.clear();
      pendingWritesMap.clear();
      initialFiles.clear();
      fileSizes.clear();
    }
  };
}
