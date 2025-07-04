/**
 * sqlite-wasm-memory-vfs.js
 * 
 * A SQLite VFS for sqlite-wasm that uses in-memory ArrayBuffers for storage.
 * This can be imported as an ES module.
 */

/**
 * Creates and registers a memory-based VFS for sqlite-wasm
 * 
 * @param {Object} sqlite3 - The sqlite3 module from sqlite-wasm
 * @param {string} vfsName - The name this VFS will be registered under
 * @param {Object} options - Configuration options
 * @param {string} [options.name='memory'] - Name for the VFS
 * @returns {Object} VFS controller with utility methods
 */
export function registerVfs(sqlite3, vfsName, options = {}) {
  if (!sqlite3 || !sqlite3.capi || !sqlite3.wasm) {
    throw new Error("sqlite3 argument is required and must have capi and wasm properties.");
  }

  const capi = sqlite3.capi;
  const wasm = sqlite3.wasm;
  
  // Create VFS and IO Methods structures
  const memoryIoMethods = new capi.sqlite3_io_methods();
  const memoryVfs = new capi.sqlite3_vfs();
  
  // Store open files, keyed by file ID (sqlite3_file pointer)
  const openFiles = Object.create(null);
  
  // Store file data, keyed by filename
  const fileStorage = new Map();

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

  // Helper to generate a random filename if none is specified
  const randomFilename = function() {
    return 'memory-' + Math.random().toString(36).slice(2);
  };

  // IO Method implementations
  const ioMethods = {
    xClose: function(pFile) {
      const f = openFiles[pFile];
      if (f) {
        delete openFiles[pFile];
        if (f.sq3File) f.sq3File.dispose();
      }
      return 0;
    },
    
    xRead: function(pFile, pDest, nBytes, offset64) {
      // console.log('SqliteWasmMemoryVFS.xRead called with', {pFile, pDest, nBytes, offset64});
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
      // console.log('SqliteWasmMemoryVFS.xWrite called with', {pFile, pSrc, nBytes, offset64});
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
      // No-op for memory VFS
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
      // console.log('SQLiteWasmMemoryVFS | xOpen called with', {zName});
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
        if (fh.readOnly) {
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
        return result ? 0 : capi.SQLITE_IOERR_DELETE;
      } catch (e) {
        console.error('xDelete error:', e);
        return capi.SQLITE_IOERR_DELETE;
      }
    },
    
    xAccess: function(pVfs, zName, flags, pOut) {
      try {
        const filename = wasm.cstrToJs(zName);
        const exists = fileStorage.has(filename);
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
    const MemoryDb = function(...args) {
      const opt = sqlite3.oo1.DB.dbCtorHelper.normalizeArgs(...args);
      opt.vfs = vfsName;
      sqlite3.oo1.DB.dbCtorHelper.call(this, opt);
    };
    MemoryDb.prototype = Object.create(sqlite3.oo1.DB.prototype);
    sqlite3.oo1.MemoryDb = MemoryDb;
  }
  
  // Return the VFS controller object with utility methods
  return {
    vfs: memoryVfs,
    name: vfsName,
    
    isReady: function() {
      return true;
    },

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
     * Lists all files currently in storage
     * 
     * @returns {string[]} - Array of filenames
     */
    listFiles: function() {
      return [...fileStorage.keys()];
    },
    
    /**
     * Sets file data from ArrayBuffer
     * 
     * @param {string} filename - The name of the file to create or update
     * @param {ArrayBuffer} data - The data to store
     */
    setFileData: function(filename, data) {
      if (!(data instanceof ArrayBuffer)) {
        throw new Error("Data must be an ArrayBuffer");
      }
      fileStorage.set(filename, data.slice(0)); // Use slice to clone the buffer
    },
    
    /**
     * Imports a database from an ArrayBuffer
     * 
     * @param {string} filename - The name to give the database file
     * @param {ArrayBuffer} buffer - The database content as an ArrayBuffer
     */
    importDb: function(filename, buffer) {
      this.setFileData(filename, buffer);
    },
    
    /**
     * Exports a database as an ArrayBuffer
     * 
     * @param {string} filename - The name of the database file
     * @returns {ArrayBuffer|null} - The database content or null if not found
     */
    exportDb: function(filename) {
      const buffer = this.getFileData(filename);
      return buffer ? buffer.slice(0) : null; // Return a copy
    }
  };
}
