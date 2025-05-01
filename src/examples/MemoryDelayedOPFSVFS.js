import { FacadeVFS } from '../FacadeVFS.js';
import * as VFS from '../VFS.js';

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
  #opfsFilename = 'db.sqlite';

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

  async #initOpfs() {
    try {
      // Request access to the root directory
      this.#rootDir = await navigator.storage.getDirectory();
      
      try {
        // Try to open existing file
        this.#dbFileHandle = await this.#rootDir.getFileHandle(this.#opfsFilename);
        
        // Load existing data from OPFS
        await this.#loadFromOpfs();
      } catch (e) {
        // File doesn't exist yet, will be created when needed
        console.log(`OPFS file doesn't exist yet: ${e.message}`);
      }
      
      return true;
    } catch (e) {
      console.error(`Failed to initialize OPFS: ${e.message}`);
      return false;
    }
  }

  async #loadFromOpfs() {
    if (!this.#dbFileHandle) return;
    
    try {
      // Open the file for reading
      const file = await this.#dbFileHandle.getFile();
      const data = await file.arrayBuffer();
      
      // Create a file object with the data
      const pathname = `/${this.#opfsFilename}`;
      const fileObj = {
        pathname,
        flags: VFS.SQLITE_OPEN_READWRITE, // Default flags
        size: data.byteLength,
        data: data
      };
      
      // Add to our in-memory map
      this.mapNameToFile.set(pathname, fileObj);
      console.log(`Loaded ${data.byteLength} bytes from OPFS`);
    } catch (e) {
      console.error(`Failed to load from OPFS: ${e.message}`);
    }
  }

  async #writeToOpfs(pathname, data, size) {
    // Don't wait for OPFS to be ready, as this is a fire-and-forget operation
    this.#opfsReady.then(async (ready) => {
      if (!ready) {
        console.error('OPFS is not ready, cannot write');
        return;
      }
      
      try {
        // Create the file if it doesn't exist
        if (!this.#dbFileHandle) {
          this.#dbFileHandle = await this.#rootDir.getFileHandle(this.#opfsFilename, { create: true });
        }
        
        // Create a writable stream
        const writable = await this.#dbFileHandle.createWritable();
        
        // Write the data
        await writable.write(new Uint8Array(data, 0, size));
        
        // Close the stream
        await writable.close();
        
        console.log(`Wrote ${size} bytes to OPFS asynchronously`);
      } catch (e) {
        console.error(`Failed to write to OPFS: ${e.message}`);
      }
    });
  }

  async isReady() {
    return this.#opfsReady;
  }

  close() {
    for (const fileId of this.mapIdToFile.keys()) {
      this.jClose(fileId);
    }
  }

  /**
   * @param {string?} filename 
   * @param {number} fileId 
   * @param {number} flags 
   * @param {DataView} pOutFlags 
   * @returns {number|Promise<number>}
   */
  jOpen(filename, fileId, flags, pOutFlags) {
    const url = new URL(filename || Math.random().toString(36).slice(2), 'file://');
    const pathname = url.pathname;

    let file = this.mapNameToFile.get(pathname);
    if (!file) {
      if (flags & VFS.SQLITE_OPEN_CREATE) {
        // Create a new file object.
        file = {
          pathname,
          flags,
          size: 0,
          data: new ArrayBuffer(0)
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
    const file = this.mapIdToFile.get(fileId);
    this.mapIdToFile.delete(fileId);

    if (file.flags & VFS.SQLITE_OPEN_DELETEONCLOSE) {
      this.mapNameToFile.delete(file.pathname);
      
      // Attempt to delete from OPFS if this is our database file
      if (file.pathname === `/${this.#opfsFilename}` && this.#dbFileHandle) {
        this.#opfsReady.then(async (ready) => {
          if (ready && this.#rootDir) {
            try {
              await this.#rootDir.removeEntry(this.#opfsFilename);
              this.#dbFileHandle = null;
            } catch (e) {
              console.error(`Failed to delete file from OPFS: ${e.message}`);
            }
          }
        });
      }
    } else {
      // Final persistence of the file to OPFS on close
      if (file.pathname === `/${this.#opfsFilename}`) {
        this.#writeToOpfs(file.pathname, file.data, file.size);
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
    const file = this.mapIdToFile.get(fileId);
    if (iOffset + pData.byteLength > file.data.byteLength) {
      // Resize the ArrayBuffer to hold more data.
      const newSize = Math.max(iOffset + pData.byteLength, 2 * file.data.byteLength);
      const data = new ArrayBuffer(newSize);
      new Uint8Array(data).set(new Uint8Array(file.data, 0, file.size));
      file.data = data;
    }

    // Copy data.
    new Uint8Array(file.data, iOffset, pData.byteLength).set(pData);
    file.size = Math.max(file.size, iOffset + pData.byteLength);
    
    // If this is our database file, asynchronously persist to OPFS
    if (file.pathname === `/${this.#opfsFilename}`) {
      // Don't await - fire and forget
      this.#writeToOpfs(file.pathname, file.data, file.size);
    }
    
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} fileId 
   * @param {number} iSize 
   * @returns {number|Promise<number>}
   */
  jTruncate(fileId, iSize) {
    const file = this.mapIdToFile.get(fileId);

    // For simplicity we don't make the ArrayBuffer smaller.
    file.size = Math.min(file.size, iSize);
    
    // If this is our database file, asynchronously persist truncated file to OPFS
    if (file.pathname === `/${this.#opfsFilename}`) {
      this.#writeToOpfs(file.pathname, file.data, file.size);
    }
    
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} fileId 
   * @param {DataView} pSize64 
   * @returns {number|Promise<number>}
   */
  jFileSize(fileId, pSize64) {
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
    const url = new URL(name, 'file://');
    const pathname = url.pathname;

    this.mapNameToFile.delete(pathname);
    
    // If this is our database file, delete from OPFS
    if (pathname === `/${this.#opfsFilename}` && this.#dbFileHandle) {
      this.#opfsReady.then(async (ready) => {
        if (ready && this.#rootDir) {
          try {
            await this.#rootDir.removeEntry(this.#opfsFilename);
            this.#dbFileHandle = null;
          } catch (e) {
            console.error(`Failed to delete file from OPFS: ${e.message}`);
          }
        }
      });
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
    const url = new URL(name, 'file://');
    const pathname = url.pathname;

    const file = this.mapNameToFile.get(pathname);
    pResOut.setInt32(0, file ? 1 : 0, true);
    return VFS.SQLITE_OK;
  }
}
