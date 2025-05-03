import { FacadeVFS } from "../FacadeVFS.js";
import * as VFS from "../VFS.js";

export async function buildEncryptionKey(fromPassword) {
  // Initialize encryption key from password
  const encoder = new TextEncoder();
  const passwordData = encoder.encode(fromPassword);
  
  // Derive a key from the password
  const keyMaterial = await crypto.subtle.importKey(
    "raw",
    passwordData,
    "PBKDF2",
    false,
    ["deriveBits", "deriveKey"]
  );
  
  // Use PBKDF2 to derive a key
  return await crypto.subtle.deriveKey(
    {
      name: "PBKDF2",
      salt: encoder.encode("wa-sqlite-encrypted-vfs"),
      iterations: 100000,
      hash: "SHA-256"
    },
    keyMaterial,
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"]
  );
}

/**
 * @typedef {Object} MappedFile
 * @property {string} pathname
 * @property {number} flags
 * @property {number} size
 * @property {ArrayBuffer} data
 */

/**
 * @typedef {Object} PageMeta
 * @property {number} pageIndex
 * @property { {offset: number, length: number, iv: Uint8Array} } encData
 */

// Memory-based VFS with asynchronous OPFS persistence.
export class MemoryDelayedOPFSVFS extends FacadeVFS {
  // Map of existing files, keyed by filename.
  /** @type {Map<string, MappedFile>} */ mapNameToFile = new Map();

  // Map of open files, keyed by id (sqlite3_file pointer).
  /** @type {Map<number, MappedFile>} */ mapIdToFile = new Map();

  // OPFS root directory handle
  /** @type {FileSystemDirectoryHandle} */ #rootDir = null;

  // OPFS file handle for the database
  /** @type {FileSystemFileHandle} */ #dbFileHandle = null;

  // Track if OPFS is ready
  /** @type {Promise<boolean>} */ #vfsReady = null;

  // Database filename in OPFS
  #opfsFilename = "db.sqlite";

  // OPFS page size for storage (default 64KB)
  #opfsPageSize = 65536;

  // Queue for pending writes to OPFS - stores regions to be written
  #writeQueue = [];

  // Flag to track if write processing is active
  #isProcessingWrites = false;

  // Buffer for loaded OPFS data (not immediately added to mapNameToFile)
  /** @type {ArrayBuffer} */ #opfsDataBuffer = null;
  
  /** @type {string} */ #encryptionPassword = null;
  /** @type {CryptoKey} */ #encryptionKey = null;
  
  // In-memory PageMeta storage (maps OPFS page index to encryption/plaintext metadata)
  /** @type {Map<number, PageMeta>} */ #pages = new Map();
  
  // IDB database for page metadata storage
  /** @type {IDBDatabase} */ #idb = null;

  /**
   * @param {string} name 
   * @param {*} module
   * @param {{key?: CryptoKey, dbName?: string, opfsPageSize?: number}} options - Optional encryption key and configuration
   * @returns 
   */
  static async create(name, module, options = {}) {
    const vfs = new MemoryDelayedOPFSVFS(name, module, options);
    await vfs.isReady();
    return vfs;
  }

  /**
   * @param {string} name 
   * @param {*} module
   * @param {{encryptionPassword?: string, dbName?: string, opfsPageSize?: number}} options
   * @returns 
   */
  constructor(name, module, options) {
    super(name, module);
    this.#opfsFilename = options.dbName ?? "db.sqlite";
    this.#encryptionPassword = options.encryptionPassword;
    if (options.opfsPageSize) {
      this.#opfsPageSize = options.opfsPageSize;
    }

    this.#vfsReady = this.init()
    // console.log("MemoryDelayedOPFSVFS constructor complete")
  }

  async isReady() {
    // console.log("MemoryDelayedOPFSVFS isReady, waiting for vfsReady")
    return this.#vfsReady;
  }

  async init() {
    if (this.#encryptionPassword) {
      this.#encryptionKey = await buildEncryptionKey(this.#encryptionPassword);
    }
    // Initialize IndexedDB first (so we have the IVs), then OPFS (where we decrypt the existing db)
    // console.log("MemoryDelayedOPFSVFS constructor about to call initIndexedDb")
    await this.#initIndexedDb()
    await this.#initOpfs()
    return true;
  }

  /**
   * Encrypt data using AES-GCM with a random IV
   * @param {Uint8Array} data - Data to encrypt
   * @returns {Promise<{encryptedData: Uint8Array, iv: Uint8Array}>}
   */
  async #encryptData(data) { 
    // Generate a random IV
    const iv = crypto.getRandomValues(new Uint8Array(12)); // 12 bytes is recommended for AES-GCM
    
    // Encrypt the data
    const encryptedBuffer = await crypto.subtle.encrypt(
      {
        name: 'AES-GCM',
        iv
      },
      this.#encryptionKey,
      data
    );
    const encryptedData = new Uint8Array(encryptedBuffer);

    // Return both the encrypted data and the IV
    return {
      encryptedData: encryptedData,
      iv
    };
  }

  /**
   * Decrypt data using AES-GCM
   * @param {Uint8Array} encryptedData - Encrypted data
   * @param {Uint8Array} iv - Initialization vector used for encryption
   * @returns {Promise<Uint8Array>}
   */
  async #decryptData(encryptedData, iv) {
    // Decrypt the data
    const decryptedBuffer = await crypto.subtle.decrypt(
      {
        name: 'AES-GCM',
        iv
      },
      this.#encryptionKey,
      encryptedData
    );
    
    // Return the decrypted data
    const decrypted = new Uint8Array(decryptedBuffer);
    return decrypted;
  }

  /**
   * Decrypt a file from OPFS using stored page metadata
   * @param {ArrayBuffer} encryptedData - The encrypted file data from OPFS
   * @returns {Promise<ArrayBuffer>} - The decrypted file data
   */
  async #decryptFile(encryptedData) {
    if (encryptedData.byteLength === 0) {
      return encryptedData;
    }

    console.log(`Decrypting ${encryptedData.byteLength} bytes from OPFS, with ${this.#pages.size} OPFS pages`);
    
    // Calculate the size needed for the decrypted buffer
    // We need to find the highest page index and determine its end offset
    let maxPlainEnd = 0;
    
    for (const [pageIndex, pageEntry] of this.#pages.entries()) {
      const pageEnd = this.#getOpfsPageEnd(pageIndex);
      if (pageEnd > maxPlainEnd) {
        maxPlainEnd = pageEnd;
      }
    }
    
    // Create a new buffer for the decrypted data with the correct size
    const decryptedBuffer = new ArrayBuffer(maxPlainEnd);
    const decryptedView = new Uint8Array(decryptedBuffer);
    
    // Sort page indices for sequential processing
    const pageIndices = Array.from(this.#pages.keys()).sort((a, b) => a - b);
    
    // Decrypt each page and write it to the right location in the buffer
    for (const pageIndex of pageIndices) {
      const pageEntry = this.#pages.get(pageIndex);
      const pageStart = this.#getOpfsPageStart(pageIndex);
      const { offset: encOffset, length: encLength, iv } = pageEntry.encData;
      
      if (encOffset === undefined || encLength === undefined || !iv) {
        console.warn(`Missing encryption data for page ${pageIndex}`);
        continue;
      }
      
      try {
        // Extract the encrypted data
        const encryptedChunk = new Uint8Array(encryptedData, encOffset, encLength);
        
        // Decrypt the data
        const decryptedChunk = await this.#decryptData(encryptedChunk, iv);
        
        // Copy the decrypted data to the output buffer
        decryptedView.set(
          new Uint8Array(decryptedChunk.buffer, 0, decryptedChunk.byteLength), 
          pageStart
        );
      } catch (e) {
        console.error(`Error decrypting page ${pageIndex}: ${e.message}`);
      }
    }
    
    console.log(`Finished decrypting file, produced ${maxPlainEnd} bytes of plaintext`);
    return decryptedBuffer;
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
      this.#opfsDataBuffer = await this.#readFileFromOPFS();

      return true;
    } catch (e) {
      console.error(`Failed to initialize OPFS`, e.message, e.stack, e.className);
      return false;
    }
  }

  async #readFileFromOPFS() {
    // Load existing data from OPFS
    const file = await this.#dbFileHandle.getFile();
    let encryptedData = await file.arrayBuffer();

    // Decrypt the file if encryption is enabled and we have data
    if (encryptedData.byteLength > 0 && this.#encryptionKey) {
      try {
      return await this.#decryptFile(encryptedData);
      } catch (e) {
        if (e instanceof DOMException && e.name === "OperationError") {
          console.error("Incorrect encryption key or corrupted data");
          return new ArrayBuffer(0);
        } else {
          throw e;
        }
      }
    } else {
      // No encryption or empty file, just store the data as-is
      return encryptedData;
    }
  }
  
  /**
   * Execute an IndexedDB transaction and return the result
   * @param {IDBTransactionMode} mode - Transaction mode ('readonly' or 'readwrite')
   * @param {function(IDBObjectStore): IDBRequest} operation - Function that performs the operation on the store
   * @returns {Promise<any>} - Result of the operation
   */
  async #executeIDBTransaction(mode, operation) {
    if (!this.#idb) {
      throw new Error("IndexedDB not initialized");
    }
    
    return new Promise((resolve, reject) => {
      const tx = this.#idb.transaction("pages", mode);
      const store = tx.objectStore("pages");
      
      const request = operation(store);
      
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  }

  async #initIndexedDb() {
      // Initialize IndexedDB for page storage
      this.#idb = await new Promise((resolve, reject) => {
        const request = indexedDB.open(`MemoryDelayedOPFSVFS-${this.#opfsFilename}`, 1);
        request.onupgradeneeded = (event) => {
          const db = request.result;
          db.createObjectStore('pages', { keyPath: 'pageIndex' });
        };
        request.onsuccess = () => {
          resolve(request.result);
        }
        request.onerror = () => {
          reject(request.error);
        }
      });
      
      // Load the pages from IndexedDB
      await this.#loadPageMetas();
      return true;
  }
  
  /**
   * Load pages from IndexedDB
   */
  async #loadPageMetas() {
    try {
      const result = await this.#executeIDBTransaction('readonly', store => store.getAll());
      
      // Populate the pages map
      for (const entry of result) {
        this.#pages.set(entry.pageIndex, entry);
      }
    } catch (e) {
      console.error(`Failed to load pages from IndexedDB: ${e.message}`);
    }
  }
  
  /**
   * Save page data to IndexedDB
   * @param {number} pageIndex - The page index
   * @param {number} encOffset - The encrypted offset in the file
   * @param {number} encLength - The length of the encrypted data
   * @param {Uint8Array} iv - The initialization vector used for encryption
   */
  async #savePageMeta(pageIndex, encOffset, encLength, iv) {
    try {
      // Create the page entry
      const entry = { 
        pageIndex,
        encData: {
          offset: encOffset,
          length: encLength,
          iv
        }
      };
      
      await this.#executeIDBTransaction('readwrite', store => store.put(entry));
      
      // Store in memory too
      this.#pages.set(pageIndex, entry);
    } catch (e) {
      console.error(`Failed to save page to IndexedDB: ${e.message}`);
    }
  }
  
  /**
   * Delete a page from IndexedDB
   * @param {number} pageIndex - The page index to delete
   */
  async #deletePageMeta(pageIndex) {
    try {
      await this.#executeIDBTransaction('readwrite', store => store.delete(pageIndex));
      
      // Remove from memory too
      this.#pages.delete(pageIndex);
    } catch (e) {
      console.error(`Failed to delete page from IndexedDB: ${e.message}`);
    }
  }
  
  /**
   * Clear all pages from IndexedDB
   */
  async #clearPageMetas() {
    try {
      await this.#executeIDBTransaction('readwrite', store => store.clear());
      
      // Clear memory too
      this.#pages.clear();
    } catch (e) {
      console.error(`Failed to clear pages from IndexedDB: ${e.message}`);
    }
  }

  /**
   * Check if a pathname matches our tracked DB file
   * @param {string} pathname - The pathname to check
   * @returns {boolean} - Whether this is our tracked DB file
   */
  #isTrackedDbFile(pathname) {
    // Check if the pathname matches our OPFS filename
    return pathname === `/${this.#opfsFilename}`;
  }

  /**
   * Calculate the OPFS page index for a given offset
   * @param {number} offset - Byte offset into the file
   * @returns {number} - OPFS page index
   */
  #getOpfsPageIndex(offset) {
    return Math.floor(offset / this.#opfsPageSize);
  }

  /**
   * Get the start offset of an OPFS page
   * @param {number} pageIndex - OPFS page index
   * @returns {number} - Byte offset of the start of the page
   */
  #getOpfsPageStart(pageIndex) {
    return pageIndex * this.#opfsPageSize;
  }

  /**
   * Get the end offset of an OPFS page (exclusive)
   * @param {number} pageIndex - OPFS page index
   * @returns {number} - Byte offset of the end of the page
   */
  #getOpfsPageEnd(pageIndex) {
    return (pageIndex + 1) * this.#opfsPageSize;
  }

  /**
   * Get the range of OPFS pages affected by a write operation
   * @param {number} offset - Start offset of the write
   * @param {number} length - Length of the write
   * @returns {{startPageIndex: number, endPageIndex: number}} - Range of page indices (inclusive)
   */
  #getAffectedOpfsPageRange(offset, length) {
    if (length === 0) return { startPageIndex: 0, endPageIndex: -1 }; // Empty range
    
    const startPageIndex = this.#getOpfsPageIndex(offset);
    const endPageIndex = this.#getOpfsPageIndex(offset + length - 1);
    return { startPageIndex, endPageIndex };
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
   * Process all pending operations in the queue using page-aligned writes
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
    console.log(`Processing ${this.#writeQueue.length} write operations`);

    try {
      // Find the in-memory database file object from the mapNameToFile
      const memSqliteFile = this.mapNameToFile.get(`/${this.#opfsFilename}`);
      if (!memSqliteFile) {
        console.error(
          `Cannot find file /${this.#opfsFilename} in mapNameToFile`
        );
        this.#isProcessingWrites = false;
        return;
      }

      // Create a writable stream to the OPFS file
      const writable = await this.#dbFileHandle.createWritable({ keepExistingData: true });
      let writeFileLength = (await this.#dbFileHandle.getFile()).size;

      const operations = this.#writeQueue.splice(0);
      
      // Track dirty pages that need to be written
      /** @type {Set<number>} */ let dirtyPages = new Set();
      
      // Process all operations in order
      for (const operation of operations) {
        if (operation.type === "write") {
          // Collect the affected pages for this write
          const { offset, length } = operation;
          const { startPageIndex, endPageIndex } = this.#getAffectedOpfsPageRange(offset, length);
          
          // Mark each affected page as dirty
          for (let pageIndex = startPageIndex; pageIndex <= endPageIndex; pageIndex++) {
            dirtyPages.add(pageIndex);
          }
        } 
        else if (operation.type === "truncate") {
          const size = operation.size;
          const truncatePageIndex = this.#getOpfsPageIndex(size);
          
          // Filter out completely truncated pages, keeping only pages within the new file size
          Array.from(dirtyPages).forEach((pageIndex) => {
              // Remove any pages that are completely truncated
              if (pageIndex > truncatePageIndex) {
                dirtyPages.delete(pageIndex);
              }
          })
          
          this.#processTruncateOperation(size, writable);
        } 
        else if (operation.type === "delete") {
          // Discard all dirty pages - no need to write before deletion
          dirtyPages.clear();
          this.#processDeleteOperation(writable);
          
          // Operations after a delete would be for a new file, so exit the loop
          break;
        }
      }
      
      // Write any remaining dirty pages
      if (dirtyPages.size > 0 && this.#dbFileHandle) {
        for (const pageIndex of dirtyPages.keys()) {
          writeFileLength = await this.#processOpfsPage(pageIndex, memSqliteFile, writable, writeFileLength);
        }
      }
      
      // Close writable if we still have one
      if (this.#dbFileHandle) {
        await writable.close();
      }
    } catch (e) {
      console.error(`Failed to process operation queue: ${e.message}`);
    } finally {
      this.#isProcessingWrites = false;
      const end = performance.now();
      console.log(`Processed writes in ${(end - start).toFixed(2)} ms`);

      // If more operations were added while processing, start again
      if (this.#writeQueue.length > 0) {
        setTimeout(() => this.#processWriteQueue(), 0);
      }
    }
  }
  
  /**
   * Process a single OPFS page
   * @param {number} pageIndex - The OPFS page index
   * @param {Object} memSqliteFile - The in-memory SQLite file
   * @param {FileSystemWritableFileStream} writable - The OPFS writable stream
   * @param {number} writeFileLength - Current length of the OPFS file
   * @returns {Promise<number>} - New length of the OPFS file
   */
  async #processOpfsPage(pageIndex, memSqliteFile, writable, writeFileLength) {
    // Calculate the page boundaries in the SQLite file
    const pageStart = this.#getOpfsPageStart(pageIndex);
    const pageEnd = Math.min(this.#getOpfsPageEnd(pageIndex), memSqliteFile.size);
    const readDataSize = pageEnd - pageStart;
    
    // Extract the page data from memory
    const plainData = new Uint8Array(memSqliteFile.data, pageStart, readDataSize);
    
    if (this.#encryptionKey) {
      // Encrypt the page
      const { encryptedData, iv } = await this.#encryptData(plainData);
      
      // Determine where to write the encrypted page
      let encOffset;
      const existingPage = this.#pages.get(pageIndex);
      
      if (existingPage) {
        // Reuse the existing location if we've written this page before
        encOffset = existingPage.encData.offset;
      } else {
        // Append to the end of the file
        encOffset = writeFileLength;
        writeFileLength += encryptedData.byteLength;
      }
      
      // Write the encrypted page to OPFS
      await writable.seek(encOffset);
      await writable.write(encryptedData);
      
      // Save the page metadata
      await this.#savePageMeta(pageIndex, encOffset, encryptedData.byteLength, iv);
      
      return writeFileLength;
    } else {
      // For unencrypted storage, write directly at the page-aligned offset
      await writable.seek(pageStart);
      await writable.write(plainData);
      
      return Math.max(writeFileLength, pageStart + readDataSize);
    }
  }
  
  /**
   * Process a truncate operation (used for direct truncate calls only)
   * @param {number} size - The new size of the unencrypted SQLite file, in bytes
   * @param {FileSystemWritableFileStream} writable - The OPFS writable stream
   */
  async #processTruncateOperation(size, writable) {
    if (this.#encryptionKey) {
      // For encrypted storage, delete pages after truncation point
      const truncatePageIndex = this.#getOpfsPageIndex(size);
      const pagesToDelete = Array.from(this.#pages.keys()).filter(
        pageIdx => pageIdx > truncatePageIndex
      );
      
      for (const pageIdx of pagesToDelete) {
        await this.#deletePageMeta(pageIdx);
      }

      // as the pages may be written to OPFS out-of-order, we cannot truncate the file
      // space can be reclaimed by VACUUM

    } else {
      // For unencrypted storage, just truncate the file
      await writable.truncate(size);
    }
  }
  
  /**
   * Process a delete operation (used for direct delete calls only)
   * @param {FileSystemWritableFileStream} writable - The OPFS writable stream
   */
  async #processDeleteOperation(writable) {
    try {
      // Delete the file from OPFS
      await this.#rootDir.removeEntry(this.#opfsFilename);
      this.#dbFileHandle = null;
      
      // Clear all page metadata
      if (this.#encryptionKey) {
        await this.#clearPageMetas();
      }
      
      console.log(`Deleted OPFS file`);
    } catch (e) {
      console.error(`Failed to delete OPFS file: ${e.message}`);
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
   * @returns {number}
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
   * @returns {number}
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
   * @returns {number}
   */
  jRead(fileId, pData, iOffset) {
    // console.log(`MemoryDelayedOPFSVFS.jRead(${fileId}, ${pData.byteLength}, ${iOffset})`);
    const file = this.mapIdToFile.get(fileId);

    // if (this.#isTrackedDbFile(file.pathname)) {
    //   console.log(`MemoryDelayedOPFSVFS.jRead(${iOffset}, ${pData.byteLength})`);
    // }

    // Clip the requested read to the file boundary.
    const bgn = Math.min(iOffset, file.size);
    const end = Math.min(iOffset + pData.byteLength, file.size);
    const nBytes = end - bgn;

    if (nBytes) {
      // Read data from memory
      const data = new Uint8Array(file.data, bgn, nBytes);
      
      // Data in memory is already decrypted (we decrypt on load and encrypt on write)
      // so we can just copy it directly
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

    // Copy data to in-memory storage (unencrypted for easy access)
    new Uint8Array(file.data, iOffset, pData.byteLength).set(pData);
    file.size = Math.max(file.size, iOffset + pData.byteLength);

    // If this is our database file, queue only the changed page for writing to OPFS
    if (this.#isTrackedDbFile(file.pathname)) {
      // console.log(`MemoryDelayedOPFSVFS.jWrite(${iOffset}, ${pData.byteLength})`);
      // Queue just the offset and length - the actual data is already in memory
      this.#queueWrite(iOffset, pData.byteLength);
    }

    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} fileId
   * @param {number} iSize
   * @returns {number}
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
   * @returns {number}
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
   * @returns {number}
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
   * @returns {number}
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
