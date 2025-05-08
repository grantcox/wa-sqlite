/**
 * EncryptedPermutedOPFSWorker.js
 * 
 * Web Worker implementation for handling persistence operations for SyncMemoryProxyAsyncWorkerVFS.
 * This worker handles IndexedDB, OPFS operations, and encryption/decryption.
 */

import { BaseWriteWorker } from './BaseWriteWorker.js';

/**
 * @typedef {Object} PageMeta
 * @property {number} pageIndex
 * @property { {offset: number, length: number, iv: Uint8Array} } encData
 */

/**
 * Worker implementation that persists data to OPFS with mandatory encryption
 * Extends the BaseWriteWorker to handle ordered operations with OPFS-specific persistence
 */
class EncryptedPermutedOPFSWorker extends BaseWriteWorker {
  // OPFS state
  #rootDir = null;
  #dbFileHandle = null;
  
  // IndexedDB state
  #idb = null;
  
  // Using encryption key from base class via getEncryptionKey()
  
  // Configuration
  #dbName = "db.sqlite";
  #opfsPageSize = 65536;
  
  // Page tracking
  /** @type {Map<number, PageMeta>} */
  #pages = new Map();
  
  // Write queue for OPFS operations
  #writeQueue = [];
  #processingWritesActive = false;
  
  /**
   * Initialize the worker with configuration
   * @param {VFSConfig} config Worker configuration
   * @returns {Promise<ArrayBuffer>} Initial file data
   */
  async init(config) {
    try {
      this.#dbName = config.dbName || "db.sqlite";
      if (!this.getEncryptionKey()) {
        throw new Error("Encryption key not initialized in base class");
      }
      
      // Set page size if provided
      if (config.pageSize) {
        this.#opfsPageSize = config.pageSize;
      }
      
      // Initialize IndexedDB first, then OPFS
      await this.#initIndexedDb(this.#dbName);
      const fileData = await this.#initOpfs(this.#dbName);
      this.setFileData(fileData);
      
      console.log(`EncryptedPermutedOPFSWorker | Initialized with ${fileData.byteLength} bytes of data`);
    } catch (e) {
      console.error('EncryptedPermutedOPFSWorker | Initialization failed:', e);
      throw e;
    }
  }

  /**
   * Process a write operation, overriding base class
   * @param {number} offset Byte offset to write
   * @param {Uint8Array} data Data to write
   */
  async processWrite(offset, data) {
    // Call the base implementation to update in-memory data
    await super.processWrite(offset, data);
    
    // Queue page writes for OPFS
    const { startPageIndex, endPageIndex } = this.#getAffectedOpfsPageRange(offset, data.byteLength);
    
    // Queue these pages for writing to OPFS
    for (let pageIndex = startPageIndex; pageIndex <= endPageIndex; pageIndex++) {
      this.#writeQueue.push({
        type: "page",
        pageIndex
      });
    }
  }

  /**
   * Process a truncate operation, overriding base class
   * @param {number} size New file size
   */
  async processTruncate(size) {
    // Call the base implementation to update in-memory data
    await super.processTruncate(size);
    
    // Queue the truncate operation
    this.#writeQueue.push({
      type: "truncate",
      size
    });
  }

  /**
   * Process a delete operation, overriding base class
   */
  async processDelete() {
    // Call the base implementation to update in-memory data
    await super.processDelete();
    
    // Queue the delete operation
    this.#writeQueue.push({
      type: "delete"
    });
  }

  /**
   * Process the write queue to persist changes to OPFS
   * Required implementation from BaseWriteWorker
   */
  async processWriteQueue() {
    if (this.#processingWritesActive || this.#writeQueue.length === 0 || !this.#dbFileHandle) {
      return;
    }

    this.#processingWritesActive = true;
    const start = performance.now();
    let writtenPageCount = 0;

    try {
      // Create a writable stream to the OPFS file
      const writable = await this.#dbFileHandle.createWritable({
        keepExistingData: true,
      });
      let writeFileLength = (await this.#dbFileHandle.getFile()).size;

      const operations = this.#writeQueue.splice(0);

      // Track dirty pages that need to be written
      /** @type {Set<number>} */ 
      let dirtyPages = new Set();

      // Process all operations in order
      for (const operation of operations) {
        if (operation.type === "page") {
          dirtyPages.add(operation.pageIndex);
        } else if (operation.type === "truncate") {
          const size = operation.size;
          const truncatePageIndex = this.#getOpfsPageIndex(size);

          // Filter out completely truncated pages, keeping only pages within the new file size
          Array.from(dirtyPages).forEach((pageIndex) => {
            // Remove any pages that are completely truncated
            if (pageIndex > truncatePageIndex) {
              dirtyPages.delete(pageIndex);
            }
          });

          await this.#processTruncateOperation(size, writable);
        } else if (operation.type === "delete") {
          // Discard all dirty pages - no need to write before deletion
          dirtyPages.clear();
          await this.#processDeleteOperation(writable);

          // Operations after a delete would be for a new file, so exit the loop
          break;
        }
      }

      // Write any remaining dirty pages
      if (dirtyPages.size > 0 && this.#dbFileHandle) {
        for (const pageIndex of dirtyPages.keys()) {
          writeFileLength = await this.#processOpfsPage(pageIndex, writable, writeFileLength);
          writtenPageCount++;
        }
      }

      // Close writable if we still have one
      if (this.#dbFileHandle) {
        await writable.close();
      }
    } catch (e) {
      console.error(`EncryptedPermutedOPFSWorker | Failed to process operation queue: ${e.message}`);
    } finally {
      this.#processingWritesActive = false;
      const end = performance.now();
      console.log(`EncryptedPermutedOPFSWorker | Wrote ${writtenPageCount} pages in ${(end - start).toFixed(2)} ms`);

      // If more operations were added while processing, start again
      if (this.#writeQueue.length > 0) {
        setTimeout(() => this.processWriteQueue(), 0);
      }
    }
  }

  // --------------------------------------------------------------------------
  // Private methods for encryption
  // --------------------------------------------------------------------------


  /**
   * Encrypt data using AES-GCM with a random IV
   * @param {Uint8Array} data - Data to encrypt
   * @returns {Promise<{encryptedData: Uint8Array, iv: Uint8Array}>}
   */
  async #encryptData(data) {
    // Generate a random IV
    const iv = crypto.getRandomValues(new Uint8Array(12)); // 12 bytes is recommended for AES-GCM

    // Encrypt the data using key from base class
    const encryptedBuffer = await crypto.subtle.encrypt(
      {
        name: "AES-GCM",
        iv,
      },
      this.getEncryptionKey(),
      data
    );
    const encryptedData = new Uint8Array(encryptedBuffer);

    // Return both the encrypted data and the IV
    return {
      encryptedData: encryptedData,
      iv,
    };
  }

  /**
   * Decrypt data using AES-GCM
   * @param {Uint8Array} encryptedData - Encrypted data
   * @param {Uint8Array} iv - Initialization vector used for encryption
   * @returns {Promise<Uint8Array>}
   */
  async #decryptData(encryptedData, iv) {
    // Decrypt the data using key from base class
    const decryptedBuffer = await crypto.subtle.decrypt(
      {
        name: "AES-GCM",
        iv,
      },
      this.getEncryptionKey(),
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
        console.warn(`EncryptedPermutedOPFSWorker | Missing encryption data for page ${pageIndex}`);
        continue;
      }

      try {
        // Extract the encrypted data
        const encryptedChunk = new Uint8Array(encryptedData, encOffset, encLength);

        // Decrypt the data
        const decryptedChunk = await this.#decryptData(encryptedChunk, iv);

        // Copy the decrypted data to the output buffer
        decryptedView.set(new Uint8Array(decryptedChunk.buffer, 0, decryptedChunk.byteLength), pageStart);
      } catch (e) {
        console.error(`EncryptedPermutedOPFSWorker | Error decrypting page ${pageIndex}`, e);
      }
    }

    console.log(`EncryptedPermutedOPFSWorker | Finished decrypting file, produced ${maxPlainEnd} bytes of plaintext`);
    return decryptedBuffer;
  }

  // --------------------------------------------------------------------------
  // Private methods for IndexedDB
  // --------------------------------------------------------------------------

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

  /**
   * Initialize IndexedDB for page storage
   * @param {string} dbNameParam - Database name
   * @returns {Promise<boolean>}
   */
  async #initIndexedDb(dbNameParam) {
    try {
      // Initialize IndexedDB for page storage
      this.#idb = await new Promise((resolve, reject) => {
        const indexedDbName = `SyncMemoryProxyAsyncWorkerVFS-${dbNameParam}-enc`;
        const request = indexedDB.open(indexedDbName, 1);
        request.onupgradeneeded = (event) => {
          const db = request.result;
          db.createObjectStore("pages", { keyPath: "pageIndex" });
        };
        request.onsuccess = () => {
          resolve(request.result);
        };
        request.onerror = () => {
          reject(request.error);
        };
      });

      // Load the pages from IndexedDB
      await this.#loadPageMetas();
      return true;
    } catch (e) {
      console.error(`EncryptedPermutedOPFSWorker | Failed to initialize IndexedDB:`, e);
      return false;
    }
  }

  /**
   * Load pages from IndexedDB
   */
  async #loadPageMetas() {
    try {
      const result = await this.#executeIDBTransaction("readonly", (store) => store.getAll());

      // Populate the pages map
      for (const entry of result) {
        this.#pages.set(entry.pageIndex, entry);
      }
    } catch (e) {
      console.error(`EncryptedPermutedOPFSWorker | Failed to load pages from IndexedDB: ${e.message}`);
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
    // Create the page entry
    const entry = {
      pageIndex,
      encData: {
        offset: encOffset,
        length: encLength,
        iv,
      },
    };

    try {
      await this.#executeIDBTransaction("readwrite", (store) => store.put(entry));
      this.#pages.set(pageIndex, entry);
    } catch (e) {
      console.error(`EncryptedPermutedOPFSWorker | Failed to save page to IndexedDB: ${e.message}`, entry);
    }
  }

  /**
   * Delete a page from IndexedDB
   * @param {number} pageIndex - The page index to delete
   */
  async #deletePageMeta(pageIndex) {
    try {
      await this.#executeIDBTransaction("readwrite", (store) => store.delete(pageIndex));

      // Remove from memory too
      this.#pages.delete(pageIndex);
    } catch (e) {
      console.error(`EncryptedPermutedOPFSWorker | Failed to delete page ${pageIndex} from IndexedDB: ${e.message}`);
    }
  }

  /**
   * Clear all pages from IndexedDB
   */
  async #clearPageMetas() {
    try {
      await this.#executeIDBTransaction("readwrite", (store) => store.clear());

      // Clear memory too
      this.#pages.clear();
    } catch (e) {
      console.error(`EncryptedPermutedOPFSWorker | Failed to clear pages from IndexedDB: ${e.message}`);
    }
  }

  // --------------------------------------------------------------------------
  // Private methods for OPFS
  // --------------------------------------------------------------------------

  /**
   * Initialize OPFS for file access
   * @param {string} dbNameParam - Database name
   * @returns {Promise<ArrayBuffer>} - The database file content
   */
  async #initOpfs(dbNameParam) {
    try {
      // Get a handle to the file in OPFS (creating if necessary)
      this.#rootDir = await navigator.storage.getDirectory();
      const filenameInOpfs = `${dbNameParam}.enc`;
      this.#dbFileHandle = await this.#rootDir.getFileHandle(filenameInOpfs, {
        create: true,
      });

      // Load existing data from OPFS
      return await this.#readFileFromOPFS();
    } catch (e) {
      console.error(`EncryptedPermutedOPFSWorker | Failed to initialize OPFS`, e.message, e.stack, e.className);
      return new ArrayBuffer(0);
    }
  }

  /**
   * Read the database file from OPFS
   * @returns {Promise<ArrayBuffer>} - The file content
   */
  async #readFileFromOPFS() {
    // Load existing data from OPFS
    const file = await this.#dbFileHandle.getFile();
    let encryptedData = await file.arrayBuffer();

    // Decrypt the file if we have data (encryption is always enabled)
    if (encryptedData.byteLength > 0) {
      try {
        return await this.#decryptFile(encryptedData);
      } catch (e) {
        if (e instanceof DOMException && e.name === "OperationError") {
          console.error("EncryptedPermutedOPFSWorker | Incorrect encryption key or corrupted data");
          return new ArrayBuffer(0);
        } else {
          throw e;
        }
      }
    } else {
      // Empty file, just return empty buffer
      return encryptedData;
    }
  }

  /**
   * Process a single OPFS page
   * @param {number} pageIndex - The OPFS page index
   * @param {FileSystemWritableFileStream} writable - The OPFS writable stream
   * @param {number} writeFileLength - Current length of the OPFS file
   * @returns {Promise<number>} - New length of the OPFS file
   */
  async #processOpfsPage(pageIndex, writable, writeFileLength) {
    const fileData = this.getFileData();
    if (!fileData) {
      console.error(`EncryptedPermutedOPFSWorker | Cannot process page ${pageIndex} without file data`);
      return writeFileLength;
    }
    
    // Calculate the page boundaries in the SQLite file
    const pageStart = this.#getOpfsPageStart(pageIndex);
    const plainData = new Uint8Array(this.#opfsPageSize);

    // Calculate how much actual data we can copy from the source
    const availableData = Math.max(
      0,
      Math.min(
        this.#opfsPageSize, // Don't exceed page size
        fileData.byteLength - pageStart // Don't read past end of file
      )
    );

    if (availableData > 0) {
      const sourceData = new Uint8Array(fileData, pageStart, availableData);
      plainData.set(sourceData, 0);
    }

    // Encrypt the page (encryption is always enabled)
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
  }

  /**
   * Process a truncate operation
   * @param {number} size - The new size of the unencrypted SQLite file, in bytes
   * @param {FileSystemWritableFileStream} writable - The OPFS writable stream
   */
  async #processTruncateOperation(size, writable) {
    // For encrypted storage, delete pages after truncation point
    const truncatePageIndex = this.#getOpfsPageIndex(size);
    const pagesToDelete = Array.from(this.#pages.keys()).filter((pageIdx) => pageIdx > truncatePageIndex);

    for (const pageIdx of pagesToDelete) {
      await this.#deletePageMeta(pageIdx);
    }

    // As the pages may be written to OPFS out-of-order, we cannot truncate the file
    // Space can be reclaimed by VACUUM
  }

  /**
   * Process a delete operation
   * @param {FileSystemWritableFileStream} writable - The OPFS writable stream
   */
  async #processDeleteOperation(writable) {
    try {
      // Delete the file from OPFS
      await this.#rootDir.removeEntry(this.#dbName);
      this.#dbFileHandle = null;

      // Clear all page metadata
      if (this.getEncryptionKey()) {
        await this.#clearPageMetas();
      }

      console.log(`EncryptedPermutedOPFSWorker | Deleted OPFS file`);
    } catch (e) {
      console.error(`EncryptedPermutedOPFSWorker | Failed to delete OPFS file: ${e.message}`);
    }
  }

  // --------------------------------------------------------------------------
  // Utility methods for page calculations
  // --------------------------------------------------------------------------

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
}

// Create and export an instance of the worker
const worker = new EncryptedPermutedOPFSWorker();
