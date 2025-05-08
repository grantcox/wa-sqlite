/**
 * EncryptedIndexedDbWorker.js
 * 
 * Web Worker implementation for handling persistence operations for SyncMemoryProxyAsyncWorkerVFS.
 * This worker handles all persistence using IndexedDB with optional encryption.
 */

import { BaseWriteWorker } from './BaseWriteWorker.js';

/**
 * @typedef {Object} PageMeta
 * @property {number} pageIndex
 * @property {Uint8Array} encryptedData - The encrypted page data
 * @property {Uint8Array} iv - The initialization vector
 */

/**
 * Worker implementation that persists data to IndexedDB with mandatory encryption
 * Extends the BaseWriteWorker to handle ordered operations with IndexedDB-specific persistence
 */
class EncryptedIndexedDbWorker extends BaseWriteWorker {
  // IndexedDB state
  #idb = null;
  
  // Using encryption key from base class via getEncryptionKey()
  
  // Configuration
  #dbName = "db.sqlite";
  #pageSize = 65536;
  
  // Page tracking
  /** @type {Map<number, PageMeta>} */
  #pages = new Map();
  
  // Write queue for batch operations
  #writeQueue = [];
  #processingWritesActive = false;
  
  constructor() {
    super();
  }
  
  /**
   * Initialize the worker with configuration
   * @param {VFSConfig} config Worker configuration
   * @returns {Promise<ArrayBuffer>} Initial file data
   */
  async init(config) {
    try {
      this.#dbName = config.dbName || "db.sqlite";
      if (config.pageSize) {
        this.#pageSize = config.pageSize;
      }

      if (!this.getEncryptionKey()) {
        throw new Error("Encryption key not initialized in base class");
      }
      
      // Initialize IndexedDB and load data
      await this.#initIndexedDb(this.#dbName);
      const fileData = await this.#loadFileData();
      this.setFileData(fileData);
      
      console.log(`EncryptedIndexedDbWorker | Initialized with ${fileData.byteLength} bytes of data`);
    } catch (e) {
      console.error('EncryptedIndexedDbWorker | Initialization failed:', e);
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
    
    // Queue page writes for IndexedDB
    const { startPageIndex, endPageIndex } = this.#getAffectedPageRange(offset, data.byteLength);
    
    // Queue these pages for writing to IndexedDB
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
   * Process the write queue to persist changes to IndexedDB
   * Required implementation from BaseWriteWorker
   */
  async processWriteQueue() {
    if (this.#processingWritesActive || this.#writeQueue.length === 0 || !this.#idb) {
      return;
    }

    this.#processingWritesActive = true;
    const start = performance.now();
    console.log(`EncryptedIndexedDbWorker | Processing ${this.#writeQueue.length} write operations`);
    let writtenPageCount = 0;

    try {
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
          const truncatePageIndex = this.#getPageIndex(size);

          // Filter out completely truncated pages, keeping only pages within the new file size
          Array.from(dirtyPages).forEach((pageIndex) => {
            // Remove any pages that are completely truncated
            if (pageIndex > truncatePageIndex) {
              dirtyPages.delete(pageIndex);
            }
          });

          await this.#processTruncateOperation(size);
        } else if (operation.type === "delete") {
          // Discard all dirty pages - no need to write before deletion
          dirtyPages.clear();
          await this.#processDeleteOperation();

          // Operations after a delete would be for a new file, so exit the loop
          break;
        }
      }

      // Write any remaining dirty pages
      if (dirtyPages.size > 0 && this.#idb) {
        const writePromises = [];
        for (const pageIndex of dirtyPages.keys()) {
          writePromises.push(this.#processPage(pageIndex));
          writtenPageCount++;
        }
        // Wait for all page writes to complete
        await Promise.all(writePromises);
      }
    } catch (e) {
      console.error(`EncryptedIndexedDbWorker | Failed to process operation queue: ${e.message}`);
    } finally {
      this.#processingWritesActive = false;
      const end = performance.now();
      console.log(`EncryptedIndexedDbWorker | Wrote ${writtenPageCount} pages in ${(end - start).toFixed(2)} ms`);

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
      encryptedData,
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

  // --------------------------------------------------------------------------
  // Private methods for IndexedDB
  // --------------------------------------------------------------------------

  /**
   * Execute an IndexedDB transaction and return the result
   * @param {string} storeName - The store to operate on ('pages' or 'data')
   * @param {IDBTransactionMode} mode - Transaction mode ('readonly' or 'readwrite')
   * @param {function(IDBObjectStore): IDBRequest} operation - Function that performs the operation on the store
   * @returns {Promise<any>} - Result of the operation
   */
  async #executeIDBTransaction(storeName, mode, operation) {
    if (!this.#idb) {
      throw new Error("IndexedDB not initialized");
    }

    return new Promise((resolve, reject) => {
      const tx = this.#idb.transaction(storeName, mode);
      const store = tx.objectStore(storeName);

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
        const indexedDbName = `EncryptedIndexedDbWorker-${dbNameParam}-enc`;
        const request = indexedDB.open(indexedDbName, 1);
        
        request.onupgradeneeded = (event) => {
          const db = request.result;
          // Store for page metadata
          db.createObjectStore("pages", { keyPath: "pageIndex" });
          // Store for encrypted page data
          db.createObjectStore("data", { keyPath: "pageIndex" });
        };
        
        request.onsuccess = () => {
          resolve(request.result);
        };
        
        request.onerror = () => {
          reject(request.error);
        };
      });

      // Load the pages from IndexedDB
      await this.#loadPages();
      return true;
    } catch (e) {
      console.error(`EncryptedIndexedDbWorker | Failed to initialize IndexedDB:`, e);
      return false;
    }
  }

  /**
   * Load all pages from IndexedDB
   */
  async #loadPages() {
    try {
      // First load all page metadata
      const pages = await this.#executeIDBTransaction("pages", "readonly", (store) => store.getAll());
      
      // Populate the pages map
      for (const page of pages) {
        this.#pages.set(page.pageIndex, page);
      }
      
      console.log(`EncryptedIndexedDbWorker | Loaded ${pages.length} page metadata entries`);
    } catch (e) {
      console.error(`EncryptedIndexedDbWorker | Failed to load pages from IndexedDB: ${e.message}`);
    }
  }

  /**
   * Load the file data from IndexedDB
   * @returns {Promise<ArrayBuffer>} The complete file data
   */
  async #loadFileData() {
    if (this.#pages.size === 0) {
      return new ArrayBuffer(0);
    }

    try {
      // Determine the size of the file
      let maxPlainEnd = 0;
      for (const [pageIndex, _] of this.#pages.entries()) {
        const pageEnd = this.#getPageEnd(pageIndex);
        if (pageEnd > maxPlainEnd) {
          maxPlainEnd = pageEnd;
        }
      }

      // Create a buffer for the file
      const fileBuffer = new ArrayBuffer(maxPlainEnd);
      const fileView = new Uint8Array(fileBuffer);

      // Sort page indices for sequential processing
      const pageIndices = Array.from(this.#pages.keys()).sort((a, b) => a - b);

      // Load and decrypt each page
      for (const pageIndex of pageIndices) {
        const pageData = await this.#executeIDBTransaction("data", "readonly", 
          (store) => store.get(pageIndex));
        
        if (!pageData) {
          console.warn(`EncryptedIndexedDbWorker | Missing data for page ${pageIndex}`);
          continue;
        }

        const pageMeta = this.#pages.get(pageIndex);
        const pageStart = this.#getPageStart(pageIndex);

        try {
          // Decrypt the page (encryption is always enabled)
          const plainData = await this.#decryptData(pageData.encryptedData, pageMeta.iv);

          // Copy the page data to the file buffer
          fileView.set(plainData, pageStart);
        } catch (e) {
          console.error(`EncryptedIndexedDbWorker | Error processing page ${pageIndex}: ${e.message}`);
        }
      }

      console.log(`EncryptedIndexedDbWorker | Loaded file data (${maxPlainEnd} bytes)`);
      return fileBuffer;
    } catch (e) {
      console.error(`EncryptedIndexedDbWorker | Failed to load file data: ${e.message}`);
      return new ArrayBuffer(0);
    }
  }

  /**
   * Process and save a single page to IndexedDB
   * @param {number} pageIndex The page index to process
   */
  async #processPage(pageIndex) {
    const fileData = this.getFileData();
    if (!fileData) {
      console.error(`EncryptedIndexedDbWorker | Cannot process page ${pageIndex} without file data`);
      return;
    }
    
    // Calculate the page boundaries in the file
    const pageStart = this.#getPageStart(pageIndex);
    const plainData = new Uint8Array(this.#pageSize);

    // Calculate how much actual data we can copy from the source
    const availableData = Math.max(
      0,
      Math.min(
        this.#pageSize, // Don't exceed page size
        fileData.byteLength - pageStart // Don't read past end of file
      )
    );

    if (availableData > 0) {
      const sourceData = new Uint8Array(fileData, pageStart, availableData);
      plainData.set(sourceData, 0);
    }

    // Encrypt the page (encryption is always enabled)
    const encResult = await this.#encryptData(plainData);
    const dataToStore = encResult.encryptedData;
    const iv = encResult.iv;

    // Save page metadata
    const pageMeta = {
      pageIndex,
      iv
    };
    
    // Save the encrypted data
    const pageData = {
      pageIndex,
      encryptedData: dataToStore
    };

    // Store both the metadata and data
    await Promise.all([
      this.#executeIDBTransaction("pages", "readwrite", (store) => store.put(pageMeta)),
      this.#executeIDBTransaction("data", "readwrite", (store) => store.put(pageData))
    ]);
    
    // Update in-memory page tracking
    this.#pages.set(pageIndex, pageMeta);
  }

  /**
   * Delete a page from IndexedDB
   * @param {number} pageIndex The page index to delete
   */
  async #deletePage(pageIndex) {
    try {
      // Delete both metadata and data
      await Promise.all([
        this.#executeIDBTransaction("pages", "readwrite", (store) => store.delete(pageIndex)),
        this.#executeIDBTransaction("data", "readwrite", (store) => store.delete(pageIndex))
      ]);
      
      // Remove from memory too
      this.#pages.delete(pageIndex);
    } catch (e) {
      console.error(`EncryptedIndexedDbWorker | Failed to delete page ${pageIndex}: ${e.message}`);
    }
  }

  /**
   * Clear all pages from IndexedDB
   */
  async #clearAllPages() {
    try {
      // Clear both stores
      await Promise.all([
        this.#executeIDBTransaction("pages", "readwrite", (store) => store.clear()),
        this.#executeIDBTransaction("data", "readwrite", (store) => store.clear())
      ]);
      
      // Clear memory too
      this.#pages.clear();
    } catch (e) {
      console.error(`EncryptedIndexedDbWorker | Failed to clear pages: ${e.message}`);
    }
  }

  /**
   * Process a truncate operation
   * @param {number} size - The new size of the file, in bytes
   */
  async #processTruncateOperation(size) {
    // Delete pages after truncation point
    const truncatePageIndex = this.#getPageIndex(size);
    const pagesToDelete = Array.from(this.#pages.keys()).filter((pageIdx) => pageIdx > truncatePageIndex);

    const deletePromises = [];
    for (const pageIdx of pagesToDelete) {
      deletePromises.push(this.#deletePage(pageIdx));
    }
    
    // Wait for all deletes to complete
    await Promise.all(deletePromises);
  }

  /**
   * Process a delete operation
   */
  async #processDeleteOperation() {
    // Clear all pages
    await this.#clearAllPages();
  }

  // --------------------------------------------------------------------------
  // Utility methods for page calculations
  // --------------------------------------------------------------------------

  /**
   * Calculate the page index for a given offset
   * @param {number} offset - Byte offset into the file
   * @returns {number} - Page index
   */
  #getPageIndex(offset) {
    return Math.floor(offset / this.#pageSize);
  }

  /**
   * Get the start offset of a page
   * @param {number} pageIndex - Page index
   * @returns {number} - Byte offset of the start of the page
   */
  #getPageStart(pageIndex) {
    return pageIndex * this.#pageSize;
  }

  /**
   * Get the end offset of a page (exclusive)
   * @param {number} pageIndex - Page index
   * @returns {number} - Byte offset of the end of the page
   */
  #getPageEnd(pageIndex) {
    return (pageIndex + 1) * this.#pageSize;
  }

  /**
   * Get the range of pages affected by a write operation
   * @param {number} offset - Start offset of the write
   * @param {number} length - Length of the write
   * @returns {{startPageIndex: number, endPageIndex: number}} - Range of page indices (inclusive)
   */
  #getAffectedPageRange(offset, length) {
    if (length === 0) return { startPageIndex: 0, endPageIndex: -1 }; // Empty range

    const startPageIndex = this.#getPageIndex(offset);
    const endPageIndex = this.#getPageIndex(offset + length - 1);
    return { startPageIndex, endPageIndex };
  }
}

// Create and export an instance of the worker
const worker = new EncryptedIndexedDbWorker();
