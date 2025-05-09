/**
 * EncryptedOPFSWorker.js
 * 
 * Web Worker implementation for handling persistence operations for SyncMemoryProxyAsyncWorkerVFS.
 * This worker handles OPFS operations with mandatory encryption, storing all data
 * directly in the OPFS file at predictable offsets using a synchronous access handle.
 */

import { BaseWriteWorker } from './BaseWriteWorker.js';

/**
 * Worker implementation that persists data to OPFS with mandatory encryption
 * Each page is stored at a deterministic location based on its index
 */
class EncryptedOPFSWorker extends BaseWriteWorker {
  // OPFS state
  #rootDir = null;
  #fileName = null;
  
  // Synchronous access handle for OPFS file
  #accessHandle = null;
  
  // Configuration
  #dbName = "db.sqlite";
  #sourcePageSize = 65536; // Size of unencrypted pages
  
  // Fixed page size constants
  #IV_SIZE = 12;
  #AUTH_TAG_SIZE = 16; // GCM authentication tag size
  #opfsPageSize = this.#sourcePageSize + this.#IV_SIZE + this.#AUTH_TAG_SIZE;
  
  // Write queue for OPFS operations
  #writeQueue = [];
  #activeWrites = []; // Operations currently being processed
  totalWriteTime = 0;
  
  /**
   * Initialize the worker with configuration
   * @param {Object} config Worker configuration
   * @returns {Promise<ArrayBuffer>} Initial file data
   */
  async init(config) {
    try {
      this.#dbName = config.dbName || "db.sqlite";
      this.#fileName = `${this.#dbName}.enc`;
      
      // Encryption is required, verify key was created in base class
      if (!this.getEncryptionKey()) {
        throw new Error("Encryption key not initialized in base class");
      }
      
      // Set page size if provided (but maintain the fixed format)
      if (config.pageSize) {
        this.#sourcePageSize = config.pageSize;
        this.#opfsPageSize = this.#sourcePageSize + this.#IV_SIZE + this.#AUTH_TAG_SIZE;
      }
      
      // Initialize OPFS
      const fileData = await this.#initOpfs(this.#dbName);
      
      // Set the initial file data in base class
      this.setFileData(fileData);
      
      console.log(`EncryptedOPFSWorker | Initialized with ${fileData.byteLength} bytes of data`);
      return fileData;
    } catch (e) {
      console.error('EncryptedOPFSWorker | Initialization failed:', e);
      throw e;
    }
  }

  /**
   * Process a write operation, overriding base class
   * @param {number} offset Start byte offset that was written
   * @param {number} size Length of write
   */
  async processWrite(offset, size) {
    // Queue page writes for OPFS
    const { startPageIndex, endPageIndex } = this.#getAffectedPageRange(offset, size);
    
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
   * Process the entire write queue to persist changes to OPFS.
   * To ensure our file is in a consistent state, we should process the entire queue,
   * or none of it. The BaseWriteWorker restricts how many changes are in the queue,
   * so it shouldn't get too large.
   */
  async processWriteQueue() {
    // If nothing to process, exit early
    if (this.#activeWrites.length === 0 && this.#writeQueue.length === 0) {
      return;
    }

    const start = performance.now();
    let writtenPageCount = 0;

    try {
      // Ensure we have an access handle before processing
      if (!this.#accessHandle) {
        throw new Error("No access handle available for OPFS file");
      }

      // If we have pending active writes from a previous interrupted operation, process them first
      const priorWriteCount = this.#activeWrites.length;
      const newWrites = this.#writeQueue.splice(0);
      this.#activeWrites = this.#activeWrites.concat(newWrites);
      console.log(`EncryptedOPFSWorker | Processing ${(priorWriteCount + newWrites.length)} write operations${(priorWriteCount > 0) ? ` (${priorWriteCount} from prior failed attempt)` : ""}`);

      // Track dirty pages that need to be written
      /** @type {Set<number>} */ let dirtyPages = new Set();

      // Process all operations in order
      for (const operation of this.#activeWrites) {       
        if (operation.type === "page") {
          dirtyPages.add(operation.pageIndex);

        } else if (operation.type === "truncate") {
          const size = operation.size;
          const truncatePageIndex = this.#getPageIndex(size);

          // Filter out completely truncated pages
          Array.from(dirtyPages).forEach((pageIndex) => {
            if (pageIndex > truncatePageIndex) {
              dirtyPages.delete(pageIndex);
            }
          });

          await this.#processTruncateOperation(size);
        } else if (operation.type === "delete") {
          dirtyPages.clear();
          
          await this.#processDeleteOperation();
          // Need to recreate access handle after delete
          // Operations after a delete would be for a new file, so we'll break the loop after this
          break;
        }
      }

      // Write all dirty pages
      if (dirtyPages.size > 0) {
        for (const pageIndex of dirtyPages) {
          await this.#processPage(pageIndex);
          writtenPageCount++;
        }
      }

      // All operations processed successfully
      this.#activeWrites = [];
      
      // Sync changes to disk
      this.#accessHandle.flush();

    } catch (e) {
      console.error(`EncryptedOPFSWorker | Failed to process operation queue: ${e.message}`);
      // Note: We don't clear activeWrites here so they can be retried on next call
    } finally {
      const end = performance.now();
      const duration = end - start;
      this.totalWriteTime += duration;
      console.log(`EncryptedOPFSWorker | Wrote ${writtenPageCount} pages in ${(end - start).toFixed(1)} ms (${this.totalWriteTime.toFixed(1)} ms total)`);

      // If there are more operations in the queue, continue processing
      if (this.#writeQueue.length > 0 || this.#activeWrites.length > 0) {
        await this.processWriteQueue();
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
    const iv = crypto.getRandomValues(new Uint8Array(this.#IV_SIZE));

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
    return new Uint8Array(decryptedBuffer);
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
      
      // Get file handle
      const fileHandle = await this.#rootDir.getFileHandle(this.#fileName, {
        create: true,
      });
      
      // Create a synchronous access handle for the file
      this.#accessHandle = await fileHandle.createSyncAccessHandle();

      // Load existing data from OPFS
      return await this.#readFileFromOPFS();
    } catch (e) {
      console.error(`EncryptedOPFSWorker | Failed to initialize OPFS`, e.message, e.stack);
      return new ArrayBuffer(0);
    }
  }

  /**
   * Read the database file from OPFS
   * @returns {Promise<ArrayBuffer>} - The file content
   */
  async #readFileFromOPFS() {
    try {
      if (!this.#accessHandle) {
        return new ArrayBuffer(0);
      }
      
      // Get file size using the access handle
      const fileSize = this.#accessHandle.getSize();
      
      // If the file is empty, return empty buffer
      if (fileSize === 0) {
        console.log(`EncryptedOPFSWorker | Empty database file, returning empty buffer`);
        return new ArrayBuffer(0);
      }
      
      // Read file data and decrypt it
      const buffer = new ArrayBuffer(fileSize);
      const dataView = new Uint8Array(buffer);
      
      // Use the sync read method to read the entire file
      const bytesRead = this.#accessHandle.read(dataView, { at: 0 });
      
      if (bytesRead !== fileSize) {
        console.warn(`EncryptedOPFSWorker | Read only ${bytesRead} bytes from file of size ${fileSize}`);
      }
      
      return await this.#decryptFile(buffer);
    } catch (e) {
      console.error("EncryptedOPFSWorker | Error reading file from OPFS:", e);
      if (e instanceof DOMException && e.name === "OperationError") {
        console.error("EncryptedOPFSWorker | Incorrect encryption key or corrupted data");
      }
      return new ArrayBuffer(0);
    }
  }

  /**
   * Decrypt the entire file
   * @param {ArrayBuffer} fileData - The encrypted file data from OPFS
   * @returns {Promise<ArrayBuffer>} - The decrypted file data
   */
  async #decryptFile(fileData) {
    if (fileData.byteLength === 0) {
      return new ArrayBuffer(0);
    }

    // Calculate the number of pages in the file
    const pageCount = Math.floor(fileData.byteLength / this.#opfsPageSize);
    
    // Find the highest page to determine the final output size
    let highestPageIndex = -1;
    
    // Scan file to find the highest valid page
    for (let i = pageCount - 1; i >= 0; i--) {
      // We'll try to decrypt each page to determine if it exists
      try {
        const offset = i * this.#opfsPageSize;
        
        // Skip pages beyond the file end
        if (offset + this.#IV_SIZE + 1 >= fileData.byteLength) {
          continue;
        }
        
        // Read the IV at the start of the page
        const iv = new Uint8Array(fileData, offset, this.#IV_SIZE);
        
        // Read the encrypted data (including authentication tag)
        const encryptedData = new Uint8Array(
          fileData, 
          offset + this.#IV_SIZE, 
          Math.min(this.#sourcePageSize + this.#AUTH_TAG_SIZE, fileData.byteLength - (offset + this.#IV_SIZE))
        );
        
        // Try to decrypt - if successful, this is a valid page
        await this.#decryptData(encryptedData, iv);
        
        // If we're here, decryption succeeded, so this is a valid page
        highestPageIndex = i;
        break;
      } catch (e) {
        // Decryption failed - this isn't a valid page or is corrupted
        continue;
      }
    }
    
    // If no valid pages found, return empty buffer
    if (highestPageIndex === -1) {
      return new ArrayBuffer(0);
    }
    
    // Calculate output size based on highest page found
    const outputSize = (highestPageIndex + 1) * this.#sourcePageSize;
    const decryptedBuffer = new ArrayBuffer(outputSize);
    const decryptedView = new Uint8Array(decryptedBuffer);
    
    // Decrypt each page
    for (let pageIndex = 0; pageIndex <= highestPageIndex; pageIndex++) {
      const offset = pageIndex * this.#opfsPageSize;
      
      // Skip pages beyond the file end
      if (offset + this.#IV_SIZE >= fileData.byteLength) {
        continue;
      }
      
      try {
        // Read the IV at the start of the page
        const iv = new Uint8Array(fileData, offset, this.#IV_SIZE);
        
        // Read the encrypted data (including authentication tag)
        const encryptedData = new Uint8Array(
          fileData, 
          offset + this.#IV_SIZE, 
          Math.min(this.#sourcePageSize + this.#AUTH_TAG_SIZE, fileData.byteLength - (offset + this.#IV_SIZE))
        );
        
        // Decrypt the data
        const decryptedChunk = await this.#decryptData(encryptedData, iv);
        
        // Copy the decrypted data to the output buffer
        const destOffset = pageIndex * this.#sourcePageSize;
        decryptedView.set(decryptedChunk, destOffset);
      } catch (e) {
        // If decryption fails, leave this page as zeros
        console.error(`EncryptedOPFSWorker | Error decrypting page ${pageIndex}:`, e);
      }
    }

    console.log(`EncryptedOPFSWorker | Finished decrypting file, produced ${decryptedBuffer.byteLength} bytes of plaintext`);
    return decryptedBuffer;
  }

  /**
   * Process a single page
   * @param {number} pageIndex - The page index
   */
  async #processPage(pageIndex) {
    const fileData = this.getFileData();
    if (!fileData) {
      console.error(`EncryptedOPFSWorker | Cannot process page ${pageIndex} without file data`);
      return;
    }
    
    // Calculate the page boundaries in the SQLite file
    const pageStart = this.#getPageStart(pageIndex);
    const plainData = new Uint8Array(this.#sourcePageSize);

    // Calculate how much actual data we can copy from the source
    const availableData = Math.max(
      0,
      Math.min(
        this.#sourcePageSize,
        fileData.byteLength - pageStart
      )
    );

    if (availableData > 0) {
      const sourceData = new Uint8Array(fileData, pageStart, availableData);
      plainData.set(sourceData, 0);
    }

    // Encrypt the page
    const { encryptedData, iv } = await this.#encryptData(plainData);

    // Calculate exact file position for this page
    const pageOffset = pageIndex * this.#opfsPageSize;
    
    // Create a combined buffer with IV + encrypted data
    const combinedData = new Uint8Array(this.#IV_SIZE + encryptedData.byteLength);
    combinedData.set(iv, 0);
    combinedData.set(encryptedData, this.#IV_SIZE);

    // Write the data to the file using the sync access handle
    const bytesWritten = this.#accessHandle.write(combinedData, { at: pageOffset });
    
    if (bytesWritten !== combinedData.byteLength) {
      throw new Error(`Failed to write entire page. Wrote ${bytesWritten} of ${combinedData.byteLength} bytes`);
    }
  }

  /**
   * Process a truncate operation
   * @param {number} size - The new size of the unencrypted SQLite file, in bytes
   */
  async #processTruncateOperation(size) {
    // Calculate the page index for the truncation point
    const truncatePageIndex = this.#getPageIndex(size);
    
    // Calculate the file size after truncation (including the partial last page)
    const finalSize = (truncatePageIndex + 1) * this.#opfsPageSize;
    
    // Truncate the file using the sync access handle
    this.#accessHandle.truncate(finalSize);
    
    console.log(`EncryptedOPFSWorker | Truncated file to ${finalSize} bytes (${truncatePageIndex + 1} pages)`);
  }

  /**
   * Process a delete operation
   */
  async #processDeleteOperation() {
    try {
      // Close the current access handle
      this.#accessHandle.close();
      this.#accessHandle = null;
      
      // Delete the file from OPFS
      await this.#rootDir.removeEntry(this.#fileName);
      
      // Recreate an empty database file
      const fileHandle = await this.#rootDir.getFileHandle(this.#fileName, {
        create: true,
      });
      
      // Create a new synchronous access handle for the file
      this.#accessHandle = await fileHandle.createSyncAccessHandle();

      console.log(`EncryptedOPFSWorker | Deleted OPFS file`);
    } catch (e) {
      console.error(`EncryptedOPFSWorker | Failed to delete OPFS file: ${e.message}`);
    }
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
    return Math.floor(offset / this.#sourcePageSize);
  }

  /**
   * Get the start offset of a page
   * @param {number} pageIndex - Page index
   * @returns {number} - Byte offset of the start of the page
   */
  #getPageStart(pageIndex) {
    return pageIndex * this.#sourcePageSize;
  }

  /**
   * Get the end offset of a page (exclusive)
   * @param {number} pageIndex - Page index
   * @returns {number} - Byte offset of the end of the page
   */
  #getPageEnd(pageIndex) {
    return (pageIndex + 1) * this.#sourcePageSize;
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
const worker = new EncryptedOPFSWorker();
