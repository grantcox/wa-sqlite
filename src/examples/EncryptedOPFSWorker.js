/**
 * EncryptedOPFSWorker.js
 * 
 * Web Worker implementation for handling persistence operations for SyncMemoryProxyAsyncWorkerVFS.
 * This worker handles OPFS operations with mandatory encryption, using an append-only storage model
 * with a page index header to ensure atomicity even when using the sync access handle APIs.
 */

import { BaseWriteWorker } from './BaseWriteWorker.js';

/**
 * Worker implementation that persists data to OPFS with mandatory encryption
 * Uses an append-only strategy for page writes to ensure atomicity.
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
  
  // Fixed constants
  #IV_SIZE = 12;
  #AUTH_TAG_SIZE = 16; // GCM authentication tag size
  #opfsPageSize = this.#sourcePageSize + this.#IV_SIZE + this.#AUTH_TAG_SIZE;
  // the header needs to be large enough to hold all the page lookups and free page indices
  // 128KB is roughly enough for a 1GB SQLite file
  #HEADER_SIZE = 131072;
  
  // Append-only storage structures
  /** @type {Map<number, number>} */ #pageIndex = new Map(); // Maps logical page index to physical offset
  /** @type {Set<number>} */ #freeOffsets = new Set(); // Set of offsets that can be reused
  #nextAppendOffset = this.#HEADER_SIZE; // First offset after the header
  
  // Write queue for OPFS operations
  #writeQueue = [];
  #activeWrites = []; // Operations currently being processed
  
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
      const fileData = await this.#initOpfs();
      
      // Set the initial file data in base class
      this.setFileData(fileData);
      
      console.log(`EncryptedOPFSWorker | Loaded initial database file, ${fileData.byteLength} bytes`);
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
   * Uses an append-only approach to ensure atomicity.
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
      /** @type {Map<number, number>} */ let updatedPageIndex = new Map(this.#pageIndex);

      // Process all operations in order
      for (const operation of this.#activeWrites) {       
        if (operation.type === "page") {
          dirtyPages.add(operation.pageIndex);

        } else if (operation.type === "truncate") {
          const size = operation.size;
          const truncatePageIndex = this.#getPageIndex(size);

          // Remove pages beyond the truncation point from the index
          for (const [pageIdx, _] of updatedPageIndex) {
            if (pageIdx > truncatePageIndex) {
              updatedPageIndex.delete(pageIdx);
              dirtyPages.delete(pageIdx);
            }
          }

        } else if (operation.type === "delete") {
          // Clear everything
          dirtyPages.clear();
          updatedPageIndex.clear();
          
          await this.#processDeleteOperation();
          // Need to recreate access handle after delete
          // Operations after a delete would be for a new file, so we'll break the loop after this
          break;
        }
      }

      // Write all dirty pages and update the page index
      if (dirtyPages.size > 0) {
        const writePages = Array.from(dirtyPages)
        const offsets = await this.#writePages(writePages);
        
        // Update the page index with new locations
        for (let i = 0; i < offsets.length; i++) {
          const pageIndex = writePages[i];
          const offset = offsets[i];
          updatedPageIndex.set(pageIndex, offset);
          writtenPageCount++;
        }
      }

      // Write the updated page index as the last step
      if (dirtyPages.size > 0) {
        await this.#writePageIndex(updatedPageIndex);
        // Update our in-memory page index to match what we wrote
        this.#pageIndex = updatedPageIndex;
      }

      // All operations processed successfully
      this.#activeWrites = [];
      
      // Sync changes to disk
      console.log(`EncryptedOPFSWorker | Flushing changes to disk...`);
      this.#accessHandle.flush();

    } catch (e) {
      console.error(`EncryptedOPFSWorker | Failed to process operation queue: ${e.message}`);
      // Note: We don't clear activeWrites here so they can be retried on next call
    } finally {
      const end = performance.now();
      console.log(`EncryptedOPFSWorker | Wrote ${writtenPageCount} pages in ${(end - start).toFixed(2)} ms`);

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
  // Private methods for page index management
  // --------------------------------------------------------------------------

  /**
   * Write the page index to the header of the file
   * @param {Map<number, number>} pageIndex - The page index mapping to write
   */
  async #writePageIndex(pageIndex) {
    // Convert the page index to a serializable format
    const entries = Array.from(pageIndex.entries());
    const indexData = {
      nextAppendOffset: this.#nextAppendOffset,
      freeOffsets: Array.from(this.#freeOffsets),
      entries: entries,
    };
    
    // Serialize to JSON and convert to a buffer
    const serialized = JSON.stringify(indexData);
    const encoder = new TextEncoder();
    const rawData = encoder.encode(serialized);
    
    // Encrypt the header data
    const { encryptedData, iv } = await this.#encryptData(rawData);
    
    // Prepare the header buffer: IV + encrypted data
    const headerBuffer = new Uint8Array(this.#HEADER_SIZE);
    headerBuffer.fill(0); // Initialize to zeros
    headerBuffer.set(iv, 0); // IV at the beginning
    headerBuffer.set(encryptedData, this.#IV_SIZE); // Encrypted data right after the IV
    
    // Write the header to the start of the file
    const bytesWritten = this.#accessHandle.write(headerBuffer, { at: 0 });
    
    if (bytesWritten !== headerBuffer.length) {
      throw new Error(`Failed to write entire header. Wrote ${bytesWritten} of ${headerBuffer.length} bytes`);
    }
  }

  /**
   * Read and parse the page index from the file header
   * @returns {Promise<Map<number, number>>} The page index mapping
   */
  async #readPageIndex() {
    try {
      // Get the file size using the access handle
      const fileSize = this.#accessHandle.getSize();
      
      // If the file is too small to have a header, return an empty index
      if (fileSize < this.#HEADER_SIZE) {
        return new Map();
      }
      
      // Read the header
      const headerBuffer = new Uint8Array(this.#HEADER_SIZE);
      const bytesRead = this.#accessHandle.read(headerBuffer, { at: 0 });
      
      if (bytesRead !== this.#HEADER_SIZE) {
        console.warn(`EncryptedOPFSWorker | Read only ${bytesRead} bytes from header`);
        return new Map();
      }
      
      // Extract the IV from the beginning of the header
      const iv = headerBuffer.slice(0, this.#IV_SIZE);
      
      // Extract the encrypted data
      const encryptedData = headerBuffer.slice(this.#IV_SIZE);
      
      // Decrypt the header data
      const decryptedData = await this.#decryptData(encryptedData, iv);
      
      // Parse the decrypted data
      const decoder = new TextDecoder();
      const jsonStr = decoder.decode(decryptedData);
      const indexData = JSON.parse(jsonStr);
            
      // Update our next append offset and free offsets
      this.#nextAppendOffset = indexData.nextAppendOffset || this.#HEADER_SIZE;
      this.#freeOffsets = new Set(indexData.freeOffsets || []);
      
      // Convert the entries to a Map
      return new Map(indexData.entries);
      
    } catch (e) {
      console.warn(`EncryptedOPFSWorker | Failed to read page index: ${e.message}`);
      return new Map();
    }
  }

  // --------------------------------------------------------------------------
  // Private methods for append-only page operations
  // --------------------------------------------------------------------------
  
  /**
   * Write multiple pages to the file, using the append-only strategy
   * @param {number[]} pageIndices - Array of page indices to write
   * @returns {Promise<number[]>} - Array of physical offsets where each page was written
   */
  async #writePages(pageIndices) {
    const fileData = this.getFileData();
    if (!fileData) {
      throw new Error("Cannot write pages without file data");
    }
    
    const writtenOffsets = [];
    
    for (const pageIndex of pageIndices) {
      // Get the next available offset, either from free list or by appending
      const offset = this.#getNextWriteOffset();
      
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
      
      // Create a combined buffer with IV + encrypted data
      const combinedData = new Uint8Array(this.#IV_SIZE + encryptedData.byteLength);
      combinedData.set(iv, 0);
      combinedData.set(encryptedData, this.#IV_SIZE);
      
      // Write the data to the file using the sync access handle at the chosen offset
      const bytesWritten = this.#accessHandle.write(combinedData, { at: offset });
      
      if (bytesWritten !== combinedData.byteLength) {
        throw new Error(`Failed to write entire page. Wrote ${bytesWritten} of ${combinedData.byteLength} bytes`);
      }
      
      // Record where this page was written
      writtenOffsets.push(offset);
      
      // Update the next append offset if we used it
      if (offset === this.#nextAppendOffset) {
        this.#nextAppendOffset += this.#opfsPageSize;
      }
    }
    
    return writtenOffsets;
  }
  
  /**
   * Get the next available offset for writing a page
   * @returns {number} - The offset to write at
   */
  #getNextWriteOffset() {
    // Check if we have any free offsets to reuse
    if (this.#freeOffsets.size > 0) {
      // Get the first free offset
      const offset = this.#freeOffsets.values().next().value;
      // Remove it from the free list
      this.#freeOffsets.delete(offset);
      return offset;
    }
    
    // Otherwise, append to the end
    return this.#nextAppendOffset;
  }

  // --------------------------------------------------------------------------
  // Private methods for OPFS
  // --------------------------------------------------------------------------

  /**
   * Initialize OPFS for file access
   * @returns {Promise<ArrayBuffer>} - The database file content
   */
  async #initOpfs() {
    try {
      // Get a handle to the file in OPFS (creating if necessary)
      this.#rootDir = await navigator.storage.getDirectory();
      
      // Get file handle
      const fileHandle = await this.#rootDir.getFileHandle(this.#fileName, {
        create: true,
      });
      
      // Create a synchronous access handle for the file
      this.#accessHandle = await fileHandle.createSyncAccessHandle();

      // Read the page index from the file
      this.#pageIndex = await this.#readPageIndex();
      
      // Load existing data from OPFS
      return await this.#readFileFromOPFS();
    } catch (e) {
      console.error(`EncryptedOPFSWorker | Failed to initialize OPFS`, e.message, e.stack);
      return new ArrayBuffer(0);
    }
  }

  /**
   * Read the database file from OPFS using the page index
   * @returns {Promise<ArrayBuffer>} - The file content
   */
  async #readFileFromOPFS() {
    try {
      if (!this.#accessHandle) {
        return new ArrayBuffer(0);
      }
      
      // If no pages in the index, return empty buffer
      if (this.#pageIndex.size === 0) {
        console.log(`EncryptedOPFSWorker | Empty database file, returning empty buffer`);
        return new ArrayBuffer(0);
      }
      
      // Find the highest logical page index
      const highestPageIndex = Math.max(...this.#pageIndex.keys());
      
      // Calculate the size of the decrypted buffer
      const decryptedSize = (highestPageIndex + 1) * this.#sourcePageSize;
      const decryptedBuffer = new ArrayBuffer(decryptedSize);
      const decryptedView = new Uint8Array(decryptedBuffer);
      
      // Read and decrypt each page
      for (const [logicalPageIdx, physicalOffset] of this.#pageIndex.entries()) {
        try {
          // Read the encrypted page from its physical location
          const encryptedPage = new Uint8Array(this.#opfsPageSize);
          const bytesRead = this.#accessHandle.read(encryptedPage, { at: physicalOffset });
          
          if (bytesRead !== this.#opfsPageSize) {
            console.warn(`EncryptedOPFSWorker | Read less data than expected - only ${bytesRead} bytes from page ${logicalPageIdx} at offset ${physicalOffset}`);
            continue;
          }
          
          // Extract the IV from the beginning of the page
          const iv = encryptedPage.slice(0, this.#IV_SIZE);
          
          // Extract the encrypted data
          const encryptedData = encryptedPage.slice(this.#IV_SIZE);
          
          // Decrypt the page
          const decryptedPage = await this.#decryptData(encryptedData, iv);
          
          // Copy the decrypted data to the correct logical position
          const destOffset = logicalPageIdx * this.#sourcePageSize;
          decryptedView.set(decryptedPage, destOffset);
        } catch (e) {
          // If decryption fails, leave this page as zeros
          console.error(`EncryptedOPFSWorker | Error reading/decrypting page ${logicalPageIdx}:`, e);
        }
      }
      
      console.log(`EncryptedOPFSWorker | Finished reading file with ${this.#pageIndex.size} pages, produced ${decryptedBuffer.byteLength} bytes of plaintext`);
      return decryptedBuffer;
    } catch (e) {
      console.error("EncryptedOPFSWorker | Error reading file from OPFS:", e);
      if (e instanceof DOMException && e.name === "OperationError") {
        console.error("EncryptedOPFSWorker | Incorrect encryption key or corrupted data");
      }
      return new ArrayBuffer(0);
    }
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
      
      // Reset our page index and other tracking structures
      this.#pageIndex = new Map();
      this.#freeOffsets = new Set();
      this.#nextAppendOffset = this.#HEADER_SIZE;
      
      // Write an empty page index
      await this.#writePageIndex(this.#pageIndex);

      console.log(`EncryptedOPFSWorker | Deleted OPFS file and initialized new one`);
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
