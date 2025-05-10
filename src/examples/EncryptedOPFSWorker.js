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
  #HEADER_VERSION = "YNABDB01"; // 8-byte version/magic identifier
  
  // Append-only storage structures
  /** @type {Map<number, number>} */ #pageIndex = new Map(); // Maps logical page index to physical offset
  /** @type {Set<number>} */ #freeOffsets = new Set(); // Set of offsets that can be reused
  #nextAppendOffset = this.#HEADER_SIZE; // First offset after the header
  
  // No need for a write queue anymore as operations are passed directly
  
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
   * Process operations to persist changes to OPFS.
   * Uses an append-only approach to ensure atomicity.
   * @param {Array<PendingOperation>} operations - Array of operations to process
   */
  async processWriteQueue(operations) {
    // If nothing to process, exit early
    if (!operations || operations.length === 0) {
      return;
    }

    const start = performance.now();
    let writtenPageCount = 0;

    try {
      // Ensure we have an access handle before processing
      if (!this.#accessHandle) {
        throw new Error("No access handle available for OPFS file");
      }

      // Track dirty pages that need to be written
      /** @type {Set<number>} */ let dirtyPages = new Set();
      /** @type {Map<number, number>} */ let updatedPageIndex = new Map(this.#pageIndex);
      for (const operation of operations) {
        if (operation.type === 'write') {
          // For write operations, determine the affected pages
          const { startPageIndex, endPageIndex } = this.#getAffectedPageRange(operation.offset, operation.size);

          for (let pageIndex = startPageIndex; pageIndex <= endPageIndex; pageIndex++) {
            dirtyPages.add(pageIndex);
          }
        } else if (operation.type === 'truncate') {
          const size = operation.size;
          const truncatePageIndex = this.#getPageIndex(size);

          // Remove pages beyond the truncation point from the index
          for (const [pageIdx, _] of updatedPageIndex) {
            if (pageIdx > truncatePageIndex) {
              updatedPageIndex.delete(pageIdx);
              dirtyPages.delete(pageIdx);
            }
          }
        } else if (operation.type === 'delete') {
          // Clear everything
          updatedPageIndex.clear();
          dirtyPages.clear();
          // Create a whole new file
          await this.#processDeleteOperation();
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

        // Write the updated page index as the last step
        await this.#writePageIndex(updatedPageIndex);
        // Update our in-memory page index to match what we wrote
        this.#pageIndex = updatedPageIndex;
      }

      // Sync changes to disk
      this.#accessHandle.flush();

    } catch (e) {
      console.error(`EncryptedOPFSWorker | Failed to process operation queue: ${e.message}`);
      // In this case, we don't retry old operations - we'll get a new state in the next message
    } finally {
      const end = performance.now();
      console.log(`EncryptedOPFSWorker | Wrote ${writtenPageCount} pages in ${(end - start).toFixed(1)} ms`);
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

    // Prepare the header buffer with new format
    const headerBuffer = new Uint8Array(this.#HEADER_SIZE);
    headerBuffer.fill(0); // Initialize to zeros

    // Calculate the positions based on sizes
    const versionBytes = encoder.encode(this.#HEADER_VERSION);      // 8 bytes
    const ivSize = this.#IV_SIZE;                                   // 12 bytes
    const lengthSize = 4;                                           // 4 bytes (uint32)

    let position = 0;

    // 1. Write header version
    headerBuffer.set(versionBytes, position);
    position += versionBytes.length;

    // 2. Write IV
    headerBuffer.set(iv, position);
    position += ivSize;

    // 3. Write encrypted data length (4 bytes, little-endian uint32)
    const dataLength = encryptedData.byteLength;
    const dataView = new DataView(headerBuffer.buffer, position, lengthSize);
    dataView.setUint32(0, dataLength, true); // true = little-endian
    position += lengthSize;

    // 4. Write encrypted data
    headerBuffer.set(encryptedData, position);

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
      const versionSize = this.#HEADER_VERSION.length; // 8 bytes
      const prefixSize = versionSize + this.#IV_SIZE + 4; // 4 bytes for length field

      // If the file is too small to have a minimal header, return an empty index
      if (fileSize < prefixSize) {
        console.log(`EncryptedOPFSWorker | File too small for header: ${fileSize} bytes`);
        return new Map();
      }

      // First read just the header prefix to check version and get data length
      const headerPrefix = new Uint8Array(prefixSize);
      this.#accessHandle.read(headerPrefix, { at: 0 });
      let position = 0;

      // 1. Check header version
      const decoder = new TextDecoder();
      const version = decoder.decode(headerPrefix.slice(0, versionSize));
      position += versionSize;

      if (version !== this.#HEADER_VERSION) {
        console.warn(`EncryptedOPFSWorker | Invalid header version: ${version}`);
        return new Map();
      }

      // 2. Extract the IV
      const iv = headerPrefix.slice(position, position + this.#IV_SIZE);
      position += this.#IV_SIZE;

      // 3. Get the encrypted data length
      const dataView = new DataView(headerPrefix.buffer, position, 4);
      const dataLength = dataView.getUint32(0, true); // true = little-endian

      // Validate the data length to prevent reading garbage
      if (dataLength <= 0 || dataLength > (this.#HEADER_SIZE - prefixSize)) {
        console.warn(`EncryptedOPFSWorker | Invalid encrypted data length: ${dataLength}`);
        return new Map();
      }

      // 4. Read the encrypted data at the position right after the prefix
      const encryptedData = new Uint8Array(dataLength);
      this.#accessHandle.read(encryptedData, { at: prefixSize });

      // Decrypt and parse the header data
      const decryptedData = await this.#decryptData(encryptedData, iv);
      const jsonStr = decoder.decode(decryptedData);
      const indexData = JSON.parse(jsonStr);

      // Update our next append offset and free offsets
      this.#nextAppendOffset = indexData.nextAppendOffset || this.#HEADER_SIZE;
      this.#freeOffsets = new Set(indexData.freeOffsets || []);

      // Convert the page index entries to a Map
      const pageIndex = new Map(indexData.entries);
      return pageIndex;
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
