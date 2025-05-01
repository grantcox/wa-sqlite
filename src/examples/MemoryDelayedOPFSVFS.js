import { FacadeVFS } from "../FacadeVFS.js";
import * as VFS from "../VFS.js";

// Memory-based VFS with asynchronous OPFS persistence.
export class MemoryDelayedOPFSVFS extends FacadeVFS {
  // Map of existing files, keyed by filename.
  mapNameToFile = new Map();

  // Map of open files, keyed by id (sqlite3_file pointer).
  mapIdToFile = new Map();

  // OPFS root directory handle
  /** @type {FileSystemDirectoryHandle} */ #rootDir = null;

  // OPFS file handle for the database
  /** @type {FileSystemFileHandle} */ #dbFileHandle = null;

  // Track if OPFS is ready
  /** @type {Promise<boolean>} */ #vfsReady = null;

  // Database filename in OPFS
  #opfsFilename = "db.sqlite";

  // Queue for pending writes to OPFS - stores regions to be written
  #writeQueue = [];

  // Flag to track if write processing is active
  #isProcessingWrites = false;

  // Buffer for loaded OPFS data (not immediately added to mapNameToFile)
  /** @type {ArrayBuffer} */ #opfsDataBuffer = null;
  
  // Encryption key
  /** @type {CryptoKey} */ #encryptionKey = null;
  
  // Pages storage (maps plaintext offsets to encryption/plaintext metadata)
  /** @type {Map<number, {plainOffset: number, plainLength: number, encOffset: number, encLength: number, iv: Uint8Array}>} */ #pages = new Map();
  
  // IDB database for page storage
  /** @type {IDBDatabase} */ #idb = null;

  /**
   * @param {string} name 
   * @param {*} module
   * @param {{key?: CryptoKey, dbName?: string}} options - Optional encryption key
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
   * @param {{key?: CryptoKey, dbName?: string}} options
   * @returns 
   */
  constructor(name, module, options) {
    super(name, module);
    this.#opfsFilename = options.dbName ?? "db.sqlite";
    this.#encryptionKey = options.key;

    // Initialize IndexedDB first (so we have the IVs), then OPFS (where we decrypt the existing db)
    // console.log("MemoryDelayedOPFSVFS constructor about to call initIndexedDb")
    this.#vfsReady = this.#initIndexedDb()
      .then(() => {
        return this.#initOpfs()
      })
      // console.log("MemoryDelayedOPFSVFS constructor complete")
  }

  async isReady() {
    // console.log("MemoryDelayedOPFSVFS isReady, waiting for vfsReady")
    return this.#vfsReady;
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

    // console.log(`Decrypting ${encryptedData.byteLength} bytes from OPFS, with ${this.#pages.size} pages`);
    
    // Calculate the size needed for the decrypted buffer
    // We need to find the maximum plainOffset + plainLength
    let maxPlainEnd = 0;
    for (const [plainOffset, entry] of this.#pages.entries()) {
      const plainEnd = plainOffset + entry.plainLength;
      if (plainEnd > maxPlainEnd) {
        maxPlainEnd = plainEnd;
      }
    }
    
    // Create a new buffer for the decrypted data with the correct size
    const decryptedBuffer = new ArrayBuffer(maxPlainEnd);
    const decryptedView = new Uint8Array(decryptedBuffer);
    
    // Decrypt each page and write it to the right location in the buffer
    for (const [plainOffset, entry] of this.#pages.entries()) {
      // console.log(`Decrypting page entry`, plainOffset, entry);
      const { iv, encOffset, encLength, plainLength } = entry;
      
      // Extract the encrypted data at the specified offset
      const encryptedChunk = new Uint8Array(encryptedData, encOffset, encLength);
      // console.log(`Encrypted chunk at ${encOffset}, length ${encLength}`, encryptedChunk);
      
      // Decrypt the data chunk
      const decryptedChunk = await this.#decryptData(encryptedChunk, iv);
      // console.log(`Decrypted chunk for plainOffset ${plainOffset}, length ${plainLength}`, decryptedChunk);
      
      // Write the decrypted data to the appropriate position in the buffer
      decryptedView.set(decryptedChunk, plainOffset);
      // console.log(`Wrote decrypted chunk to plainOffset ${plainOffset}`);
    }
    
    // console.log(`Finished decrypting file, produced ${maxPlainEnd} bytes of plaintext`);
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
  
  async #initIndexedDb() {
      // Initialize IndexedDB for page storage
      // console.log(`initIndexedDb 001`)
      this.#idb = await new Promise((resolve, reject) => {
        // console.log(`initIndexedDb about to open()`)
        const request = indexedDB.open(`MemoryDelayedOPFSVFS-${this.#opfsFilename}`, 1);
        // console.log(`initIndexedDb called open()`)
        request.onupgradeneeded = () => {
          // console.log(`initIndexedDb onupgradeneeded`)
          const db = request.result;
          db.createObjectStore('pages', { keyPath: 'plainOffset' });
        };
        request.onsuccess = () => {
          // console.log(`initIndexedDb onsuccess`)
          resolve(request.result);
        }
        request.onerror = () => {
          // console.log(`initIndexedDb onerror`)
          reject(request.error);
        }
      });
      // console.log(`initIndexedDb 002`)
      
      // Load the pages from IndexedDB
      await this.#loadPages();
      return true;
  }
  
  /**
   * Load pages from IndexedDB
   */
  async #loadPages() {
    try {
      const tx = this.#idb.transaction('pages', 'readonly');
      const store = tx.objectStore('pages');
      const request = store.getAll();
      
      const result = await new Promise((resolve, reject) => {
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      });
      
      // Populate the pages map
      for (const entry of result) {
        this.#pages.set(entry.plainOffset, entry);
      }
      
    } catch (e) {
      console.error(`Failed to load pages from IndexedDB: ${e.message}`);
    }
  }
  
  /**
   * Save page data to IndexedDB
   * @param {Uint8Array} iv - The initialization vector
   * @param {number} plainOffset - The plaintext offset
   * @param {number} plainLength - The plaintext length
   * @param {number} encOffset - The encrypted offset in OPFS file
   * @param {number} encLength - The encrypted length
   */
  async #savePage(iv, plainOffset, plainLength, encOffset, encLength) {
    try {
      const tx = this.#idb.transaction('pages', 'readwrite');
      const store = tx.objectStore('pages');
      const entry = { iv, plainOffset, plainLength, encOffset, encLength };

      await new Promise((resolve, reject) => {
        const request = store.put(entry);
        request.onsuccess = resolve;
        request.onerror = () => reject(request.error);
      });
      
      // Store in memory too
      this.#pages.set(plainOffset, entry);
    } catch (e) {
      console.error(`Failed to save page to IndexedDB: ${e.message}`);
    }
  }


  #isTrackedDbFile(pathname) {
    // Check if the pathname matches our OPFS filename
    return pathname === `/${this.#opfsFilename}`;
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
   * Process all pending operations in the queue in order without coalescing
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
    let processed = 0;
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

      // Create a writable stream - we'll use a single writable for all operations
      // to avoid the overhead of opening and closing the file multiple times
      // console.log(`Opening file for processing ${this.#writeQueue.length} operations`);
      const writable = await this.#dbFileHandle.createWritable({
        keepExistingData: true,
      });
      let writeFileLength = (await this.#dbFileHandle.getFile()).size;

      // Process each operation in order
      let continueProcessing = true;

      while (this.#writeQueue.length > 0 && continueProcessing) {
        processed++;
        const operation = this.#writeQueue.shift();

        if (operation.type === "write") {
          let { offset, length } = operation;

          // Get the data from the in-memory file
          const plainData = new Uint8Array(memSqliteFile.data, offset, length);

          if (this.#encryptionKey) {
            const { encryptedData, iv } = await this.#encryptData(plainData);

            // store the encryption metadata in the pages map
            const existingPage = this.#pages.get(offset);
            let encOffset;
            if (existingPage) {
              // Re-use existing encrypted offset if we already have one for this offset
              encOffset = existingPage.encOffset;
            } else {
              // Append to the end of the file
              encOffset = writeFileLength;
              writeFileLength += encryptedData.byteLength;
            }
            
            // Save the page metadata for later decryption
            await this.#savePage(iv, offset, length, encOffset, encryptedData.byteLength);
            
            // Seek to the position and write the encrypted data
            await writable.seek(encOffset);
            await writable.write(encryptedData);

            // console.log(`Wrote ${length} bytes (plaintext) / ${encryptedData.byteLength} bytes (encrypted) at plainOffset ${offset}, encOffset ${encOffset} to OPFS with encryption`, encryptedData);

          } else {
            // Write the data without encryption
            await writable.seek(offset);
            await writable.write(plainData);
          }

          // console.log(`Wrote ${length} bytes at offset ${offset} to OPFS`);
        } else if (operation.type === "truncate") {
          // Truncate operation
          const { size } = operation;
          
          // Perform truncation
          await writable.truncate(size);
          writeFileLength = size;
          
          // If using encryption, remove pages for truncated data
          if (this.#encryptionKey && this.#idb) {
            try {
              const tx = this.#idb.transaction('pages', 'readwrite');
              const store = tx.objectStore('pages');
              
              // Delete all pages at plainOffsets >= size
              for (const plainOffset of Array.from(this.#pages.keys())) {
                if (plainOffset >= size) {
                  store.delete(plainOffset);
                  this.#pages.delete(plainOffset);
                }
              }
              
              await new Promise(resolve => {
                tx.oncomplete = resolve;
                tx.onerror = resolve; // Continue on error
              });
            } catch (e) {
              console.error(`Error cleaning up pages after truncate: ${e.message}`);
            }
          }
          // console.log(`Truncated OPFS file to ${size} bytes`);
        } else if (operation.type === "delete") {
          // Close the current writable since we're deleting the file
          await writable.close();

          try {
            // Delete the file
            await this.#rootDir.removeEntry(this.#opfsFilename);
            this.#dbFileHandle = null;
            
            // If using encryption, clear all pages
            if (this.#encryptionKey && this.#idb) {
              try {
                const tx = this.#idb.transaction('pages', 'readwrite');
                const store = tx.objectStore('pages');
                
                // Clear all pages
                store.clear();
                this.#pages.clear();
                
                await new Promise(resolve => {
                  tx.oncomplete = resolve;
                  tx.onerror = resolve; // Continue on error
                });
              } catch (e) {
                console.error(`Error clearing pages after delete: ${e.message}`);
              }
            }
            
            console.log(`Deleted OPFS file`);

            // Stop processing further operations
            continueProcessing = false;
            this.#writeQueue = [];
          } catch (e) {
            console.error(`Failed to delete OPFS file: ${e.message}`);
          }
        }
      }

      // Close the stream after all operations if we haven't already closed it
      if (continueProcessing) {
        await writable.close();
      }
    } catch (e) {
      console.error(`Failed to process operation queue: ${e.message}`);
    } finally {
      this.#isProcessingWrites = false;
      const end = performance.now();
      console.log(`Processed ${processed} write operations in ${(end - start).toFixed(2)} ms`);

      // If more operations were added while processing, start again
      if (this.#writeQueue.length > 0) {
        setTimeout(() => this.#processWriteQueue(), 0);
      }
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
