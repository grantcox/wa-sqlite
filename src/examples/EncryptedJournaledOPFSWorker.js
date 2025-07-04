/**
 * EncryptedJournaledOPFSWorker.js
 * 
 * Web Worker implementation for handling persistence operations for SyncMemoryProxyAsyncWorkerVFS.
 * This worker handles OPFS operations with mandatory encryption, using direct page writes to
 * specific locations based on the logical page index.
 * 
 * Since SQLite database and journal files are now handled separately, we can safely write pages
 * directly to their logical locations without worrying about atomicity issues. This simplifies
 * the implementation by eliminating the need for page index tracking and makes the code more
 * efficient by writing pages directly to their logical positions.
 * 
 * Each file has a simple header to identify the format, followed by encrypted pages at
 * fixed offsets corresponding to their logical positions in the SQLite file.
 * 
 * This worker supports multiple files with a shared encryption key.
 */

/**
 * @typedef {Object} PendingOperation
 * @property {'write'|'truncate'|'delete'} type
 * @property {number} [offset]
 * @property {number} [size]
 */

/**
 * @typedef {Object} WriteMessage
 * @property {Array<PendingOperation>} operations
 * @property {Uint8Array} databaseState
 * @property {number} startOffset
 * @property {number} totalSize
 */

/**
 * @typedef {Object} VFSConfig
 * @property {string} encryptionPassword - Required password for encryption
 * @property {string} dbName - Database name for files
 */

/**
 * @typedef {Object} FileState
 * @property {string} filename - File name
 * @property {FileSystemFileHandle} fileHandle - OPFS file handle
 * @property {FileSystemSyncAccessHandle} accessHandle - Sync access handle for OPFS
 * @property {ArrayBuffer} fileData - Current file data in memory
 * @property {number} fileSize - Logical size of the file in bytes (unencrypted size)
 * @property {number} pageCount - Number of valid pages in the file
 */

/**
 * Worker implementation that persists data to OPFS with mandatory encryption
 * Uses direct page writes at fixed locations based on logical page indices.
 */
class EncryptedJournaledOPFSWorker {
  // if we detect this is a secondary tab, we disable writes
  #writesEnabled = true;

  #initialized = false;
  #encryptionKey = null;

  // OPFS state
  /** @type {FileSystemDirectoryHandle} */ #rootDir = null;
  
  // Database name
  #dbName = "db";
  
  // Configuration
  #sourcePageSize = 65536; // Size of unencrypted pages
  
  // Fixed constants
  #IV_SIZE = 12;
  #AUTH_TAG_SIZE = 16; // GCM authentication tag size
  #opfsPageSize = this.#sourcePageSize + this.#IV_SIZE + this.#AUTH_TAG_SIZE;
  // the header needs to be large enough to hold all the page lookups and free page indices
  // 128KB is roughly enough for a 1GB SQLite file
  #HEADER_SIZE = 131072;
  #HEADER_VERSION = "YNABDB01"; // 8-byte version/magic identifier
  
  // Map of all open files, keyed by file name
  /** @type {Map<string, FileState>} */ #files = new Map();
  
  // Concurrency control - tracks which files are currently being processed
  /** @type {Set<string>} */ #processingFiles = new Set();

  // Queue for 'writes' messages per file
  /** @type {Map<string, Array<WriteMessage>>} */ #writeMessageQueues = new Map();

  /** @type {Map<string, number>} */ #writeTotalDuration = new Map();
  /** @type {Map<string, number>} */ #writeTotalPages = new Map();

  constructor() {
    // Set up the message handler
    self.onmessage = this.#handleMessage.bind(this);
  }

  /**
   * Base initialization
   * @param {VFSConfig} config - Configuration parameters for the worker
   * @returns {Promise<Map<string, ArrayBuffer>>} Map of filename to file data
   */
  async init(config) {
    if (!config.encryptionPassword) {
      throw new Error("Encryption password is required");
    }
    
    // Store the database name
    this.#dbName = config.dbName || "db";
    
    await this.buildEncryptionKey(config.encryptionPassword);

    try {
      // Initialize OPFS root directory
      this.#rootDir = await navigator.storage.getDirectory();
      
      // Preload all existing files and get their data
      const loadedFiles = await this.#preloadExistingFiles();
      
      console.log(`EncryptedJournaledOPFSWorker | Initialized with database name: ${this.#dbName}`);
      this.#initialized = true;
      
      return loadedFiles;
    } catch (e) {
      console.error('EncryptedJournaledOPFSWorker | Initialization failed:', e);
      throw e;
    }
  }

  /**
   * Initialize the encryption key from password
   * @param {string} password - Password to derive key from
   * @returns {Promise<CryptoKey>} - The derived encryption key
   */
  async buildEncryptionKey(password) {
    if (!password) {
      throw new Error("Encryption password is required");
    }
    
    // Initialize encryption key from password
    const encoder = new TextEncoder();
    const passwordData = encoder.encode(password);

    // Derive a key from the password
    const keyMaterial = await crypto.subtle.importKey(
      "raw", 
      passwordData, 
      "PBKDF2", 
      false, 
      ["deriveBits", "deriveKey"]
    );

    // Use PBKDF2 to derive a key
    this.#encryptionKey = await crypto.subtle.deriveKey(
      {
        name: "PBKDF2",
        salt: encoder.encode("wa-sqlite-encrypted-vfs"),
        iterations: 100000,
        hash: "SHA-256",
      },
      keyMaterial,
      { name: "AES-GCM", length: 256 },
      false,
      ["encrypt", "decrypt"]
    );
    
    return this.#encryptionKey;
  }

  /**
   * Preload all existing files that match the configured prefix
   * @returns {Promise<Map<string, ArrayBuffer>>} Map of filename to file data
   */
  async #preloadExistingFiles() {
    try {
      const loadedFiles = new Map();
      
      // Get all files in the root directory
      for await (const entry of this.#rootDir.values()) {
        // Only process files (not directories) that start with our dbName
        if (entry.kind === 'file' && entry.name.startsWith(this.#dbName)) {
          const filename = entry.name;
        
          // Load the file
          const fileState = await this.#loadFile(entry.name);
          if (fileState && fileState.fileData) {
            // Create a copy of the file data for sending
            const fileDataCopy = new Uint8Array(fileState.fileData.byteLength);
            fileDataCopy.set(new Uint8Array(fileState.fileData));
            
            // Add to the map with filename as key
            loadedFiles.set(filename, fileDataCopy.buffer);
          }
          
          console.log(`EncryptedJournaledOPFSWorker | Preloaded file: ${filename}`);
        }
      }
      
      return loadedFiles;
    } catch (e) {
      console.error('EncryptedJournaledOPFSWorker | Failed to preload existing files:', e);
      return new Map();
    }
  }

  /**
   * Load a specific file
   * @param {string} filename - The file name
   * @returns {Promise<FileState|null>}
   */
  async #loadFile(filename) {
    try {
      // Skip if already loaded
      if (this.#files.has(filename)) {
        return this.#files.get(filename);
      }
      
      // Get file handle
      const fileHandle = await this.#rootDir.getFileHandle(filename, {
        create: true,
      });
      const initialEncryptedFile = await fileHandle.getFile();
      
      // Read header to get file metadata
      const headerInfo = await this.#readHeader(initialEncryptedFile);
      
      // Load existing data from OPFS
      const fileData = await this.#decryptFile(initialEncryptedFile);

      // Create a file state object first (without access handle)
      const fileState = {
        filename: filename,
        fileHandle,
        accessHandle: null, // Will be set later if available
        fileData,
        fileSize: headerInfo.isValid ? headerInfo.fileSize : fileData.byteLength,
        pageCount: headerInfo.isValid ? headerInfo.pageCount : Math.ceil(fileData.byteLength / this.#sourcePageSize)
      };
      
      // Store in our files map
      this.#files.set(filename, fileState);
      
      // Initialize an empty write queue for this file
      this.#writeMessageQueues.set(filename, []);
      
      // Now try to get the sync access handle, but don't let failure stop the whole process
      try {
        // Create a synchronous access handle for the file
        const accessHandle = await fileHandle.createSyncAccessHandle();
        fileState.accessHandle = accessHandle;
      } catch (accessHandleError) {
        if (accessHandleError instanceof DOMException && accessHandleError.name === "NoModificationAllowedError") {
          console.warn(`EncryptedJournaledOPFSWorker | OPFS file ${filename} is already open in another context. This tab will be read-only.`);
          fileState.accessHandle = null;
        } else {
          console.error(`EncryptedJournaledOPFSWorker | Failed to get sync access handle for ${filename}:`, accessHandleError);
          fileState.accessHandle = null;
        }
        // any error means we can't write to this file
        this.#writesEnabled = false;
      }
      
      return fileState;
    } catch (e) {
      if (e instanceof DOMException && e.name === "OperationError") {
        console.error(`EncryptedJournaledOPFSWorker | Incorrect encryption key or corrupted data for file ${filename}`);
        await this.#processDeleteOperation(filename);
        return this.#loadFile(filename);
      }
      
      console.error(`EncryptedJournaledOPFSWorker | Failed to load file ${filename}:`, e);
      // unknown error so we disable writes
      this.#writesEnabled = false;
      
      // Return a minimal file state with empty buffer
      const minimalFileState = {
        filename: filename,
        fileHandle: null,
        accessHandle: null,
        fileData: new ArrayBuffer(0),
        fileSize: 0,
        pageCount: 0
      };
      
      // Add to our maps to avoid repeated load attempts
      this.#files.set(filename, minimalFileState);
      this.#writeMessageQueues.set(filename, []);
      
      return minimalFileState;
    }
  }

  /**
   * Handle message received from main thread
   * @param {MessageEvent} e Message event
   */
  async #handleMessage(e) {
    const msg = e.data;
    
    switch (msg.type) {
      case 'init':
        try {
          // Initialize and get all preloaded files
          const loadedFiles = await this.init(msg.config);
          
          // Convert the map to an array of file objects for sending
          const fileDetails = [];
          for (const [filename, fileData] of loadedFiles.entries()) {
            const fileDataUintArray = new Uint8Array(fileData);

            fileDetails.push({
              name: filename,
              data: fileDataUintArray
            });
          }
          
          // Prepare transfer list for all file data buffers
          const transferObjects = fileDetails.map(file => file.data.buffer);
          
          // Respond with initialization success and all file data
          self.postMessage({
            type: 'initComplete',
            writesEnabled: this.#writesEnabled,
            files: fileDetails
          }, transferObjects);
        } catch (error) {
          console.error('EncryptedJournaledOPFSWorker | Initialization failed:', error);
          self.postMessage({
            type: 'error',
            message: error.message
          });
        }
        break;
        
      case 'writes':
        // Process database state and operations for a specific file
        if (!this.#initialized) {
          throw new Error('Worker not initialized');
        }
        
        if (this.#writesEnabled) {
          const filename = msg.filename;
          if (!filename) {
            throw new Error('File name is required for writes');
          }
          
          this.#handleWrites(filename, msg.operations, msg.databaseState, msg.startOffset, msg.totalSize);
        }
        break;

      default:
        console.error('EncryptedJournaledOPFSWorker | Unknown message type:', msg.type);
    }
  }

  /**
   * Handle a batch of write operations with a partial database state for a specific file
   * @param {string} filename - The file name
   * @param {Array<PendingOperation>} operations - Array of operations that were performed
   * @param {Uint8Array} partialData - Partial database state (specific range)
   * @param {number} startOffset - Starting offset of the partial data
   * @param {number} totalSize - Total size of the complete file
   */
  #handleWrites(filename, operations, partialData, startOffset, totalSize) {
    // Make sure we have a queue for this file
    if (!this.#writeMessageQueues.has(filename)) {
      this.#writeMessageQueues.set(filename, []);
    }
    
    // Add this message to the queue
    const queue = this.#writeMessageQueues.get(filename);
    queue.push({
      operations,
      databaseState: partialData,
      startOffset,
      totalSize
    });

    // Start processing the queue (will exit immediately if already running)
    this.#processWriteMessageQueue(filename);
  }

  /**
   * Process queued write messages for a specific file
   * @param {string} filename - The file name
   */
  async #processWriteMessageQueue(filename) {
    // If already processing this file, exit early
    if (this.#processingFiles.has(filename)) {
      return;
    }

    // Set processing flag for this file
    this.#processingFiles.add(filename);

    try {
      // Get the queue for this file
      const queue = this.#writeMessageQueues.get(filename) || [];
      
      // Process all messages in the queue
      while (queue.length > 0) {
        const message = queue.shift();

        // Make sure the file exists
        if (!this.#files.has(filename)) {
          await this.#loadFile(filename);
        }
        
        // Get the file state
        const fileState = this.#files.get(filename);
      
        // Handle partial data update
        const partialData = new Uint8Array(message.databaseState);
        const startOffset = message.startOffset;
        const totalSize = message.totalSize;
        
        // Make sure we have a buffer large enough
        if (!fileState.fileData || fileState.fileData.byteLength < totalSize) {
          // Create a new buffer with the total size
          const newBuffer = new ArrayBuffer(totalSize);
          const newView = new Uint8Array(newBuffer);
          
          // Copy existing data if any
          if (fileState.fileData) {
            newView.set(new Uint8Array(fileState.fileData));
          }
          
          fileState.fileData = newBuffer;
        }
        
        // Copy the partial data into the correct position in the buffer
        const fileView = new Uint8Array(fileState.fileData);
        fileView.set(partialData, startOffset);
        
        // Update file size if needed
        fileState.fileSize = Math.max(fileState.fileSize, totalSize);
        fileState.pageCount = Math.ceil(fileState.fileSize / this.#sourcePageSize);


        // Call processWriteQueue to persist changes with all operations
        await this.processWriteQueue(filename, message.operations);
      }
    } finally {
      // Clear processing flag for this file
      this.#processingFiles.delete(filename);

      // If new messages arrived while we were processing, start processing again
      const queue = this.#writeMessageQueues.get(filename) || [];
      if (queue.length > 0) {
        // Use setTimeout to prevent stack overflow with deep recursion
        setTimeout(() => this.#processWriteMessageQueue(filename), 0);
      }
    }
  }

  /**
   * Process operations to persist changes to OPFS for a specific file.
   * Uses direct writes at logical page locations.
   * @param {string} filename - The file name
   * @param {Array<PendingOperation>} operations - Array of operations to process
   */
  async processWriteQueue(filename, operations) {
    // If nothing to process, exit early
    if (!operations || operations.length === 0) {
      return;
    }

    // Get the file state
    const fileState = this.#files.get(filename);
    if (!fileState) {
      console.error(`EncryptedJournaledOPFSWorker | File ${filename} not found for write operation`);
      return;
    }

    // In read-only mode, we can't process write operations
    if (!fileState.accessHandle) {
      console.warn(`EncryptedJournaledOPFSWorker | File ${filename} is in read-only mode (no access handle), cannot process write operations`);
      return;
    }

    const start = performance.now();
    let writtenPageCount = 0;

    try {
      // Track dirty pages that need to be written
      /** @type {Set<number>} */ let dirtyPages = new Set();
      let needsHeader = false;
      let newSize = fileState.fileSize;
      
      for (const operation of operations) {
        if (operation.type === 'write') {
          // For write operations, determine the affected pages
          const { startPageIndex, endPageIndex } = this.#getAffectedPageRange(operation.offset, operation.size);

          for (let pageIndex = startPageIndex; pageIndex <= endPageIndex; pageIndex++) {
            dirtyPages.add(pageIndex);
          }
          
          // Update file size if this write extends the file
          const writeEndOffset = operation.offset + operation.size;
          if (writeEndOffset > newSize) {
            newSize = writeEndOffset;
          }
          
          needsHeader = true;
        } else if (operation.type === 'truncate') {
          const size = operation.size;
          const truncatePageIndex = this.#getPageIndex(size);

          // Remove pages beyond the truncation point from dirty pages
          for (const pageIdx of dirtyPages) {
            if (pageIdx > truncatePageIndex) {
              dirtyPages.delete(pageIdx);
            }
          }
          
          // Update file size based on truncation
          newSize = size;
          needsHeader = true;
        } else if (operation.type === 'delete') {
          // treat it as a truncate 0
          dirtyPages.clear();
          newSize = 0;
          needsHeader = true;
        }
      }

      // Write a header if needed, with updated file size
      if (needsHeader) {
        await this.#writeHeader(filename, newSize);
      }

      // Write all dirty pages
      if (dirtyPages.size > 0) {
        const writePages = Array.from(dirtyPages);
        await this.#writePages(filename, writePages);
        writtenPageCount = writePages.length;
      }

      // Sync changes to disk
      fileState.accessHandle?.flush();

    } catch (e) {
      console.error(`EncryptedJournaledOPFSWorker | Failed to process operation queue for ${filename}: ${e.message}`);
    } finally {
      const end = performance.now();
      this.#writeTotalDuration.set(filename, (this.#writeTotalDuration.get(filename) || 0) + (end - start));
      this.#writeTotalPages.set(filename, (this.#writeTotalPages.get(filename) || 0) + writtenPageCount);
      console.log(`EncryptedJournaledOPFSWorker | ${filename}: Wrote ${writtenPageCount} pages in ${(end - start).toFixed(1)} ms`);
      console.log(`EncryptedJournaledOPFSWorker | ${filename}: Total write time ${this.#writeTotalDuration.get(filename).toFixed(1)} ms for ${this.#writeTotalPages.get(filename)} pages`);
    }
  }

  /**
   * Close a specific file
   * @param {string} filename - The file name to close
   */
  async closeFile(filename) {
    const fileState = this.#files.get(filename);
    if (!fileState) return;
    
    try {
      // Close the access handle
      if (fileState.accessHandle) {
        fileState.accessHandle.close();
      }
      
      // Remove from our maps
      this.#files.delete(filename);
      this.#writeMessageQueues.delete(filename);
      this.#processingFiles.delete(filename);
    } catch (e) {
      console.error(`EncryptedJournaledOPFSWorker | Error closing file ${filename}:`, e);
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

    // Encrypt the data using encryption key
    const encryptedBuffer = await crypto.subtle.encrypt(
      {
        name: "AES-GCM",
        iv,
      },
      this.#encryptionKey,
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
    // Decrypt the data using the encryption key
    const decryptedBuffer = await crypto.subtle.decrypt(
      {
        name: "AES-GCM",
        iv,
      },
      this.#encryptionKey,
      encryptedData
    );

    // Return the decrypted data
    return new Uint8Array(decryptedBuffer);
  }

  // --------------------------------------------------------------------------
  // Private methods for file header management
  // --------------------------------------------------------------------------

  /**
   * Write header with file metadata to the file
   * @param {string} filename - The file name
   * @param {number} [size] - Optional size to set (if not provided, uses the current file size)
   */
  async #writeHeader(filename, size) {
    const fileState = this.#files.get(filename);
    if (!fileState || !fileState.accessHandle) {
      throw new Error(`Cannot write header: File ${filename} not found or not accessible`);
    }
    
    // If size is provided, update the fileState
    if (size !== undefined) {
      fileState.fileSize = size;
      fileState.pageCount = Math.ceil(size / this.#sourcePageSize);
    }

    // Prepare the header buffer
    const headerBuffer = new Uint8Array(this.#HEADER_SIZE);
    headerBuffer.fill(0); // Initialize to zeros

    // Calculate header positions
    const versionSize = this.#HEADER_VERSION.length; // 8 bytes
    let position = 0;

    // 1. Write version identifier
    const encoder = new TextEncoder();
    const versionBytes = encoder.encode(this.#HEADER_VERSION);
    headerBuffer.set(versionBytes, position);
    position += versionSize;

    // 2. Write file size (8 bytes for big files, little-endian)
    const dataView = new DataView(headerBuffer.buffer);
    dataView.setBigUint64(position, BigInt(fileState.fileSize), true);
    position += 8;

    // 3. Write page count (4 bytes, little-endian)
    dataView.setUint32(position, fileState.pageCount, true);
    position += 4;

    // Write the header to the start of the file
    const bytesWritten = fileState.accessHandle.write(headerBuffer, { at: 0 });

    if (bytesWritten !== headerBuffer.length) {
      throw new Error(`Failed to write entire header for ${filename}. Wrote ${bytesWritten} of ${headerBuffer.length} bytes`);
    }
  }

  // --------------------------------------------------------------------------
  // Private methods for direct page operations
  // --------------------------------------------------------------------------
  
  /**
   * Write multiple pages to a file at their direct logical locations
   * @param {string} filename - The file name
   * @param {number[]} pageIndices - Array of page indices to write
   * @returns {Promise<number[]>} - Array of physical offsets where each page was written
   */
  async #writePages(filename, pageIndices) {
    const fileState = this.#files.get(filename);
    if (!fileState) {
      throw new Error(`File ${filename} not found`);
    }
    
    const fileData = fileState.fileData;
    if (!fileData) {
      throw new Error(`Cannot write pages for ${filename} without file data`);
    }
    
    const writtenOffsets = [];
    
    for (const pageIndex of pageIndices) {
      // Calculate the physical offset for this page - directly based on its index
      // We add the header size to leave room for the header
      const physicalOffset = this.#HEADER_SIZE + (pageIndex * this.#opfsPageSize);
      
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
        const sourceData = new Uint8Array(fileData.slice(pageStart, pageStart + availableData));
        plainData.set(sourceData, 0);
      }

      // Encrypt the page
      const { encryptedData, iv } = await this.#encryptData(plainData);
      
      // Create a combined buffer with IV + encrypted data
      const combinedData = new Uint8Array(this.#IV_SIZE + encryptedData.byteLength);
      combinedData.set(iv, 0);
      combinedData.set(encryptedData, this.#IV_SIZE);

      // Write the data to the file using the sync access handle at the fixed offset
      const bytesWritten = fileState.accessHandle.write(combinedData, { at: physicalOffset });
      
      if (bytesWritten !== combinedData.byteLength) {
        throw new Error(`Failed to write entire page for ${filename}. Wrote ${bytesWritten} of ${combinedData.byteLength} bytes`);
      }
      
      // Record the physical offset used
      writtenOffsets.push(physicalOffset);
    }
    
    return writtenOffsets;
  }
  
  // We no longer need the getNextWriteOffset method since we now use direct page locations

  // --------------------------------------------------------------------------
  // Private methods for OPFS
  // --------------------------------------------------------------------------

  /**
   * Read the header from an encrypted file
   * @param {File} encryptedFile - The encrypted file from OPFS
   * @returns {Promise<{fileSize: number, pageCount: number, isValid: boolean}>} - Header information
   */
  async #readHeader(encryptedFile) {
    // Default header info
    const headerInfo = {
      fileSize: 0,
      pageCount: 0,
      isValid: false
    };
    
    // If file is smaller than header size, it can't be valid
    if (encryptedFile.size < this.#HEADER_SIZE) {
      return headerInfo;
    }
    
    try {
      // Read just the header portion
      const headerSlice = await encryptedFile.slice(0, this.#HEADER_SIZE).arrayBuffer();
      const headerView = new Uint8Array(headerSlice);
      
      // Check version identifier
      const versionSize = this.#HEADER_VERSION.length; // 8 bytes
      const decoder = new TextDecoder();
      const versionBytes = headerView.slice(0, versionSize);
      const version = decoder.decode(versionBytes);
      
      if (version !== this.#HEADER_VERSION) {
        console.warn(`EncryptedJournaledOPFSWorker | Invalid header version: ${version}`);
        return headerInfo;
      }
      
      // Extract file size and page count from header
      const dataView = new DataView(headerSlice);
      let position = versionSize;
      
      // Read file size (8 bytes)
      const fileSize = Number(dataView.getBigUint64(position, true));
      position += 8;
      
      // Read page count (4 bytes)
      const pageCount = dataView.getUint32(position, true);
      position += 4;
      
      // Validate that the size and page count make sense
      if (fileSize >= 0 && pageCount >= 0 && 
          Math.ceil(fileSize / this.#sourcePageSize) === pageCount) {
        headerInfo.fileSize = fileSize;
        headerInfo.pageCount = pageCount;
        headerInfo.isValid = true;
      }
      
      console.log(`EncryptedJournaledOPFSWorker | Read header: file size ${fileSize}, page count ${pageCount}`);
      return headerInfo;
    } catch (e) {
      console.error(`EncryptedJournaledOPFSWorker | Error reading header:`, e);
      return headerInfo;
    }
  }

  /**
   * Read the database file from OPFS by directly reading pages from their logical locations
   * @param {File} encryptedFile - The encrypted file from OPFS
   * @returns {Promise<ArrayBuffer>} - The file content
   */
  async #decryptFile(encryptedFile) {
    // If the file is just the header, return an empty buffer
    if (encryptedFile.size <= this.#HEADER_SIZE) {
      console.log(`EncryptedJournaledOPFSWorker | Empty database file, returning empty buffer`);
      return new ArrayBuffer(0);
    }
    
    // Read the header to get file size and page count
    const headerInfo = await this.#readHeader(encryptedFile);
    
    // Determine how many pages to read
    let pagesToRead = 0;
    
    if (headerInfo.isValid) {
      // Use the page count from the header
      pagesToRead = headerInfo.pageCount;
    } else {
      // Fall back to calculating based on physical file size
      const pagesInFile = Math.floor((encryptedFile.size - this.#HEADER_SIZE) / this.#opfsPageSize);
      pagesToRead = pagesInFile;
    }
    
    if (pagesToRead === 0) {
      return new ArrayBuffer(0);
    }

    // Calculate the size of the decrypted buffer (based on logical file size)
    let decryptedSize;
    if (headerInfo.isValid) {
      decryptedSize = headerInfo.fileSize;
    } else {
      decryptedSize = pagesToRead * this.#sourcePageSize;
    }
    
    const decryptedBuffer = new ArrayBuffer(decryptedSize);
    const decryptedView = new Uint8Array(decryptedBuffer);

    // Process the file with direct page locations
    for (let pageIndex = 0; pageIndex < pagesToRead; pageIndex++) {
      try {
        // Calculate the physical offset for this page
        const physicalOffset = this.#HEADER_SIZE + (pageIndex * this.#opfsPageSize);
        
        // Read the encrypted page from its physical location
        const pageSlice = await encryptedFile.slice(
          physicalOffset,
          physicalOffset + this.#opfsPageSize
        ).arrayBuffer();
        const encryptedPage = new Uint8Array(pageSlice);

        if (encryptedPage.length !== this.#opfsPageSize) {
          console.warn(`EncryptedJournaledOPFSWorker | Read less data than expected - only ${encryptedPage.length} bytes from page ${pageIndex}`);
          continue;
        }

        // Extract the IV from the beginning of the page
        const iv = encryptedPage.slice(0, this.#IV_SIZE);

        // Extract the encrypted data
        const encryptedData = encryptedPage.slice(this.#IV_SIZE);

        // Decrypt the page
        const decryptedPage = await this.#decryptData(encryptedData, iv);

        // Copy the decrypted data to the correct logical position
        const destOffset = pageIndex * this.#sourcePageSize;
        
        // Make sure we don't write past the end of the buffer
        const bytesToWrite = Math.min(decryptedPage.length, decryptedBuffer.byteLength - destOffset);
        if (bytesToWrite > 0) {
          decryptedView.set(new Uint8Array(decryptedPage.buffer, 0, bytesToWrite), destOffset);
        }
      } catch (e) {
        // If decryption fails, leave this page as zeros
        console.error(`EncryptedJournaledOPFSWorker | Error reading/decrypting page ${pageIndex}:`, e);
      }
    }

    console.log(`EncryptedJournaledOPFSWorker | Finished reading file with ${pagesToRead} pages, produced ${decryptedBuffer.byteLength} bytes of plaintext`);
    return decryptedBuffer;
  }

  /**
   * Process a delete operation for a specific file
   * @param {string} filename - The file name to delete
   */
  async #processDeleteOperation(filename) {
    const fileState = this.#files.get(filename);
    if (!fileState) return;
    
    // Close the current access handle if it exists
    if (fileState.accessHandle) {
      fileState.accessHandle.close();
    }

    try {
      // Delete the file from OPFS
      await this.#rootDir.removeEntry(filename);
      
      // Reset the file state
      this.#files.delete(filename);
      
      console.log(`EncryptedJournaledOPFSWorker | Deleted OPFS file ${filename}`);
    } catch (e) {
      console.error(`EncryptedJournaledOPFSWorker | Error deleting file ${filename}:`, e);
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
const worker = new EncryptedJournaledOPFSWorker();
