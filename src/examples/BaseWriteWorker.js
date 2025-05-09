/**
 * BaseWriteWorker.js
 * 
 * Base class for web workers that handle ordered write operations.
 * This handles message passing and operation ordering but delegates 
 * the actual persistence to subclasses.
 */

/**
 * @typedef {Object} PendingOperation
 * @property {'write'|'truncate'|'delete'} type
 * @property {number} [offset]
 * @property {number} [bufferOffset]
 * @property {number} [length]
 * @property {number} [size]
 */

/**
 * @typedef {Object} VFSConfig
 * @property {string} encryptionPassword - Required password for encryption
 * @property {string} [dbName] - Optional database name
 * @property {string} [pageSize] - Optional page size to write
 */

export class BaseWriteWorker {
  // Order tracking for writes
  #pendingOperations = [];
  /** @type {ArrayBuffer} */ #fileData = null;
  #initialized = false;
  #encryptionKey = null;
  
  // Maximum number of write operations to process in a single iteration
  #maxWritesPerIteration = 1000;
  
  // Concurrency control
  #processingOperations = false;
  
  constructor() {
    // Set up the message handler
    self.onmessage = this.#handleMessage.bind(this);
  }

  /**
   * Base initialization
   * @param {VFSConfig} config - Configuration parameters for the worker
   */
  async _init(config) {
    if (!config.encryptionPassword) {
      throw new Error("Encryption password is required");
    }
    await this.buildEncryptionKey(config.encryptionPassword);
    
    // Initialize the worker
    await this.init(config);
    this.#initialized = true;
  }

  /**
   * Initialize worker with configuration
   * @param {VFSConfig} config - Configuration parameters for the worker
   */
  async init(config) {
    // This method should be overridden by subclasses
    throw new Error('init() must be implemented by subclass');
  }

  /**
   * Process a write queue of pending operations
   * This method should be overridden by subclasses
   */
  async processWriteQueue() {
    // This method should be overridden by subclasses
    throw new Error('processWriteQueue() must be implemented by subclass');
  }

  /**
   * Set file data - accessor for subclasses
   * @param {ArrayBuffer} data New file data
   */
  setFileData(data) {
    this.#fileData = data;
  }

  /**
   * Get file data - accessor for subclasses
   * @returns {ArrayBuffer} Current file data
   */
  getFileData() {
    return this.#fileData;
  }

  /**
   * Get encryption key - accessor for subclasses
   * @returns {CryptoKey} Encryption key
   */
  getEncryptionKey() {
    return this.#encryptionKey;
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
   * Process a write operation
   * @param {number} offset Where to write
   * @param {Uint8Array} data Data to write
   */
  async processWrite(offset, data) {
    // Default implementation updates in-memory representation only
    if (!this.#fileData) {
      this.#fileData = new ArrayBuffer(0);
    }
    
    // Make sure our in-memory representation is large enough
    if (offset + data.byteLength > this.#fileData.byteLength) {
      const newSize = Math.max(offset + data.byteLength, 2 * this.#fileData.byteLength);
      const newFileData = new ArrayBuffer(newSize);
      new Uint8Array(newFileData).set(new Uint8Array(this.#fileData));
      this.#fileData = newFileData;
    }
    
    // Copy the data at the specified offset
    new Uint8Array(this.#fileData, offset, data.byteLength).set(data);
  }

  /**
   * Process a truncate operation
   * @param {number} size New file size
   */
  async processTruncate(size) {
    // Default implementation updates in-memory representation only
    if (this.#fileData && size < this.#fileData.byteLength) {
      // Create a smaller buffer with the truncated size
      const newFileData = new ArrayBuffer(size);
      new Uint8Array(newFileData).set(new Uint8Array(this.#fileData, 0, size));
      this.#fileData = newFileData;
    }
  }

  /**
   * Process a delete operation
   */
  async processDelete() {
    // Default implementation clears in-memory representation
    this.#fileData = new ArrayBuffer(0);
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
          await this._init(msg.config);
          
          // Send initialization complete message, with a copy of the file data
          const initDataCopy = new Uint8Array(new Uint8Array(this.#fileData));
          self.postMessage({
            type: 'initComplete',
            fileData: initDataCopy
          }, [initDataCopy.buffer]);

        } catch (error) {
          console.error('BaseWriteWorker | Initialization failed:', error);
          self.postMessage({
            type: 'error',
            message: error.message
          });
        }
        break;
        
      case 'writes':
        // Process writes in batch
        if (!this.#initialized) {
          self.postMessage({
            type: 'error',
            message: 'Worker not initialized'
          });
          break;
        }
        this.#handleWrites(msg.operations, msg.data);
        break;
        
      default:
        console.error('BaseWriteWorker | Unknown message type:', msg.type);
    }
  }

  /**
   * Process operations in a batch
   * Limits the number of operations processed in a single iteration
   * to avoid excessive memory usage and improve responsiveness
   */
  async #processOperationsBatch() {
    // check (and get) the processing lock
    if (this.#processingOperations) {
      return;
    }
    this.#processingOperations = true;
    
    try {
      // Process up to maxWritesPerIteration operations at once
      const operationsToProcess = this.#pendingOperations.splice(0, this.#maxWritesPerIteration);
      
      if (operationsToProcess.length > 0) {
        // Process each operation in the batch
        for (const operation of operationsToProcess) {
          // Process the operation based on its type
          if (operation.type === 'write') {
            await this.processWrite(operation.offset, operation.data);
          } else if (operation.type === 'truncate') {
            await this.processTruncate(operation.size);
          } else if (operation.type === 'delete') {
            await this.processDelete();
          }
        }
        
        // Trigger the write queue processing in the subclass
        await this.processWriteQueue();
      }
      
      // If there are more operations, schedule another processing batch
      if (this.#pendingOperations.length > 0) {
        setTimeout(() => this.#processOperationsBatch(), 0);
      }
    } finally {
      this.#processingOperations = false;
    }
  }

  /**
   * Handle a batch of write operations
   * @param {Array<PendingOperation>} operations - Array of operations to process
   * @param {Uint8Array} sharedBuffer - Buffer containing all write data
   */
  #handleWrites(operations, sharedBuffer) {
    // Check if we have operations
    if (!operations || operations.length === 0) {
      return;
    }
    
    // Process each operation
    for (const operation of operations) {
      // For write operations, extract the data from the shared buffer
      if (operation.type === 'write' && operation.bufferOffset !== undefined && operation.length !== undefined) {
        // Create a view of the data in the shared buffer
        operation.data = new Uint8Array(sharedBuffer.buffer, operation.bufferOffset, operation.length);
      }
      
      // Add to the pending operations array
      this.#pendingOperations.push(operation);
    }
    
    // Start processing the operations
    this.#processOperationsBatch();
  }
}