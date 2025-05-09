/**
 * BaseWriteWorker.js
 * 
 * Base class for web workers that handle write operations.
 * This handles message passing and delegates the actual persistence to subclasses.
 */

/**
 * @typedef {Object} PendingOperation
 * @property {'write'|'truncate'|'delete'} type
 * @property {number} [offset]
 * @property {number} [size]
 */

/**
 * @typedef {Object} VFSConfig
 * @property {string} encryptionPassword - Required password for encryption
 * @property {string} [dbName] - Optional database name
 * @property {string} [pageSize] - Optional page size to write
 */

export class BaseWriteWorker {
  /** @type {ArrayBuffer} */ #fileData = null;
  #initialized = false;
  #encryptionKey = null;
  
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
   * @param {number} offset Data that was changed
   * @param {number} size Number of bytes
   */
  async processWrite(offset, size) {
    // just exists as a hook for subclasses
  }

  /**
   * Process a truncate operation
   * @param {number} size New file size
   */
  async processTruncate(size) {
    // just exists as a hook for subclasses
  }

  /**
   * Process a delete operation
   */
  async processDelete() {
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
        // Process database state and operations
        if (!this.#initialized) {
          self.postMessage({
            type: 'error',
            message: 'Worker not initialized'
          });
          break;
        }
        this.#handleWrites(msg.operations, msg.databaseState);
        break;
        
      default:
        console.error('BaseWriteWorker | Unknown message type:', msg.type);
    }
  }

  /**
   * Process operations in a batch (protected by concurrency lock)
   * @param {Array<PendingOperation>} operations - Array of operations to process
   */
  async #processOperations(operations) {
    // check (and get) the processing lock
    if (this.#processingOperations) {
      return;
    }
    this.#processingOperations = true;
    
    try {
      // Process each operation (even though we're replacing the entire file data,
      // subclasses may need these to be called to track dirty pages or other state)
      for (const operation of operations) {
        if (operation.type === 'write') {
          await this.processWrite(operation.offset, operation.size);
        } else if (operation.type === 'truncate') {
          await this.processTruncate(operation.size);
        } else if (operation.type === 'delete') {
          await this.processDelete();
        }
      }
      
      // Call processWriteQueue to allow subclasses to persist changes
      await this.processWriteQueue();
    } finally {
      this.#processingOperations = false;
    }
  }

  /**
   * Handle a batch of write operations and a new database state
   * @param {Array<PendingOperation>} operations - Array of operations that were performed
   * @param {Uint8Array} databaseState - The new complete database state
   */
  #handleWrites(operations, databaseState) {
    // Replace the entire file data with the new state
    this.#fileData = databaseState.buffer;
    
    // Process the operations to allow subclasses to track changes
    if (operations.length > 0) {
      this.#processOperations(operations);
    } else {
      // If no specific operations, still call processWriteQueue
      this.processWriteQueue();
    }
  }
}
