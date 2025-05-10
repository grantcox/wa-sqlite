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

  // Queue for 'writes' messages
  /** @type {Array<{operations: Array, databaseState: Uint8Array}>} */ #writeMessageQueue = [];
  
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
   * Process a set of operations
   * This method should be overridden by subclasses
   * @param {Array<PendingOperation>} operations - Array of operations to process
   */
  async processWriteQueue(operations) {
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
   * Handle a batch of write operations and a new database state
   * @param {Array<PendingOperation>} operations - Array of operations that were performed
   * @param {Uint8Array} databaseState - The new complete database state
   */
  #handleWrites(operations, databaseState) {
    // Add this message to the queue
    this.#writeMessageQueue.push({
      operations,
      databaseState
    });

    // Start processing the queue (will exit immediately if already running)
    this.#processWriteMessageQueue();
  }

  /**
   * Process queued write messages
   */
  async #processWriteMessageQueue() {
    // If already processing, exit early - the current processor will handle new items
    if (this.#processingOperations) {
      return;
    }

    // Set processing flag
    this.#processingOperations = true;

    try {
      // Process all messages in the queue
      while (this.#writeMessageQueue.length > 0) {
        const message = this.#writeMessageQueue.shift();

        // Update the in-memory database state
        this.#fileData = message.databaseState.buffer;

        // Call processWriteQueue to allow subclasses to persist changes with all operations
        await this.processWriteQueue(message.operations);
      }
    } finally {
      // Clear processing flag
      this.#processingOperations = false;

      // If new messages arrived while we were processing, start processing again
      if (this.#writeMessageQueue.length > 0) {
        // Use setTimeout to prevent stack overflow with deep recursion
        setTimeout(() => this.#processWriteMessageQueue(), 0);
      }
    }
  }
}
