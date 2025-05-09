/**
 * BaseWriteWorker.js
 * 
 * Base class for web workers that handle ordered write operations.
 * This handles message passing, operation ordering, and acknowledgments,
 * but delegates the actual persistence to subclasses.
 */

/**
 * @typedef {Object} PendingOperation
 * @property {'write'|'truncate'|'delete'} type
 * @property {number} counter
 * @property {number} [offset]
 * @property {Uint8Array} [data]
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
  #lastProcessedCounter = -1;
  #pendingOperations = new Map();
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
        // Process writes in order
        if (!this.#initialized) {
          self.postMessage({
            type: 'error',
            message: 'Worker not initialized'
          });
          break;
        }
        this.#handleWrites(msg.operations);
        break;
        
      default:
        console.error('BaseWriteWorker | Unknown message type:', msg.type);
    }
  }

  /**
   * Queue an operation for processing in order
   * @param {PendingOperation} operation - The operation to queue
   * @returns {boolean} - Whether the operation was queued successfully
   */
  #queueOrderedOperation(operation) {
    if (this.#lastProcessedCounter >= operation.counter) {
      return false; // Already processed this counter or a higher one
    }

    // Store the operation in our ordered map
    this.#pendingOperations.set(operation.counter, operation);
    return true;
  }

  /**
   * Process operations in order by counter
   * Limits the number of operations processed in a single iteration
   * to avoid excessive memory usage and improve responsiveness
   * 
   * This method is designed to be called both directly and via setTimeout,
   * with concurrency controls to ensure it's never running in parallel.
   */
  async #processOrderedOperations() {
    // check (and get) the processing lock
    if (this.#processingOperations) {
      return;
    }
    this.#processingOperations = true;
    
    try {      
      let processedAny = false;
      let processedCount = 0;
      
      // Continue processing as long as we have sequential operations
      // and we haven't exceeded the maximum number of operations per iteration
      while (
        this.#pendingOperations.has(this.#lastProcessedCounter + 1) && 
        processedCount < this.#maxWritesPerIteration
      ) {
        processedAny = true;
        const nextCounter = this.#lastProcessedCounter + 1;
        const operation = this.#pendingOperations.get(nextCounter);
      
        // Process the operation based on its type
        if (operation.type === 'write') {
          await this.processWrite(operation.offset, operation.data);
        } else if (operation.type === 'truncate') {
          await this.processTruncate(operation.size);
        } else if (operation.type === 'delete') {
          await this.processDelete();
        }
        
        // Update the last processed counter
        this.#lastProcessedCounter = nextCounter;
        
        // Remove the operation from pending
        this.#pendingOperations.delete(nextCounter);
        
        // Increment the processed count
        processedCount++;
      }
      
      // Send acknowledgments for all processed operations if we processed any
      if (processedAny) {
        self.postMessage({
          type: 'writeAck',
          upToCounter: this.#lastProcessedCounter
        });

        // Trigger the write queue processing in the subclass
        await this.processWriteQueue();
      }
      
      // Log if there are gaps in the counter sequence
      if (this.#pendingOperations.size > 0) {
        const nextExpected = this.#lastProcessedCounter + 1;
        if (!this.#pendingOperations.has(nextExpected)) {
          const pendingKeys = Array.from(this.#pendingOperations.keys()).sort((a, b) => a - b);
          console.log(`BaseWriteWorker | Waiting for operation with counter ${nextExpected}. Pending operations: ${pendingKeys.join(', ')}`);
        }
      }
    } finally {
      this.#processingOperations = false;
      
      // If the next operations are ready, process them immediately
      if (this.#pendingOperations.has(this.#lastProcessedCounter + 1)) {
        setTimeout(() => this.#processOrderedOperations(), 0);
      }
    }
  }

  /**
   * Handle a batch of write operations
   * @param {Array<PendingOperation>} operations - Array of operations to process
   */
  #handleWrites(operations) {
    // Check if we have operations
    if (!operations || operations.length === 0) {
      return;
    }
    
    // Queue each operation for ordered processing
    let countersToAck = [];
    for (const operation of operations) {
      // Queue the operation
      const queued = this.#queueOrderedOperation(operation);
      if (queued) {
        countersToAck.push(operation.counter);
      }
    }
    
    // Send acknowledgment immediately for all successfully queued operations
    // Note: We don't wait for processing to complete before acknowledging receipt
    if (countersToAck.length > 0) {
      self.postMessage({
        type: 'writeAck',
        counters: countersToAck
      });
    }

    // Start processing the operations in order
    this.#processOrderedOperations();
  }
}
