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
 * @property {string} [encryptionPassword]
 * @property {string} [dbName]
 */

export class BaseWriteWorker {
  // Order tracking for writes
  #lastProcessedCounter = -1;
  #pendingOperations = new Map();
  #fileData = null;
  #initialized = false;
  
  constructor() {
    // Set up the message handler
    self.onmessage = this.#handleMessage.bind(this);
  }

  /**
   * Initialize worker with configuration
   * @param {VFSConfig} config - Configuration parameters for the worker
   * @returns {Promise<ArrayBuffer>} Initial file data
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
    
    console.log(`BaseWriteWorker | Received ${msg.type} message`);

    switch (msg.type) {
      case 'init':
        try {
          // Initialize the worker
          const initFileData = await this.init(msg.config);
          this.#fileData = initFileData;
          this.#initialized = true;
          self.postMessage({
            type: 'initComplete',
            fileData: initFileData
          });
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
   */
  async #processOrderedOperations() {
    let processedAny = false;
    
    // Continue processing as long as we have sequential operations
    while (this.#pendingOperations.has(this.#lastProcessedCounter + 1)) {
      processedAny = true;
      const nextCounter = this.#lastProcessedCounter + 1;
      const operation = this.#pendingOperations.get(nextCounter);
      
      try {
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
      } catch (error) {
        console.error(`BaseWriteWorker | Error processing operation ${nextCounter}:`, error);
        break;
      }
    }
    
    // Send acknowledgments for all processed operations if we processed any
    if (processedAny) {
      self.postMessage({
        type: 'writeAck',
        upToCounter: this.#lastProcessedCounter
      });

      // Trigger the write queue processing in the subclass
      try {
        await this.processWriteQueue();
      } catch (error) {
        console.error('BaseWriteWorker | Error processing write queue:', error);
      }
    }
    
    // Log if there are gaps in the counter sequence
    if (this.#pendingOperations.size > 0) {
      const nextExpected = this.#lastProcessedCounter + 1;
      if (!this.#pendingOperations.has(nextExpected)) {
        const pendingKeys = Array.from(this.#pendingOperations.keys()).sort((a, b) => a - b);
        console.log(`BaseWriteWorker | Waiting for operation with counter ${nextExpected}. Pending operations: ${pendingKeys.join(', ')}`);
      }
    }
  }

  /**
   * Handle a batch of write operations
   * @param {Array<PendingOperation>} operations - Array of operations to process
   */
  #handleWrites(operations) {
    console.log(`BaseWriteWorker | Received ${operations.length} write operations`);

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
