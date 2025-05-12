
/**
 * @typedef {Object} VFSConfig
 * @property {string} encryptionPassword
 * @property {string} dbName
 * @property {string} workerUrl
 */

// SQLite commit hook that implements asynchronous persistence via Web Worker
export class AsyncWorkerCommitHook {
  /** @type {SQLiteAPI} */ #sqlite3 = null;

  // The SQLiteVFS module, for accessing SQLite functions and the WASM memory
  /** @type {object} */ #module = null;

  // Worker for handling persistence operations
  /** @type {Worker} */ #worker = null;

  // Buffer for initial data loaded by the worker
  /** @type {ArrayBuffer} */ #initialData = null;

  /** @type {Promise<boolean>} */ #isReadyPromise = null;

  #db = null;

  #syncCount = 0;
  #syncDuration = 0;

  _debouncedSyncToWorker = this.debounce(this._syncToWorker.bind(this), 25);

  /**
   * @param {SQLiteAPI} sqlite3 - SQLite API instance
   * @param {object} module 
   * @param {VFSConfig} workerConfig - Optional encryption key and configuration
   */
  constructor(sqlite3, module, workerConfig) {
    this.#sqlite3 = sqlite3;
    this.#module = module;
    this.#isReadyPromise = this._init(workerConfig);
  }

  async isReady() {
    return this.#isReadyPromise;
  }

  useDatabase(db) {
    // store the SQLite database reference for serialization
    this.#db = db;
  }
  /**
   * returns the initial data loaded by the worker
   * @returns {Uint8Array|null} - The initial data buffer or null if not set
   */
  popInitialData() {  
    if (this.#initialData && this.#initialData.byteLength > 0) {
      const data = this.#initialData;
      this.#initialData = null;
      return new Uint8Array(data);
    }
    return null;
  }

  commitHook() {
    this._debouncedSyncToWorker();
  }

  /**
   * @param {VFSConfig} config - Optional encryption key and configuration
   */
  async _init(config) {
    try {
      // Create worker for persistence operations
      this.#worker = new Worker(config.workerUrl, { type: 'module' });

      // Set up message handler for worker
      const initPromise = new Promise((resolve, reject) => {
        const messageHandler = (event) => {
          const msg = event.data;
          if (msg.type === 'initComplete') {
            // Store the initial data buffer for later use when opening the SQLite database
            this.#initialData = msg.fileData.buffer;
            console.log(`CommitHook | Worker initialized with existing file data (${this.#initialData.byteLength} bytes)`);
            this.#worker.removeEventListener('message', messageHandler);
            
            resolve(true);
          } else if (msg.type === 'error') {
            reject(new Error(msg.message));
          }
        };
        
        this.#worker.addEventListener('message', messageHandler);
      });

      // Initialize the worker
      this.#worker.postMessage({
        type: 'init',
        config: config
      });

      return await initPromise;
    } catch (e) {
      console.error("SyncMemoryProxyAsyncWorkerVFS | Failed to initialize worker:", e);
      return false;
    }
  }

  debounce(func, wait = 300) {
    let timeout;
    
    return function executedFunction(...args) {
      const later = () => {
        clearTimeout(timeout);
        func(...args);
      };
      
      clearTimeout(timeout);
      timeout = setTimeout(later, wait);
    };
  }

  /**
   * Send current database state to the worker
   */
  _syncToWorker() {
    console.log("Syncing database state to worker...");
    const start = performance.now();
    for (let db of this.#sqlite3.serialize(this.#db)) {
      // Create a copy of the database to send to the worker
      const dbCopy = new Uint8Array(db.byteLength);
      dbCopy.set(new Uint8Array(db, 0, db.byteLength));
      
      // Send the entire database along with the operations log
      this.#worker.postMessage({
        type: 'sync',
        databaseState: dbCopy
      }, [dbCopy.buffer]);
    }

    const end = performance.now();
    this.#syncCount++;
    this.#syncDuration += (end - start);
    console.log(`SyncWorker | Sync completed in ${(end - start).toFixed(1)} ms. Total sync count: ${this.#syncCount}, total duration: ${this.#syncDuration.toFixed(1)} ms`);
  }
}
