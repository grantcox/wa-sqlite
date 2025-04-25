// Copyright 2024 Roy T. Hashimoto. All Rights Reserved.
import { FacadeVFS } from '../FacadeVFS.js';
import * as VFS from '../VFS.js';
import { WebLocksMixin } from '../WebLocksMixin.js';

// Options for navigator.locks.request().
/** @type {LockOptions} */ const SHARED = { mode: 'shared' };
/** @type {LockOptions} */ const POLL_SHARED = { ifAvailable: true, mode: 'shared' };
/** @type {LockOptions} */ const POLL_EXCLUSIVE = { ifAvailable: true, mode: 'exclusive' };

// Default number of transactions between flushing the OPFS file and
// reclaiming free offsets. Used only when synchronous! = 'full'.
const DEFAULT_FLUSH_INTERVAL = 64;

// Used only for debug logging.
const contextId = Math.random().toString(36).slice(2);

/**
 * @typedef {Object} Transaction
 * @property {number} txId
 * @property {Map<number, { fileOffset: number, size: number, iv: Uint8Array, digest: Uint32Array }>} [offsets]
 * @property {number} [fileSize]
 * @property {number} [oldestTxId]
 * @property {number[]} [reclaimable]
 */

/**
 * @typedef {Object} AccessRequest
 * @property {boolean} exclusive
 */

/**
 * @typedef {Object} OffsetMetadata
 * @property {number} i - Logical iOffset
 * @property {number} o - Physical fileOffset
 * @property {number} s - Size of encrypted data
 * @property {Uint8Array} iv - Initialization vector for decryption
 */

class File {
  /** @type {string} */ path;
  /** @type {number} */ flags;
  /** @type {FileSystemSyncAccessHandle} */ accessHandle;

  // Members below are only used for SQLITE_OPEN_MAIN_DB.

  /** @type {number} */ fileSize; // virtual file size exposed to SQLite

  /** @type {IDBDatabase} */ idb;

  /** @type {Transaction} */ viewTx; // last transaction incorporated
  /** @type {function?} */ viewReleaser;

  /** @type {BroadcastChannel} */ broadcastChannel;
  /** @type {(Transaction|AccessRequest)[]} */ broadcastReceived;

  /** @type {Map<number, {fileOffset: number, size: number, iv: Uint8Array}>} */ mapOffsets;
  /** @type {Map<number, Transaction>} */ mapTxToPending;

  /** @type {number} */ lockState;
  /** @type {{read?: function, write?: function, reserved?: function, hint?: function}} */ locks;

  /** @type {AbortController} */ abortController;

  /** @type {Transaction?} */ txActive; // transaction in progress
  /** @type {number} */ txRealFileSize; // physical file size
  /** @type {boolean} */ txIsOverwrite; // VACUUM in progress
  /** @type {boolean} */ txWriteHint;

  /** @type {'full'|'normal'} */ synchronous;
  /** @type {number} */ flushInterval;

  /**
   * @param {string} pathname 
   * @param {number} flags 
   */
  constructor(pathname, flags) {
    this.path = pathname;
    this.flags = flags;
  }

  /**
   * @param {string} pathname 
   * @param {number} flags
   * @returns 
   */
  static async create(pathname, flags) {
    const file = new File(pathname, flags);

    const create = !!(flags & VFS.SQLITE_OPEN_CREATE);
    const [directory, filename] = await getPathComponents(pathname, create);
    const handle = await directory.getFileHandle(filename, { create });
    // @ts-ignore
    file.accessHandle = await handle.createSyncAccessHandle({ mode: 'readwrite-unsafe' });

    if (flags & VFS.SQLITE_OPEN_MAIN_DB) {
      file.idb = await new Promise((resolve, reject) => {
        const request = indexedDB.open(pathname);
        request.onupgradeneeded = () => {
          const db = request.result;
          db.createObjectStore('offsets', { keyPath: 'i' });
          db.createObjectStore('pending', { keyPath: 'txId'});
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      });
    }
    return file;
  }
}

export class OPFSPermutedEncryptedVFS extends FacadeVFS {
  /** @type {Map<number, File>} */ #mapIdToFile = new Map();
  #lastError = null;
  /** @type {CryptoKey} */ #encryptionKey;

  log = null; // (...args) => console.debug(contextId, ...args);

  /**
   * @param {string} name 
   * @param {*} module
   * @param {{key: CryptoKey}} options - The encryption key to use
   * @returns 
   */
  static async create(name, module, options) {
    const { key } = options;
    const vfs = new OPFSPermutedEncryptedVFS(name, module);
    vfs.#encryptionKey = key;
    await vfs.isReady();
    return vfs;
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
    
    // Return both the encrypted data and the IV
    return {
      encryptedData: new Uint8Array(encryptedBuffer),
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
    return new Uint8Array(decryptedBuffer);
  }

  /**
   * @param {string?} zName 
   * @param {number} fileId 
   * @param {number} flags 
   * @param {DataView} pOutFlags 
   * @returns {Promise<number>}
   */
  async jOpen(zName, fileId, flags, pOutFlags) {
    /** @type {(() => void)[]} */ const onFinally = [];
    try {
      const url = new URL(zName || Math.random().toString(36).slice(2), 'file://');
      const path = url.pathname;

      const file = await File.create(path, flags);
      if (flags & VFS.SQLITE_OPEN_MAIN_DB) {
        file.fileSize = 0;
        file.viewTx = { txId: 0 };
        file.broadcastChannel = new BroadcastChannel(`permuted:${path}`);
        file.broadcastReceived = [];
        file.mapOffsets = new Map();
        file.mapTxToPending = new Map();
        file.lockState = VFS.SQLITE_LOCK_NONE;
        file.locks = {};
        file.abortController = new AbortController();
        file.txIsOverwrite = false;
        file.txActive = null;
        file.synchronous = 'full';
        file.flushInterval = DEFAULT_FLUSH_INTERVAL;

        // Take the write lock so no other connection changes state
        // during our initialization.
        await this.#lock(file, 'write');
        onFinally.push(() => file.locks.write());

        // Load the initial offset map from the database.
        const tx = file.idb.transaction(['offsets', 'pending']);
        const offsets = await idbX(tx.objectStore('offsets').getAll());
        
        // Find the maximum offset to determine the logical file size
        if (offsets.length > 0) {
          const maxOffset = Math.max(...offsets.map(o => o.i));
          const correspondingEntry = offsets.find(o => o.i === maxOffset);
          // Use the entry with maximum logical offset to determine file size
          file.fileSize = maxOffset + correspondingEntry.s;
        }

        // Incorporate the offset map data.
        for (const { i, o, s, iv } of offsets) {
          file.mapOffsets.set(i, { fileOffset: o, size: s, iv });
        }

        // Incorporate pending transactions.
        try {
          /** @type {Transaction[]} */
          const transactions = await idbX(tx.objectStore('pending').getAll());
          for (const transaction of transactions) {
            // Verify checksums for all offsets in this transaction.
            for (const [iOffset, { fileOffset, size, iv, digest }] of transaction.offsets) {
              // Read the encrypted data
              const encryptedData = new Uint8Array(size);
              file.accessHandle.read(encryptedData, { at: fileOffset });
              
              // Decrypt the data
              const decryptedData = await this.#decryptData(encryptedData, iv);
              
              // Verify checksum on the decrypted data
              if (checksum(decryptedData).some((v, i) => v !== digest[i])) {
                throw Object.assign(new Error('checksum error'), { txId: transaction.txId });
              }
            }
            this.#acceptTx(file, transaction);
            file.viewTx = transaction;
          }
        } catch (e) {
          if (e.message === 'checksum error') {
            console.warn(`Checksum error, removing tx ${e.txId}+`)
            const tx = file.idb.transaction('pending', 'readwrite');
            const txCommit = new Promise((resolve, reject) => {
              tx.oncomplete = resolve;
              tx.onabort = () => reject(tx.error);
            });
            const range = IDBKeyRange.lowerBound(e.txId);
            tx.objectStore('pending').delete(range);
            tx.commit();
            await txCommit;
          } else {
            throw e;
          }
        }

        // Publish our view of the database. This prevents other connections
        // from overwriting file data we still need.
        await this.#setView(file, file.viewTx);

        // Listen for broadcasts. Messages are cached until the database
        // is unlocked.
        file.broadcastChannel.addEventListener('message', event => {
          file.broadcastReceived.push(event.data);
          if (file.lockState === VFS.SQLITE_LOCK_NONE) {
            this.#processBroadcasts(file);
          }
        });

        // Connections usually hold this shared read lock so they don't
        // acquire and release it for every transaction. The only time
        // it is released is when a connection wants to VACUUM, which
        // it signals with a broadcast message.
        await this.#lock(file, 'read', SHARED)
      }

      pOutFlags.setInt32(0, flags, true);
      this.#mapIdToFile.set(fileId, file);
      return VFS.SQLITE_OK;
    } catch (e) {
      this.#lastError = e;
      return VFS.SQLITE_CANTOPEN;
    } finally {
      while (onFinally.length) {
        await onFinally.pop()();
      }
    }
  }

  /**
   * @param {string} zName 
   * @param {number} syncDir 
   * @returns {Promise<number>}
   */
  async jDelete(zName, syncDir) {
    try {
      const url = new URL(zName, 'file://');
      const pathname = url.pathname;
   
      const [directoryHandle, name] = await getPathComponents(pathname, false);
      const result = directoryHandle.removeEntry(name, { recursive: false });
      if (syncDir) {
        await result;
      }
      return VFS.SQLITE_OK;
    } catch (e) {
      return VFS.SQLITE_IOERR_DELETE;
    }
  }

  /**
   * @param {string} zName 
   * @param {number} flags 
   * @param {DataView} pResOut 
   * @returns {Promise<number>}
   */
  async jAccess(zName, flags, pResOut) {
    try {
      const url = new URL(zName, 'file://');
      const pathname = url.pathname;

      const [directoryHandle, dbName] = await getPathComponents(pathname, false);
      await directoryHandle.getFileHandle(dbName, { create: false });
      pResOut.setInt32(0, 1, true);
      return VFS.SQLITE_OK;
    } catch (e) {
      if (e.name === 'NotFoundError') {
        pResOut.setInt32(0, 0, true);
        return VFS.SQLITE_OK;
      }
      this.#lastError = e;
      return VFS.SQLITE_IOERR_ACCESS;
    }
  }

  /**
   * @param {number} fileId 
   * @returns {Promise<number>}
   */
  async jClose(fileId) {
    try {
      const file = this.#mapIdToFile.get(fileId);
      this.#mapIdToFile.delete(fileId);
      file?.accessHandle?.close();

      if (file?.flags & VFS.SQLITE_OPEN_MAIN_DB) {
        file.broadcastChannel.close();
        file.viewReleaser?.();
      }

      if (file?.flags & VFS.SQLITE_OPEN_DELETEONCLOSE) {
        const [directoryHandle, name] = await getPathComponents(file.path, false);
        await directoryHandle.removeEntry(name, { recursive: false });
      }
      return VFS.SQLITE_OK;
    } catch (e) {
      return VFS.SQLITE_IOERR_CLOSE;
    }
  }

  /**
   * @param {number} fileId 
   * @param {Uint8Array} pData 
   * @param {number} iOffset
   * @returns {Promise<number>}
   */
  async jRead(fileId, pData, iOffset) {
    try {
      const file = this.#mapIdToFile.get(fileId);

      let bytesRead = 0;
      if (file.flags & VFS.SQLITE_OPEN_MAIN_DB) {
        file.abortController.signal.throwIfAborted();

        // Look up the data location in the file. Check the offsets in
        // any active write transaction first, then the main map.
        let offsetMetadata;
        
        // Check the active transaction first
        if (file.txActive?.offsets.has(iOffset)) {
          const { fileOffset, size, iv } = file.txActive.offsets.get(iOffset);
          offsetMetadata = { fileOffset, size, iv };
        } 
        // Then check the main offset map
        else if (file.mapOffsets.has(iOffset)) {
          offsetMetadata = file.mapOffsets.get(iOffset);
        }

        if (offsetMetadata) {
          this.log?.(`read at iOffset ${iOffset}, fileOffset ${offsetMetadata.fileOffset}`);
          
          // Read the encrypted data
          const encryptedData = new Uint8Array(offsetMetadata.size);
          file.accessHandle.read(encryptedData, { at: offsetMetadata.fileOffset });
          
          // Decrypt the data
          const decryptedData = await this.#decryptData(encryptedData, offsetMetadata.iv);
          
          // Copy the data to the output buffer
          const length = Math.min(pData.length, decryptedData.length);
          pData.set(decryptedData.subarray(0, length));
          bytesRead = length;
        }
      } else {
        // On Chrome (at least), passing pData to accessHandle.read() is
        // an error because pData is a Proxy of a Uint8Array. Calling
        // subarray() produces a real Uint8Array and that works.
        bytesRead = file.accessHandle.read(pData.subarray(), { at: iOffset });
      }

      if (bytesRead < pData.byteLength) {
        pData.fill(0, bytesRead);
        return VFS.SQLITE_IOERR_SHORT_READ;
      }
      return VFS.SQLITE_OK;
    } catch (e) {
      this.#lastError = e;
      return VFS.SQLITE_IOERR_READ;
    }
  }

  /**
   * @param {number} fileId 
   * @param {Uint8Array} pData 
   * @param {number} iOffset
   * @returns {Promise<number>}
   */
  async jWrite(fileId, pData, iOffset) {
    try {
      const file = this.#mapIdToFile.get(fileId);

      if (file.flags & VFS.SQLITE_OPEN_MAIN_DB) {
        file.abortController.signal.throwIfAborted();

        // The first write begins a transaction. Note that xLock/xUnlock
        // is not a good way to determine transaction boundaries because
        // PRAGMA locking_mode can change the behavior.
        if (!file.txActive) {
          this.#beginTx(file);
        }

        // Choose the physical offset in the file to write this data
        let fileOffset;
        
        if (file.txIsOverwrite) {
          // For VACUUM, use the identity mapping to write data
          // at its canonical offset.
          fileOffset = iOffset;
        } else if (file.txActive.offsets.has(iOffset)) {
          // This offset has already been written in this transaction.
          // Use the same physical location.
          fileOffset = file.txActive.offsets.get(iOffset).fileOffset;
          this.log?.(`overwrite at iOffset ${iOffset}, fileOffset ${fileOffset}`);
        } else {
          // Write to the end of the file.
          fileOffset = file.txRealFileSize;
          this.log?.(`append at iOffset ${iOffset}, fileOffset ${fileOffset}`);
        }

        // Encrypt the data before writing
        const { encryptedData, iv } = await this.#encryptData(pData.subarray());
        const encryptedSize = encryptedData.byteLength;
        
        // Write the encrypted data
        file.accessHandle.write(encryptedData, { at: fileOffset });

        // Update the transaction.
        file.txActive.offsets.set(iOffset, {
          fileOffset: fileOffset,
          size: encryptedSize,
          iv,
          digest: checksum(pData.subarray()) // Checksum of unencrypted data
        });
        
        // Update the file size to include this write if needed
        file.txActive.fileSize = Math.max(file.txActive.fileSize, iOffset + pData.byteLength);

        // Track the actual file size.
        file.txRealFileSize = Math.max(file.txRealFileSize, fileOffset + encryptedSize);
      } else {
        // On Chrome (at least), passing pData to accessHandle.write() is
        // an error because pData is a Proxy of a Uint8Array. Calling
        // subarray() produces a real Uint8Array and that works.
        file.accessHandle.write(pData.subarray(), { at: iOffset });
      }
      return VFS.SQLITE_OK;
    } catch (e) {
      this.#lastError = e;
      return VFS.SQLITE_IOERR_WRITE;
    }
  }

  /**
   * @param {number} fileId 
   * @param {number} iSize 
   * @returns {number}
   */
  jTruncate(fileId, iSize) {
    try {
      const file = this.#mapIdToFile.get(fileId);
      if ((file.flags & VFS.SQLITE_OPEN_MAIN_DB) && !file.txIsOverwrite) {
        file.abortController.signal.throwIfAborted();
        if (!file.txActive) {
          this.#beginTx(file);
        }
        file.txActive.fileSize = iSize;

        // When truncating, any offset mapping beyond the new size is no longer needed
        // Note: We can't reclaim the space in the physical file yet
        for (const [offset] of file.txActive.offsets) {
          if (offset >= iSize) {
            file.txActive.offsets.delete(offset);
          }
        }
        return VFS.SQLITE_OK;
      }
      file.accessHandle.truncate(iSize);
      return VFS.SQLITE_OK;
    } catch (e) {
      console.error(e);
      this.lastError = e;
      return VFS.SQLITE_IOERR_TRUNCATE;
    }
  }

  /**
   * @param {number} fileId 
   * @param {number} flags 
   * @returns {number}
   */
  jSync(fileId, flags) {
    try {
      // Main DB sync is handled by SQLITE_FCNTL_SYNC.
      const file = this.#mapIdToFile.get(fileId);
      if (!(file.flags & VFS.SQLITE_OPEN_MAIN_DB)) {
        file.accessHandle.flush();
      }
      return VFS.SQLITE_OK;
    } catch (e) {
      this.#lastError = e;
      return VFS.SQLITE_IOERR_FSYNC;
    }
  }

  /**
   * @param {number} fileId 
   * @param {DataView} pSize64 
   * @returns {number}
   */
  jFileSize(fileId, pSize64) {
    try {
      const file = this.#mapIdToFile.get(fileId);

      let size;
      if (file.flags & VFS.SQLITE_OPEN_MAIN_DB) {
        file.abortController.signal.throwIfAborted();
        size = file.txActive?.fileSize ?? file.fileSize;
      } else {
        size = file.accessHandle.getSize();
      }

      pSize64.setBigInt64(0, BigInt(size), true);
      return VFS.SQLITE_OK;
    } catch (e) {
      this.#lastError = e;
      return VFS.SQLITE_IOERR_FSTAT;
    }
  }

  /**
   * @param {number} fileId 
   * @param {number} lockType 
   * @returns {Promise<number>}
   */
  async jLock(fileId, lockType) {
    const file = this.#mapIdToFile.get(fileId);
    if (lockType <= file.lockState) return VFS.SQLITE_OK;
    switch (lockType) {
      case VFS.SQLITE_LOCK_SHARED:
        if (file.txWriteHint) {
            // xFileControl() has hinted that this transaction will
            // write. Acquire the hint lock, which is required to reach
            // the RESERVED state.
            if (!await this.#lock(file, 'hint')) {
              return VFS.SQLITE_BUSY;
            }
        }

        if (!file.locks.read) {
          // Reacquire lock if it was released by a broadcast request.
          await this.#lock(file, 'read', SHARED);
        }
        break;
      case VFS.SQLITE_LOCK_RESERVED:
        // Ideally we should already have the hint lock, but if not
        // poll for it here.
        if (!file.locks.hint && !await this.#lock(file, 'hint', POLL_EXCLUSIVE)) {
          return VFS.SQLITE_BUSY;
        }

        if (!await this.#lock(file, 'reserved', POLL_EXCLUSIVE)) {
          file.locks.hint();
          return VFS.SQLITE_BUSY;
        }

        // In order to write, our view of the database must be up to date.
        // To check this, first fetch all transactions in IndexedDB equal to
        // or greater than our view.
        const tx = file.idb.transaction(['pending']);
        const range = IDBKeyRange.lowerBound(file.viewTx.txId);

        /** @type {Transaction[]} */
        const entries = await idbX(tx.objectStore('pending').getAll(range));

        // Ideally the fetched list of transactions should contain one
        // entry matching our view. If not then our view is out of date.
        if (entries.length && entries.at(-1).txId > file.viewTx.txId) {
          // There are newer transactions in IndexedDB that we haven't
          // seen via broadcast. Ensure that they are incorporated on unlock,
          // and force the application to retry.
          file.broadcastReceived.push(...entries);
          file.locks.reserved();
          return VFS.SQLITE_BUSY
        }
        break;
      case VFS.SQLITE_LOCK_EXCLUSIVE:
        await this.#lock(file, 'write');
        break;
    }
    file.lockState = lockType;
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} fileId 
   * @param {number} lockType 
   * @returns {number}
   */
  jUnlock(fileId, lockType) {
    const file = this.#mapIdToFile.get(fileId);
    if (lockType >= file.lockState) return VFS.SQLITE_OK;
    switch (lockType) {
      case VFS.SQLITE_LOCK_SHARED:
        file.locks.write?.();
        file.locks.reserved?.();
        file.locks.hint?.();
        break;
      case VFS.SQLITE_LOCK_NONE:
        // Don't release the read lock here. It will be released on demand
        // when a broadcast notifies us that another connections wants to
        // VACUUM.
        this.#processBroadcasts(file);
        file.locks.write?.();
        file.locks.reserved?.();
        file.locks.hint?.();
        break;
    }
    file.lockState = lockType;
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} fileId
   * @param {DataView} pResOut 
   * @returns {Promise<number>}
   */
  async jCheckReservedLock(fileId, pResOut) {
    try {
      const file = this.#mapIdToFile.get(fileId);
      if (await this.#lock(file, 'reserved', POLL_SHARED)) {
        // This looks backwards, but if we get the lock then no one
        // else had it.
        pResOut.setInt32(0, 0, true);
        file.locks.reserved();
      } else {
        pResOut.setInt32(0, 1, true);
      }
      return VFS.SQLITE_OK;
    } catch (e) {
      console.error(e);
      this.lastError = e;
      return VFS.SQLITE_IOERR_LOCK;
    }
  }

  /**
   * @param {number} fileId
   * @param {number} op
   * @param {DataView} pArg
   * @returns {Promise<number>}
   */
  async jFileControl(fileId, op, pArg) {
    try {
      const file = this.#mapIdToFile.get(fileId);
      switch (op) {
        case VFS.SQLITE_FCNTL_PRAGMA:
          const key = cvtString(pArg, 4);
          const value = cvtString(pArg, 8);
          this.log?.('xFileControl', file.path, 'PRAGMA', key, value);
          switch (key.toLowerCase()) {
            case 'synchronous':
              // This VFS only recognizes 'full' and not 'full'.
              if (value) {
                switch (value.toLowerCase()) {
                  case 'full':
                  case '2':
                  case 'extra':
                  case '3':
                    file.synchronous = 'full';
                    break;
                  default:
                    file.synchronous = 'normal';
                    break;
                }
              }
              break;
            case 'flush_interval':
              if (value) {
                const interval = Number(value);
                if (interval > 0) {
                  file.flushInterval = Number(value);
                } else {
                  return VFS.SQLITE_ERROR;
                }
              } else {
                // Report current value.
                const buffer = new TextEncoder().encode(file.flushInterval.toString());
                const s = this._module._sqlite3_malloc64(buffer.byteLength + 1);
                new Uint8Array(this._module.HEAPU8.buffer, s, buffer.byteLength + 1)
                  .fill(0)
                  .set(buffer);

                pArg.setUint32(0, s, true);
                return VFS.SQLITE_OK;
              }
              break;
            case 'write_hint':
              return this.jFileControl(fileId, WebLocksMixin.WRITE_HINT_OP_CODE, null);
            }
          break;
        case VFS.SQLITE_FCNTL_BEGIN_ATOMIC_WRITE:
          this.log?.('xFileControl', 'BEGIN_ATOMIC_WRITE', file.path);
          return VFS.SQLITE_OK;
        case VFS.SQLITE_FCNTL_COMMIT_ATOMIC_WRITE:
          this.log?.('xFileControl', 'COMMIT_ATOMIC_WRITE', file.path);
          return VFS.SQLITE_OK;
        case VFS.SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE:
          this.log?.('xFileControl', 'ROLLBACK_ATOMIC_WRITE', file.path);
          this.#rollbackTx(file);
          return VFS.SQLITE_OK;
        case VFS.SQLITE_FCNTL_OVERWRITE:
          // This is a VACUUM.
          this.log?.('xFileControl', 'OVERWRITE', file.path);
          await this.#prepareOverwrite(file);
          break;
        case VFS.SQLITE_FCNTL_COMMIT_PHASETWO:
          // Finish any transaction. Note that the transaction may not
          // exist if there is a BEGIN IMMEDIATE...COMMIT block that
          // does not actually call xWrite.
          this.log?.('xFileControl', 'COMMIT_PHASETWO', file.path);
          if (file.txActive) {
            await this.#commitTx(file);
          }
          break;
        case WebLocksMixin.WRITE_HINT_OP_CODE:
          file.txWriteHint = true;
          break;
      }
    } catch (e) {
      this.#lastError = e;
      return VFS.SQLITE_IOERR;
    }
    return VFS.SQLITE_NOTFOUND;
  }

  /**
   * @param {number} fileId
   * @returns {number|Promise<number>}
   */
  jDeviceCharacteristics(fileId) {
    return 0
    | VFS.SQLITE_IOCAP_BATCH_ATOMIC
    | VFS.SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN;
  }

  /**
   * @param {Uint8Array} zBuf 
   * @returns {number}
   */
  jGetLastError(zBuf) {
    if (this.#lastError) {
      console.error(this.#lastError);
      const outputArray = zBuf.subarray(0, zBuf.byteLength - 1);
      const { written } = new TextEncoder().encodeInto(this.#lastError.message, outputArray);
      zBuf[written] = 0;
    }
    return VFS.SQLITE_OK
  }

  /**
   * Acquire one of the database file internal Web Locks.
   * @param {File} file 
   * @param {'read'|'write'|'reserved'|'hint'} name 
   * @param {LockOptions} options 
   * @returns {Promise<boolean>}
   */
  #lock(file, name, options = {}) {
    return new Promise(resolve => {
      const lockName = `${file.path}@@${name}`;
      navigator.locks.request(lockName, options, lock => {
        if (lock) {
          return new Promise(release => {
            file.locks[name] = () => {
              release();
              file.locks[name] = null;
            };
            resolve(true);
          });
        } else {
          file.locks[name] = null;
          resolve(false);
        }
      }).catch(e => {
        if (e.name !== 'AbortError') throw e;
      });
    });
  }

  /**
   * @param {File} file 
   * @param {Transaction} tx 
   */
  async #setView(file, tx) {
    // Publish our view of the database with a lock name that includes
    // the transaction id. As long as we hold the lock, no other connection
    // will overwrite data we still need.
    file.viewTx = tx;
    const lockName = `${file.path}@@[${tx.txId}]`;
    const newReleaser = await new Promise(resolve => {
      navigator.locks.request(lockName, SHARED, lock => {
        return new Promise(release => {
          resolve(release);
        });
      });
    });

    // The new lock is acquired so release the old one.
    file.viewReleaser?.();
    file.viewReleaser = newReleaser;
  }

  /**
   * Handle prevously received messages from other connections.
   * @param {File} file 
   */
  #processBroadcasts(file) {
    // Sort transaction messages by id. Move other messages to the front.
    // @ts-ignore
    file.broadcastReceived.sort((a, b) => (a.txId ?? -1) - (b.txId ?? -1));

    let nHandled = 0;
    let newTx = file.viewTx;
    for (const message of file.broadcastReceived) {
      if (Object.hasOwn(message, 'txId')) {
        const messageTx = /** @type {Transaction} */ (message)
        if (messageTx.txId <= newTx.txId) {
          // This transaction is already incorporated into our view.
        } else if (messageTx.txId === newTx.txId + 1) {
          // This is the next expected transaction.
          this.log?.(`accept tx ${messageTx.txId}`);
          this.#acceptTx(file, messageTx);
          newTx = messageTx;
        } else {
          // There is a gap in the transaction sequence.
          console.warn(`missing tx ${newTx.txId + 1} (got ${messageTx.txId})`);
          break;
        }
      } else if (Object.hasOwn(message, 'exclusive')) {
        // Release the read lock if we have it.
        this.log?.('releasing read lock');
        console.assert(file.lockState === VFS.SQLITE_LOCK_NONE);
        file.locks.read?.();
      }
      nHandled++;
    }

    // Remove handled messages from the list.
    file.broadcastReceived.splice(0, nHandled);

    // Tell other connections about a change in our view.
    if (newTx.txId > file.viewTx.txId) {
      // No need to await here.
      this.#setView(file, newTx);
    }
  }

  /**
   * @param {File} file 
   * @param {Transaction} message 
   */
  #acceptTx(file, message) {
    // Add list of offsets made obsolete by this transaction.
    // Note: In this version we don't immediately reclaim space
    message.reclaimable = [];

    // Update offset mapping with transaction data.
    for (const [iOffset, { fileOffset, size, iv }] of message.offsets) {
      // If we have an existing mapping for this offset, 
      // remember it's original location (though we can't reclaim it yet)
      if (file.mapOffsets.has(iOffset)) {
        message.reclaimable.push(file.mapOffsets.get(iOffset).fileOffset);
      }
      
      // Update the mapping for this offset
      file.mapOffsets.set(iOffset, { fileOffset, size, iv });
    }

    // Remove mappings for truncated data.
    if (message.fileSize < file.fileSize) {
      // Find all offsets that are now beyond the end of the file
      for (const [iOffset, metadata] of file.mapOffsets.entries()) {
        if (iOffset >= message.fileSize) {
          message.reclaimable.push(metadata.fileOffset);
          file.mapOffsets.delete(iOffset);
        }
      }
    }

    file.fileSize = message.fileSize;
    file.mapTxToPending.set(message.txId, message);
    
    if (message.oldestTxId) {
      // Finalize pending transactions that are no longer needed.
      for (const tx of file.mapTxToPending.values()) {
        if (tx.txId > message.oldestTxId) break;
        
        // We log the offsets that could be reclaimed in the future,
        // but we don't actually try to reuse the space - we just append
        // to the end of the file until a VACUUM is performed.
        for (const offset of tx.reclaimable) {
          this.log?.(`could reclaim offset ${offset} (will be handled by VACUUM)`);
        }
        
        file.mapTxToPending.delete(tx.txId);
      }
    }
  }

  /**
   * @param {File} file 
   */
  #beginTx(file) {
    // Start a new transaction.
    file.txActive = {
      txId: file.viewTx.txId + 1,
      offsets: new Map(),
      fileSize: file.fileSize
    };
    file.txRealFileSize = file.accessHandle.getSize();
    this.log?.(`begin transaction ${file.txActive.txId}`);
  }

  /**
   * @param {File} file 
   */
  async #commitTx(file) {
    // Determine whether to finalize pending transactions, i.e. transfer
    // them to the IndexedDB offsets store.
    if (file.synchronous === 'full' ||
        file.txIsOverwrite ||
        (file.txActive.txId % file.flushInterval) === 0) {
      file.txActive.oldestTxId = await this.#getOldestTxInUse(file);
    }

    const tx = file.idb.transaction(
      ['offsets', 'pending'],
      'readwrite',
      { durability: file.synchronous === 'full' ? 'strict' : 'relaxed'});

    if (file.txActive.oldestTxId) {
      // Ensure that all pending data is safely on storage.
      if (file.txIsOverwrite) {
        file.accessHandle.truncate(file.txActive.fileSize);
      }
      file.accessHandle.flush();
      
      // Transfer offset mappings to the offsets store for all pending
      // transactions that are no longer in use.
      const offsetsStore = tx.objectStore('offsets');
      for (const tx of file.mapTxToPending.values()) {
        if (tx.txId > file.txActive.oldestTxId) break;

        for (const [iOffset, { fileOffset, size, iv }] of tx.offsets) {
          // Store the offset metadata in the offsets store
          offsetsStore.put({ i: iOffset, o: fileOffset, s: size, iv });
        }
      }

      // Delete pending store entries that are no longer needed.
      tx.objectStore('pending')
        .delete(IDBKeyRange.upperBound(file.txActive.oldestTxId));
    }

    // Publish the transaction via broadcast and IndexedDB.
    this.log?.(`commit transaction ${file.txActive.txId}`);
    tx.objectStore('pending').put(file.txActive);

    const txComplete = new Promise((resolve, reject) => {
      const message = file.txActive;
      tx.oncomplete = () => {
        file.broadcastChannel.postMessage(message);
        resolve();
      };
      tx.onabort = () => {
        file.abortController.abort();
        reject(tx.error);
      };
      tx.commit();
    });

    if (file.synchronous === 'full') {
      await txComplete;
    }

    // Advance our own view. Even if we received our own broadcasts (we
    // don't), we want our view to be updated synchronously.
    this.#acceptTx(file, file.txActive);
    this.#setView(file, file.txActive);
    file.txActive = null;
    file.txWriteHint = false;

    if (file.txIsOverwrite) {
      // Wait until all connections have seen the transaction.
      while (file.viewTx.txId !== await this.#getOldestTxInUse(file)) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }

      // Downgrade the exclusive read lock to a shared lock.
      file.locks.read();
      await this.#lock(file, 'read', SHARED);

      file.txIsOverwrite = false;
    }
  }

  /**
   * @param {File} file 
   */
  #rollbackTx(file) {
    // Nothing to do here - we just abandon the transaction
    this.log?.(`rollback transaction ${file.txActive.txId}`);
    file.txActive = null;
    file.txWriteHint = false;
  }

  /**
   * @param {File} file 
   */
  async #prepareOverwrite(file) {
    // Get an exclusive read lock to prevent other connections from
    // seeing the database in an inconsistent state.
    file.locks.read?.();
    if (!await this.#lock(file, 'read', POLL_EXCLUSIVE)) {
      // We didn't get the read lock because other connections have
      // it. Notify them that we want the lock and wait.
      const lockRequest = this.#lock(file, 'read');
      file.broadcastChannel.postMessage({ exclusive: true });
      await lockRequest;
    }

    // Create a intermediate transaction to copy all current data to
    // new locations past the end of the file.
    file.txActive = {
      txId: file.viewTx.txId + 1,
      offsets: new Map(),
      fileSize: file.fileSize
    };

    // Keep track of where we're writing in the file
    let nextFileOffset = file.accessHandle.getSize();

    // Process all offsets in the current mapping
    for (const [iOffset, { fileOffset, size, iv }] of file.mapOffsets.entries()) {
      // Read the encrypted data
      const encryptedData = new Uint8Array(size);
      if (file.accessHandle.read(encryptedData, { at: fileOffset }) !== size) {
        throw new Error('Failed to read data');
      }
      
      // Decrypt it
      const decryptedData = await this.#decryptData(encryptedData, iv);
      
      // Re-encrypt it with a new IV
      const { encryptedData: newEncryptedData, iv: newIv } = await this.#encryptData(decryptedData);
      
      // Write it at the end of the file
      if (file.accessHandle.write(newEncryptedData, { at: nextFileOffset }) !== newEncryptedData.byteLength) {
        throw new Error('Failed to write data');
      }

      // Record this in the transaction
      file.txActive.offsets.set(iOffset, {
        fileOffset: nextFileOffset,
        size: newEncryptedData.byteLength,
        iv: newIv,
        digest: checksum(decryptedData)
      });
      
      // Update our position tracker
      nextFileOffset += newEncryptedData.byteLength;
    }
    
    file.accessHandle.flush();
    
    // Publish transaction for others.
    file.broadcastChannel.postMessage(file.txActive);
    const tx = file.idb.transaction('pending', 'readwrite');
    const txComplete = new Promise((resolve, reject) => {
      tx.oncomplete = resolve;
      tx.onabort = () => reject(tx.error);
    });
    tx.objectStore('pending').put(file.txActive);
    tx.commit();
    await txComplete;

    // Incorporate the transaction into our view.
    this.#acceptTx(file, file.txActive);
    this.#setView(file, file.txActive);
    file.txActive = null;

    // Now all data has been copied to new locations.
    // The VACUUM operation will now reconstruct the database
    // at its canonical offsets. After that the file can be truncated.

    // This flag tells xWrite to write data at its canonical offset.
    file.txIsOverwrite = true;
  }

  /**
   * @param {File} file 
   * @returns {Promise<number>}
   */
  async #getOldestTxInUse(file) {
    // Each connection holds a shared Web Lock with a name that encodes
    // the latest transaction it knows about. We can find the oldest
    // transaction by listing the those locks and extracting the earliest
    // transaction id.
    const TX_LOCK_REGEX = /^(.*)@@\[(\d+)\]$/;
    let oldestTxId = file.viewTx.txId;
    const locks = await navigator.locks.query();
    for (const { name } of locks.held) {
      const m = TX_LOCK_REGEX.exec(name);
      if (m && m[1] === file.path) {
        oldestTxId = Math.min(oldestTxId, Number(m[2]));
      }
    }
    return oldestTxId;
  }
}

/**
 * Wrap IndexedDB request with a Promise.
 * @param {IDBRequest} request 
 * @returns 
 */
function idbX(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

/**
 * Given a path, return the directory handle and filename.
 * @param {string} path 
 * @param {boolean} create 
 * @returns {Promise<[FileSystemDirectoryHandle, string]>}
 */
async function getPathComponents(path, create) {
  const components = path.split('/');
  const filename = components.pop();
  let directory = await navigator.storage.getDirectory();
  for (const component of components.filter(s => s)) {
    directory = await directory.getDirectoryHandle(component, { create });
  }
  return [directory, filename];
}

/**
 * Extract a C string from WebAssembly memory.
 * @param {DataView} dataView 
 * @param {number} offset 
 * @returns 
 */
function cvtString(dataView, offset) {
  const p = dataView.getUint32(offset, true);
  if (p) {
    const chars = new Uint8Array(dataView.buffer, p);
    return new TextDecoder().decode(chars.subarray(0, chars.indexOf(0)));
  }
  return null;
}

/**
 * Compute a checksum.
 * @param {ArrayBufferView} data 
 * @returns {Uint32Array}
 */
function checksum(data) {
  const array = new Uint32Array(
    data.buffer,
    data.byteOffset,
    data.byteLength / Uint32Array.BYTES_PER_ELEMENT);

  // https://en.wikipedia.org/wiki/Fletcher%27s_checksum
  let h1 = 0;
  let h2 = 0;
  for (const value of array) {
    h1 = (h1 + value) % 4294967295;
    h2 = (h2 + h1) % 4294967295;
  }
  return new Uint32Array([h1, h2]);
}
