// Copyright 2024 Roy T. Hashimoto. All Rights Reserved.
import * as VFS from '../src/sqlite-constants.js';

const DEFAULT_SECTOR_SIZE = 512;


// Base class for a VFS.
class Base {
  name;
  mxPathname = 64;
  _module;

  /**
   * @param {string} name 
   * @param {object} module 
   */
  constructor(name, module) {
    this.name = name;
    this._module = module;
  }

  /**
   * @returns {void} 
   */
  close() {
  }

  /**
   * @param {number} pVfs 
   * @param {number} zName 
   * @param {number} pFile 
   * @param {number} flags 
   * @param {number} pOutFlags 
   * @returns {number}
   */
  xOpen(pVfs, zName, pFile, flags, pOutFlags) {
    return VFS.SQLITE_CANTOPEN;
  }

  /**
   * @param {number} pVfs 
   * @param {number} zName 
   * @param {number} syncDir 
   * @returns {number}
   */
  xDelete(pVfs, zName, syncDir) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pVfs 
   * @param {number} zName 
   * @param {number} flags 
   * @param {number} pResOut 
   * @returns {number}
   */
  xAccess(pVfs, zName, flags, pResOut) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pVfs 
   * @param {number} zName 
   * @param {number} nOut 
   * @param {number} zOut 
   * @returns {number}
   */
  xFullPathname(pVfs, zName, nOut, zOut) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pVfs 
   * @param {number} nBuf 
   * @param {number} zBuf 
   * @returns {number}
   */
  xGetLastError(pVfs, nBuf, zBuf) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @returns {number}
   */
  xClose(pFile) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @param {number} pData 
   * @param {number} iAmt 
   * @param {number} iOffset 
   * @returns {number}
   */
  xRead(pFile, pData, iAmt, iOffset) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @param {number} pData 
   * @param {number} iAmt 
   * @param {number} iOffset 
   * @returns {number}
   */
  xWrite(pFile, pData, iAmt, iOffset) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @param {number} size 
   * @returns {number}
   */
  xTruncate(pFile, size) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @param {number} flags 
   * @returns {number}
   */
  xSync(pFile, flags) {
    return VFS.SQLITE_OK;
  }

  /**
   * 
   * @param {number} pFile 
   * @param {number} pSize 
   * @returns {number}
   */
  xFileSize(pFile, pSize) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @param {number} lockType 
   * @returns {number}
   */
  xLock(pFile, lockType) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @param {number} lockType 
   * @returns {number}
   */
  xUnlock(pFile, lockType) {
    return VFS.SQLITE_OK;
  } 

  /**
   * @param {number} pFile 
   * @param {number} pResOut 
   * @returns {number}
   */
  xCheckReservedLock(pFile, pResOut) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @param {number} op 
   * @param {number} pArg 
   * @returns {number}
   */
  xFileControl(pFile, op, pArg) {
    return VFS.SQLITE_NOTFOUND;
  }

  /**
   * @param {number} pFile 
   * @returns {number}
   */
  xSectorSize(pFile) {
    return DEFAULT_SECTOR_SIZE;
  }

  /**
   * @param {number} pFile 
   * @returns {number}
   */
  xDeviceCharacteristics(pFile) {
    return 0;
  }
}

export const FILE_TYPE_MASK = [
  VFS.SQLITE_OPEN_MAIN_DB,
  VFS.SQLITE_OPEN_MAIN_JOURNAL,
  VFS.SQLITE_OPEN_TEMP_DB,
  VFS.SQLITE_OPEN_TEMP_JOURNAL,
  VFS.SQLITE_OPEN_TRANSIENT_DB,
  VFS.SQLITE_OPEN_SUBJOURNAL,
  VFS.SQLITE_OPEN_SUPER_JOURNAL,
  VFS.SQLITE_OPEN_WAL
].reduce((mask, element) => mask | element);


// Convenience base class for a JavaScript VFS.
// The raw xOpen, xRead, etc. function signatures receive only C primitives
// which aren't easy to work with. This class provides corresponding calls
// like jOpen, jRead, etc., which receive JavaScript-friendlier arguments
// such as string, Uint8Array, and DataView.
class FacadeVFS extends Base {
  /**
   * @param {string} name 
   * @param {object} module 
   */
  constructor(name, module) {
    super(name, module);
  }
  
  /**
   * Return the filename for a file id for use by mixins.
   * @param {number} pFile 
   * @returns {string}
   */
  getFilename(pFile) {
    throw new Error('unimplemented');
  }

  /**
   * @param {string?} filename 
   * @param {number} pFile 
   * @param {number} flags 
   * @param {DataView} pOutFlags 
   * @returns {number}
   */
  jOpen(filename, pFile, flags, pOutFlags) {
    return VFS.SQLITE_CANTOPEN;
  }

  /**
   * @param {string} filename 
   * @param {number} syncDir 
   * @returns {number}
   */
  jDelete(filename, syncDir) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {string} filename 
   * @param {number} flags 
   * @param {DataView} pResOut 
   * @returns {number}
   */
  jAccess(filename, flags, pResOut) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {string} filename 
   * @param {Uint8Array} zOut 
   * @returns {number}
   */
  jFullPathname(filename, zOut) {
    // Copy the filename to the output buffer.
    const { read, written } = new TextEncoder().encodeInto(filename, zOut);
    if (read < filename.length) return VFS.SQLITE_IOERR;
    if (written >= zOut.length) return VFS.SQLITE_IOERR;
    zOut[written] = 0;
    return VFS.SQLITE_OK;
  }

  /**
   * @param {Uint8Array} zBuf 
   * @returns {number}
   */
  jGetLastError(zBuf) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @returns {number}
   */
  jClose(pFile) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @param {Uint8Array} pData 
   * @param {number} iOffset 
   * @returns {number}
   */
  jRead(pFile, pData, iOffset) {
    pData.fill(0);
    return VFS.SQLITE_IOERR_SHORT_READ;
  }

  /**
   * @param {number} pFile 
   * @param {Uint8Array} pData 
   * @param {number} iOffset 
   * @returns {number}
   */
  jWrite(pFile, pData, iOffset) {
    return VFS.SQLITE_IOERR_WRITE;
  }

  /**
   * @param {number} pFile 
   * @param {number} size 
   * @returns {number}
   */
  jTruncate(pFile, size) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @param {number} flags 
   * @returns {number}
   */
  jSync(pFile, flags) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @param {DataView} pSize
   * @returns {number}
   */
  jFileSize(pFile, pSize) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @param {number} lockType 
   * @returns {number}
   */
  jLock(pFile, lockType) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @param {number} lockType 
   * @returns {number}
   */
  jUnlock(pFile, lockType) {
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile 
   * @param {DataView} pResOut 
   * @returns {number}
   */
  jCheckReservedLock(pFile, pResOut) {
    pResOut.setInt32(0, 0, true);
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} pFile
   * @param {number} op
   * @param {DataView} pArg
   * @returns {number}
   */
  jFileControl(pFile, op, pArg) {
    return VFS.SQLITE_NOTFOUND;
  }

  /**
   * @param {number} pFile
   * @returns {number}
   */
  jSectorSize(pFile) {
    return super.xSectorSize(pFile);
  }

  /**
   * @param {number} pFile
   * @returns {number}
   */
  jDeviceCharacteristics(pFile) {
    return 0;
  }

  /**
   * @param {number} pVfs 
   * @param {number} zName 
   * @param {number} pFile 
   * @param {number} flags 
   * @param {number} pOutFlags 
   * @returns {number}
   */
  xOpen(pVfs, zName, pFile, flags, pOutFlags) {
    const filename = this.#decodeFilename(zName, flags);
    const pOutFlagsView = this.#makeTypedDataView('Int32', pOutFlags);
    this['log']?.('jOpen', filename, pFile, '0x' + flags.toString(16));
    return this.jOpen(filename, pFile, flags, pOutFlagsView);
  }

  /**
   * @param {number} pVfs 
   * @param {number} zName 
   * @param {number} syncDir 
   * @returns {number}
   */
  xDelete(pVfs, zName, syncDir) {
    const filename = this.UTF8ToString(zName);
    this['log']?.('jDelete', filename, syncDir);
    return this.jDelete(filename, syncDir);
  }

  /**
   * @param {number} pVfs 
   * @param {number} zName 
   * @param {number} flags 
   * @param {number} pResOut 
   * @returns {number}
   */
  xAccess(pVfs, zName, flags, pResOut) {
    const filename = this.UTF8ToString(zName);
    const pResOutView = this.#makeTypedDataView('Int32', pResOut);
    this['log']?.('jAccess', filename, flags);
    return this.jAccess(filename, flags, pResOutView);
  }

  /**
   * @param {number} pVfs 
   * @param {number} zName 
   * @param {number} nOut 
   * @param {number} zOut 
   * @returns {number}
   */
  xFullPathname(pVfs, zName, nOut, zOut) {
    const filename = this.UTF8ToString(zName);
    const zOutArray = this._module.HEAPU8.subarray(zOut, zOut + nOut);
    this['log']?.('jFullPathname', filename, nOut);
    return this.jFullPathname(filename, zOutArray);
  }

  /**
   * @param {number} pVfs 
   * @param {number} nBuf 
   * @param {number} zBuf 
   * @returns {number}
   */
  xGetLastError(pVfs, nBuf, zBuf) {
    const zBufArray = this._module.HEAPU8.subarray(zBuf, zBuf + nBuf);
    this['log']?.('jGetLastError', nBuf);
    return this.jGetLastError(zBufArray);
  }

  /**
   * @param {number} pFile 
   * @returns {number}
   */
  xClose(pFile) {
    this['log']?.('jClose', pFile);
    return this.jClose(pFile);
  }

  /**
   * @param {number} pFile 
   * @param {number} pData 
   * @param {number} iAmt 
   * @param {number} iOffset 
   * @returns {number}
   */
  xRead(pFile, pData, iAmt, iOffset) {
    const pDataArray = this.makeDataArray(pData, iAmt);
    this['log']?.('jRead', pFile, iAmt, iOffset);
    return this.jRead(pFile, pDataArray, iOffset);
  }

  /**
   * @param {number} pFile 
   * @param {number} pData 
   * @param {number} iAmt 
   * @param {number} iOffset 
   * @returns {number}
   */
  xWrite(pFile, pData, iAmt, iOffset) {
    const pDataArray = this.makeDataArray(pData, iAmt);
    this['log']?.('jWrite', pFile, pDataArray, iOffset);
    return this.jWrite(pFile, pDataArray, iOffset);
  }

  /**
   * @param {number} pFile 
   * @param {number} size 
   * @returns {number}
   */
  xTruncate(pFile, size) {
    this['log']?.('jTruncate', pFile, size);
    return this.jTruncate(pFile, size);
  }

  /**
   * @param {number} pFile 
   * @param {number} flags 
   * @returns {number}
   */
  xSync(pFile, flags) {
    this['log']?.('jSync', pFile, flags);
    return this.jSync(pFile, flags);
  }

  /**
   * 
   * @param {number} pFile 
   * @param {number} pSize 
   * @returns {number}
   */
  xFileSize(pFile, pSize) {
    const pSizeView = this.#makeTypedDataView('BigInt64', pSize);
    this['log']?.('jFileSize', pFile);
    return this.jFileSize(pFile, pSizeView);
  }

  /**
   * @param {number} pFile 
   * @param {number} lockType 
   * @returns {number}
   */
  xLock(pFile, lockType) {
    this['log']?.('jLock', pFile, lockType);
    return this.jLock(pFile, lockType);
  }

  /**
   * @param {number} pFile 
   * @param {number} lockType 
   * @returns {number}
   */
  xUnlock(pFile, lockType) {
    this['log']?.('jUnlock', pFile, lockType);
    return this.jUnlock(pFile, lockType);
  } 

  /**
   * @param {number} pFile 
   * @param {number} pResOut 
   * @returns {number}
   */
  xCheckReservedLock(pFile, pResOut) {
    const pResOutView = this.#makeTypedDataView('Int32', pResOut);
    this['log']?.('jCheckReservedLock', pFile);
    return this.jCheckReservedLock(pFile, pResOutView);
  }

  /**
   * @param {number} pFile 
   * @param {number} op 
   * @param {number} pArg 
   * @returns {number}
   */
  xFileControl(pFile, op, pArg) {
    const pArgView = new DataView(
      this._module.HEAPU8.buffer,
      this._module.HEAPU8.byteOffset + pArg);
    this['log']?.('jFileControl', pFile, op, pArgView);
    return this.jFileControl(pFile, op, pArgView);
  }

  /**
   * @param {number} pFile 
   * @returns {number}
   */
  xSectorSize(pFile) {
    this['log']?.('jSectorSize', pFile);
    return this.jSectorSize(pFile);
  }

  /**
   * @param {number} pFile 
   * @returns {number}
   */
  xDeviceCharacteristics(pFile) {
    this['log']?.('jDeviceCharacteristics', pFile);
    return this.jDeviceCharacteristics(pFile);
  }

  /**
   * Wrapped DataView for pointer arguments.
   * Pointers to a single value are passed using DataView. A Proxy
   * wrapper prevents use of incorrect type or endianness.
   * @param {'Int32'|'BigInt64'} type 
   * @param {number} byteOffset 
   * @returns {DataView}
   */
  #makeTypedDataView(type, byteOffset) {
    const byteLength = type === 'Int32' ? 4 : 8;
    const getter = `get${type}`;
    const setter = `set${type}`;
    const makeDataView = () => new DataView(
      this._module.HEAPU8.buffer,
      this._module.HEAPU8.byteOffset + byteOffset,
      byteLength);
    let dataView = makeDataView();
    return new Proxy(dataView, {
      get(_, prop) {
        if (dataView.buffer.byteLength === 0) {
          // WebAssembly memory resize detached the buffer.
          dataView = makeDataView();
        }
        if (prop === getter) {
          return function(byteOffset, littleEndian) {
            if (!littleEndian) throw new Error('must be little endian');
            return dataView[prop](byteOffset, littleEndian);
          }
        }
        if (prop === setter) {
          return function(byteOffset, value, littleEndian) {
            if (!littleEndian) throw new Error('must be little endian');
            return dataView[prop](byteOffset, value, littleEndian);
          }
        }
        if (typeof prop === 'string' && (prop.match(/^(get)|(set)/))) {
          throw new Error('invalid type');
        }
        const result = dataView[prop];
        return typeof result === 'function' ? result.bind(dataView) : result;
      }
    });
  }

  /**
   * @param {number} byteOffset 
   * @param {number} byteLength 
   */
  makeDataArray(byteOffset, byteLength) {
    let target = this._module.HEAPU8.subarray(byteOffset, byteOffset + byteLength);
    return new Proxy(target, {
      get: (_, prop, receiver) => {
        if (target.buffer.byteLength === 0) {
          // WebAssembly memory resize detached the buffer.
          target = this._module.HEAPU8.subarray(byteOffset, byteOffset + byteLength);
        }
        const result = target[prop];
        return typeof result === 'function' ? result.bind(target) : result;
      }
    });
  }

  #decodeFilename(zName, flags) {
    if (flags & VFS.SQLITE_OPEN_URI) {
      // The first null-terminated string is the URI path. Subsequent
      // strings are query parameter keys and values.
      // https://www.sqlite.org/c3ref/open.html#urifilenamesinsqlite3open
      let pName = zName;
      let state = 1;
      const charCodes = [];
      while (state) {
        const charCode = this._module.HEAPU8[pName++];
        if (charCode) {
          charCodes.push(charCode);
        } else {
          if (!this._module.HEAPU8[pName]) state = null;
          switch (state) {
            case 1: // path
              charCodes.push('?'.charCodeAt(0));
              state = 2;
              break;
            case 2: // key
              charCodes.push('='.charCodeAt(0));
              state = 3;
              break;
            case 3: // value
              charCodes.push('&'.charCodeAt(0));
              state = 2;
              break;
          }
        }
      }
      return  new TextDecoder().decode(new Uint8Array(charCodes));
    }
    return zName ? this.UTF8ToString(zName) : null;
  }

  UTF8ToString(ptr, maxBytesToRead) {
    return ptr ? UTF8ArrayToString(HEAPU8, ptr, maxBytesToRead) : "";
  }
}

// Emscripten "legalizes" 64-bit integer arguments by passing them as
// two 32-bit signed integers.
function delegalize(lo32, hi32) {
  return (hi32 * 0x100000000) + lo32 + (lo32 < 0 ? 2**32 : 0);
}


// Sample in-memory filesystem.
export class SqliteWasmMemoryVFS extends FacadeVFS {
  // Map of existing files, keyed by filename.
  mapNameToFile = new Map();

  // Map of open files, keyed by id (sqlite3_file pointer).
  mapIdToFile = new Map();

  constructor(name, module) {
    super(name, module);
  }

  close() {
    for (const fileId of this.mapIdToFile.keys()) {
      this.jClose(fileId);
    }
  }

  /**
   * @param {string?} filename 
   * @param {number} fileId 
   * @param {number} flags 
   * @param {DataView} pOutFlags 
   * @returns {number}
   */
  jOpen(filename, fileId, flags, pOutFlags) {
    const url = new URL(filename || Math.random().toString(36).slice(2), 'file://');
    const pathname = url.pathname;

    let file = this.mapNameToFile.get(pathname);
    if (!file) {
      if (flags & VFS.SQLITE_OPEN_CREATE) {
        // Create a new file object.
        file = {
          pathname,
          flags,
          size: 0,
          data: new ArrayBuffer(0)
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
    const file = this.mapIdToFile.get(fileId);
    this.mapIdToFile.delete(fileId);

    if (file.flags & VFS.SQLITE_OPEN_DELETEONCLOSE) {
      this.mapNameToFile.delete(file.pathname);
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
    const file = this.mapIdToFile.get(fileId);

    // Clip the requested read to the file boundary.
    const bgn = Math.min(iOffset, file.size);
    const end = Math.min(iOffset + pData.byteLength, file.size);
    const nBytes = end - bgn;

    if (nBytes) {
      pData.set(new Uint8Array(file.data, bgn, nBytes));
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
    const file = this.mapIdToFile.get(fileId);
    if (iOffset + pData.byteLength > file.data.byteLength) {
      // Resize the ArrayBuffer to hold more data.
      const newSize = Math.max(iOffset + pData.byteLength, 2 * file.data.byteLength);
      const data = new ArrayBuffer(newSize);
      new Uint8Array(data).set(new Uint8Array(file.data, 0, file.size));
      file.data = data;
    }

    // Copy data.
    new Uint8Array(file.data, iOffset, pData.byteLength).set(pData);
    file.size = Math.max(file.size, iOffset + pData.byteLength);
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} fileId 
   * @param {number} iSize 
   * @returns {number}
   */
  jTruncate(fileId, iSize) {
    const file = this.mapIdToFile.get(fileId);

    // For simplicity we don't make the ArrayBuffer smaller.
    file.size = Math.min(file.size, iSize);
    return VFS.SQLITE_OK;
  }

  /**
   * @param {number} fileId 
   * @param {DataView} pSize64 
   * @returns {number}
   */
  jFileSize(fileId, pSize64) {
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
    const url = new URL(name, 'file://');
    const pathname = url.pathname;

    this.mapNameToFile.delete(pathname);
    return VFS.SQLITE_OK;
  }

  /**
   * @param {string} name 
   * @param {number} flags 
   * @param {DataView} pResOut 
   * @returns {number}
   */
  jAccess(name, flags, pResOut) {
    const url = new URL(name, 'file://');
    const pathname = url.pathname;

    const file = this.mapNameToFile.get(pathname);
    pResOut.setInt32(0, file ? 1 : 0, true);
    return VFS.SQLITE_OK;
  }

  /**
   * Override the base makeDataArray to not watch for WebAssembly memory resize.
   * as we always use the memory immediately.
   * @param {number} byteOffset 
   * @param {number} byteLength 
   */
  makeDataArray(byteOffset, byteLength) {
    return this._module.HEAPU8.subarray(byteOffset, byteOffset + byteLength);
  }
}


var UTF8Decoder = typeof TextDecoder != "undefined" ? new TextDecoder("utf8") : undefined;
var UTF8ArrayToString = (heapOrArray, idx, maxBytesToRead) => {
    var endIdx = idx + maxBytesToRead;
    var endPtr = idx;
    while (heapOrArray[endPtr] && !(endPtr >= endIdx)) ++endPtr;
    if (endPtr - idx > 16 && heapOrArray.buffer && UTF8Decoder) {
        return UTF8Decoder.decode(heapOrArray.subarray(idx, endPtr))
    }
    var str = "";
    while (idx < endPtr) {
        var u0 = heapOrArray[idx++];
        if (!(u0 & 128)) {
            str += String.fromCharCode(u0);
            continue
        }
        var u1 = heapOrArray[idx++] & 63;
        if ((u0 & 224) == 192) {
            str += String.fromCharCode((u0 & 31) << 6 | u1);
            continue
        }
        var u2 = heapOrArray[idx++] & 63;
        if ((u0 & 240) == 224) {
            u0 = (u0 & 15) << 12 | u1 << 6 | u2
        } else {
            u0 = (u0 & 7) << 18 | u1 << 12 | u2 << 6 | heapOrArray[idx++] & 63
        }
        if (u0 < 65536) {
            str += String.fromCharCode(u0)
        } else {
            var ch = u0 - 65536;
            str += String.fromCharCode(55296 | ch >> 10, 56320 | ch & 1023)
        }
    }
    return str
};
