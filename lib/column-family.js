import binding from "./binding.js";
import { BloomFilterPolicy } from "./filter-policy.js";

class RocksDBColumnFamily {
  constructor(name, opts = {}) {
    const {
      // Blob options
      enableBlobFiles = false,
      minBlobSize = 0,
      blobFileSize = 0,
      enableBlobGarbageCollection = true,
      // Block table options
      tableBlockSize = 8192,
      tableCacheIndexAndFilterBlocks = true,
      tableFormatVersion = 6,
      optimizeFiltersForMemory = false,
      blockCache = true,
      filterPolicy = new BloomFilterPolicy(10),
    } = opts;

    this._name = name;
    this._flushing = null;
    this._options = {
      enableBlobFiles,
      minBlobSize,
      blobFileSize,
      enableBlobGarbageCollection,
      tableBlockSize,
      tableCacheIndexAndFilterBlocks,
      tableFormatVersion,
      optimizeFiltersForMemory,
      blockCache,
      filterPolicy,
    };

    // For IndexedDB, we simplify column family initialization
    this._handle = { name };
  }

  cloneSettings(name) {
    return new RocksDBColumnFamily(name, this._options);
  }

  get name() {
    return this._name;
  }

  destroy() {
    // No need to destroy anything in IndexedDB implementation
    this._handle = null;
  }
}

export default RocksDBColumnFamily;
