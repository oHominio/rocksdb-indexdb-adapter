/**
 * Represents a column family in RocksDB, which maps to an object store in IndexedDB
 */
export class ColumnFamily {
  /**
   * Create a new column family
   * @param {string} name - Column family name (object store name)
   * @param {object} options - Configuration options
   */
  constructor(name, options = {}) {
    const {
      // Blob options (not directly applicable to IndexedDB but kept for API compatibility)
      enableBlobFiles = false,
      minBlobSize = 0,
      blobFileSize = 0,
      enableBlobGarbageCollection = true,
      // Block table options (not directly applicable to IndexedDB but kept for API compatibility)
      tableBlockSize = 8192,
      tableCacheIndexAndFilterBlocks = true,
      tableFormatVersion = 6,
      optimizeFiltersForMemory = false,
      blockCache = true,
      filterPolicy = null,
      // IndexedDB specific options
      autoIncrement = false,
      keyPath = null
    } = options;

    this._name = name;
    this._handle = { name }; // For compatibility with native RocksDB API
    this.options = {
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
      autoIncrement,
      keyPath
    };
    
    // For tracking pending operations and state
    this._pendingOperations = [];
    this._flushing = null;
  }

  /**
   * Clone the column family settings with a new name
   * @param {string} name - New column family name
   * @returns {ColumnFamily} New column family with the same settings
   */
  cloneSettings(name) {
    return new ColumnFamily(name, this.options);
  }

  /**
   * Get column family name
   * @returns {string} Column family name
   */
  get name() {
    return this._name;
  }

  /**
   * Set column family name
   * @param {string} value - Column family name
   */
  set name(value) {
    this._name = value;
    // Update the handle name as well for compatibility
    if (this._handle) {
      this._handle.name = value;
    }
  }

  /**
   * Check if the column family (object store) exists in the database
   * @param {IDBDatabase} db - IndexedDB database connection
   * @returns {boolean} True if the object store exists
   */
  exists(db) {
    if (!db) return false;
    return Array.from(db.objectStoreNames).includes(this.name);
  }

  /**
   * Ensure the column family exists in the IndexedDB database
   * @param {IDBDatabase} db - IndexedDB database connection
   * @returns {boolean} True if the object store existed, false if it needed creation
   */
  ensureExists(db) {
    if (!db) return false;
    
    // Check if the object store already exists
    if (this.exists(db)) {
      return true;
    }
    
    // This operation should be performed during an upgrade event
    // Return false to indicate it doesn't exist yet
    return false;
  }

  /**
   * Create the object store during database upgrade
   * @param {IDBDatabase} db - IndexedDB database during upgrade
   * @returns {IDBObjectStore} Created object store
   * @throws {Error} If the object store cannot be created
   */
  createObjectStore(db) {
    if (!db) {
      throw new Error('Database connection not provided');
    }
    
    try {
      // Check if we need to delete it first (if it exists but with different options)
      if (this.exists(db)) {
        db.deleteObjectStore(this.name);
      }
      
      // Create the object store with configured options
      const objectStoreOptions = {};
      
      if (this.options.keyPath !== null) {
        objectStoreOptions.keyPath = this.options.keyPath;
      }
      
      if (this.options.autoIncrement) {
        objectStoreOptions.autoIncrement = true;
      }
      
      const store = db.createObjectStore(this.name, objectStoreOptions);
      
      // Create any needed indexes (could be extended in the future)
      // Example: store.createIndex('by_name', 'name', { unique: false });
      
      return store;
    } catch (err) {
      console.error(`Failed to create object store '${this.name}':`, err);
      throw new Error(`Failed to create object store '${this.name}': ${err.message}`);
    }
  }

  /**
   * Destroy the column family (delete the object store)
   * Can only be called during an upgrade event
   * @param {IDBDatabase} db - IndexedDB database during upgrade
   * @throws {Error} If the object store cannot be deleted
   */
  destroy(db) {
    if (!db) {
      throw new Error('Database connection not provided');
    }
    
    try {
      if (this.exists(db)) {
        db.deleteObjectStore(this.name);
      }
    } catch (err) {
      console.error(`Failed to delete object store '${this.name}':`, err);
      throw new Error(`Failed to delete object store '${this.name}': ${err.message}`);
    }
  }
  
  /**
   * Get a transaction for this column family
   * @param {IDBDatabase} db - IndexedDB database connection
   * @param {string} mode - Transaction mode ('readonly' or 'readwrite')
   * @returns {IDBTransaction} Transaction object
   */
  getTransaction(db, mode = 'readonly') {
    if (!db) {
      throw new Error('Database connection not provided');
    }
    
    if (!this.exists(db)) {
      throw new Error(`Object store '${this.name}' does not exist`);
    }
    
    try {
      return db.transaction(this.name, mode);
    } catch (err) {
      console.error(`Failed to create transaction for '${this.name}':`, err);
      throw new Error(`Failed to create transaction for '${this.name}': ${err.message}`);
    }
  }
  
  /**
   * Get an object store for this column family
   * @param {IDBDatabase|IDBTransaction} dbOrTx - Database connection or transaction
   * @param {string} [mode] - Transaction mode if providing a database ('readonly' or 'readwrite')
   * @returns {IDBObjectStore} Object store
   */
  getObjectStore(dbOrTx, mode) {
    try {
      if (dbOrTx.objectStoreNames) {
        // It's a database connection, create a transaction
        const tx = this.getTransaction(dbOrTx, mode || 'readonly');
        return tx.objectStore(this.name);
      } else if (dbOrTx.objectStore) {
        // It's a transaction, get the object store
        return dbOrTx.objectStore(this.name);
      } else {
        throw new Error('Invalid database or transaction provided');
      }
    } catch (err) {
      console.error(`Failed to get object store '${this.name}':`, err);
      throw new Error(`Failed to get object store '${this.name}': ${err.message}`);
    }
  }
  
  /**
   * Execute a callback with a transaction for this column family
   * @param {IDBDatabase} db - IndexedDB database connection
   * @param {function} callback - Callback function receiving the object store
   * @param {string} mode - Transaction mode ('readonly' or 'readwrite')
   * @returns {Promise<any>} Promise resolving to the callback result
   */
  async withTransaction(db, callback, mode = 'readonly') {
    if (!db) {
      throw new Error('Database connection not provided');
    }
    
    return new Promise((resolve, reject) => {
      try {
        const tx = this.getTransaction(db, mode);
        const store = tx.objectStore(this.name);
        
        let result;
        try {
          result = callback(store, tx);
        } catch (err) {
          tx.abort();
          reject(err);
          return;
        }
        
        tx.oncomplete = () => {
          resolve(result);
        };
        
        tx.onerror = (event) => {
          reject(new Error(`Transaction error: ${event.target.error?.message || 'Unknown error'}`));
        };
        
        tx.onabort = (event) => {
          reject(new Error(`Transaction aborted: ${event.target.error?.message || 'User aborted'}`));
        };
      } catch (err) {
        reject(err);
      }
    });
  }
}

export default ColumnFamily 