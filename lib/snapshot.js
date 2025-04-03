/**
 * Snapshot class for RocksDB-compatible IndexedDB adapter
 * Provides a consistent view of the database at the time the snapshot was created
 * by storing a copy of the data in a dedicated snapshot store.
 */
export class Snapshot {
  /**
   * Create a new snapshot
   * @param {object} db - Database session
   */
  constructor(db) {
    this.db = db;
    this._refCount = 0;
    this._snapshotId = `snap_${Date.now()}`;
    this._storeName = `_snapshot_${this._snapshotId}`;
    this._initialized = false;
    this.cfName = this._getCfName();
    
    // Initialize immediately unless explicitly deferred
    const deferInit = db._state && db._state.deferSnapshotInit;
    if (!deferInit) {
      this._init().catch(err => console.error('Error initializing snapshot:', err));
    }
    
    // Register the snapshot with the database state
    if (db._state) {
      if (!db._state._snapshots) {
        db._state._snapshots = new Set();
      }
      db._state._snapshots.add(this);
    }
    
    // Keep track of the database state
    if (db._ref) db._ref();
  }

  /**
   * Get the column family name for this snapshot
   * @private
   * @returns {string} Column family name
   */
  _getCfName() {
    if (!this.db || !this.db._columnFamily) {
      return 'default';
    }
    
    return typeof this.db._columnFamily === 'string' 
      ? this.db._columnFamily 
      : (this.db._columnFamily.name || 'default');
  }

  /**
   * Initialize the snapshot (creates snapshot store and copies data)
   * @returns {Promise<void>}
   */
  async _init() {
    if (this._initialized) return;
    
    try {
      // Get database connection
      await this.db._state.ready();
      const mainDb = this.db._state._db;
      if (!mainDb) throw new Error('Database not available');
      
      // Get source column family/store
      const sourceCfName = this.cfName;

      // Clean up old snapshots before creating new one
      await this._removeOldSnapshots();
      
      // Check if we need to create the snapshot store
      if (!mainDb.objectStoreNames.contains(this._storeName)) {
        await this._createSnapshotStore();
      }
      
      // Copy current data to snapshot store
      await this._copyDataToSnapshot(sourceCfName);
      
      // Register this as the current snapshot
      this.db._state.currentSnapshot = this;
      this._initialized = true;
      
    } catch (err) {
      console.error('Failed to initialize snapshot:', err);
    }
  }

  /**
   * Create the snapshot store in the database
   * @private
   * @returns {Promise<void>}
   */
  async _createSnapshotStore() {
    const state = this.db._state;
    const mainDb = state._db;
    const currentVersion = mainDb.version;
    
    // Close current connection
    mainDb.close();
    state._db = null;
    
    // Open with version upgrade to add the snapshot store
    try {
      const db = await new Promise((resolve, reject) => {
        const request = indexedDB.open(state.path, currentVersion + 1);
        
        request.onupgradeneeded = (event) => {
          const db = event.target.result;
          db.createObjectStore(this._storeName);
        };
        
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      });
      
      // Update the state with new DB connection
      state._db = db;
    } catch (err) {
      console.error('Error creating snapshot store:', err);
      // Try to reopen the original database
      const reopenRequest = indexedDB.open(state.path, currentVersion);
      reopenRequest.onsuccess = () => {
        state._db = reopenRequest.result;
      };
      throw err;
    }
  }

  /**
   * Copy data from source store to snapshot store
   * @private
   * @param {string} sourceCfName - Source column family name
   * @returns {Promise<void>}
   */
  async _copyDataToSnapshot(sourceCfName) {
    const db = this.db._state._db;
    
    try {
      const tx = db.transaction([sourceCfName, this._storeName], 'readwrite');
      const sourceStore = tx.objectStore(sourceCfName);
      const snapshotStore = tx.objectStore(this._storeName);
      
      // Clear any existing data in snapshot store
      snapshotStore.clear();
      
      // Copy all data from source to snapshot
      await new Promise((resolve, reject) => {
        const cursorRequest = sourceStore.openCursor();
        
        cursorRequest.onsuccess = (event) => {
          const cursor = event.target.result;
          if (cursor) {
            // Copy this key-value pair to snapshot store
            snapshotStore.put(cursor.value, cursor.key);
            cursor.continue();
          } else {
            // Finished copying all data
            resolve();
          }
        };
        
        cursorRequest.onerror = () => reject(cursorRequest.error);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
      });
    } catch (err) {
      console.error('Error copying data to snapshot:', err);
      throw err;
    }
  }

  /**
   * Remove all existing snapshot stores
   * @private
   * @returns {Promise<void>}
   */
  async _removeOldSnapshots() {
    const state = this.db._state;
    const db = state._db;
    const storeNames = Array.from(db.objectStoreNames);
    
    // Find snapshot stores
    const snapshotStores = storeNames.filter(name => 
      name.startsWith('_snapshot_') && name !== this._storeName
    );
    
    if (snapshotStores.length === 0) return;
    
    // Need version upgrade to remove object stores
    const currentVersion = db.version;
    
    // Close current connection
    db.close();
    state._db = null;
    
    try {
      // Reopen with version upgrade
      const newDb = await new Promise((resolve, reject) => {
        const request = indexedDB.open(state.path, currentVersion + 1);
        
        request.onupgradeneeded = (event) => {
          const db = event.target.result;
          // Delete old snapshot stores
          for (const storeName of snapshotStores) {
            if (db.objectStoreNames.contains(storeName)) {
              db.deleteObjectStore(storeName);
            }
          }
        };
        
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      });
      
      // Update state with new connection
      state._db = newDb;
    } catch (err) {
      console.error('Error removing old snapshots:', err);
      // Try to reopen the original database
      const reopenRequest = indexedDB.open(state.path, currentVersion);
      reopenRequest.onsuccess = () => {
        state._db = reopenRequest.result;
      };
      throw err;
    }
  }

  /**
   * Get a value from the snapshot
   * @param {*} key - The key to get
   * @returns {Promise<*>} The value or null if not found
   */
  async getValue(key) {
    try {
      // Make sure we're initialized
      if (!this._initialized) {
        await this._init();
      }
      
      const db = this.db._state._db;
      if (!db) return null;
      
      try {
        const tx = db.transaction([this._storeName], 'readonly');
        const store = tx.objectStore(this._storeName);
        
        return await new Promise((resolve) => {
          const request = store.get(key);
          request.onsuccess = () => resolve(request.result || null);
          request.onerror = () => resolve(null);
        });
      } catch (err) {
        console.error('Error reading from snapshot:', err);
        return null;
      }
    } catch (err) {
      console.error('Error in getValue:', err);
      return null;
    }
  }

  /**
   * Create a read batch that uses this snapshot
   * @param {object} options - Batch options
   * @returns {Promise<Batch>} Promise with the batch
   */
  async read(options = {}) {
    // Make sure we're initialized before allowing reads
    if (!this._initialized) {
      await this._init();
    }
    
    // Create a read batch with this snapshot
    options.snapshot = this;
    return this.db.read(options);
  }

  /**
   * Create a read session that uses this snapshot
   * @param {object} options - Session options
   * @returns {Session} Session object
   */
  session(options = {}) {
    options.snapshot = this;
    return this.db.session(options);
  }

  /**
   * Create an iterator for this snapshot
   * @param {object} range - Range options
   * @param {object} options - Iterator options
   * @returns {Iterator} Iterator instance
   */
  iterator(range, options = {}) {
    // Set the snapshot store name for the iterator
    options.snapshot = this;
    options.snapshotStoreName = this._storeName;
    return this.db.iterator(range, options);
  }

  /**
   * Check if a key exists in the snapshot
   * @param {*} key - The key to check
   * @returns {Promise<boolean>} True if the key exists
   */
  async hasValue(key) {
    try {
      const value = await this.getValue(key);
      return value !== null && value !== undefined;
    } catch (err) {
      console.error('Error in hasValue:', err);
      return false;
    }
  }

  /**
   * Close the snapshot and free its resources
   * @returns {Promise<void>} Promise that resolves when snapshot is closed
   */
  async close() {
    // Unregister from the database state
    if (this.db && this.db._state && this.db._state._snapshots) {
      this.db._state._snapshots.delete(this);
      
      // If this is the current snapshot, clear it
      if (this.db._state.currentSnapshot === this) {
        this.db._state.currentSnapshot = null;
      }
    }
    
    // We don't delete the snapshot store here
    // It will be removed when a new snapshot is created
    
    // Unreference the database
    if (this.db && this.db._unref) {
      this.db._unref();
    }
    
    this._initialized = false;
    return Promise.resolve();
  }

  /**
   * Increase reference count
   * @private
   */
  _ref() {
    this._refCount++;
    return this;
  }

  /**
   * Decrease reference count and cleanup if zero
   * @private
   */
  _unref() {
    if (--this._refCount <= 0) {
      this._cleanup();
    }
    return this;
  }

  /**
   * Clean up resources when snapshot is no longer needed
   * @private
   */
  _cleanup() {
    // Automatic resource cleanup when ref count reaches zero
    this.close().catch(err => {
      console.error('Error closing snapshot:', err);
    });
  }

  // Alias for compatibility with different RocksDB interfaces
  destroy() {
    return this.close();
  }
}

export default Snapshot; 