/**
 * Snapshot class for RocksDB-compatible IndexedDB adapter
 * Provides a consistent view of the database at the time the snapshot was created
 * using a shadow database approach rather than full in-memory copies
 */
export class Snapshot {
  /**
   * Create a new snapshot
   * @param {object} db - Database session
   */
  constructor(db) {
    this.db = db;
    this._refCount = 0;
    this._handle = null;
    this._initialized = false;
    this._snapshotId = `snap_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    
    // Don't initialize immediately if deferring is enabled
    if (db._state && db._state.deferSnapshotInit === false) {
      this._init();
    }
  }
  
  /**
   * Initialize the snapshot by creating shadow object store
   * @private
   */
  async _init() {
    if (this._initialized) return;
    this._initialized = true;
    
    if (!this.db._state || !this.db._state._db) {
      console.warn('Cannot initialize snapshot: No database connection');
      return;
    }
    
    const state = this.db._state;
    const dbConn = state._db;
    const cfName = this._getCfName();
    const snapshotStoreName = `${cfName}_${this._snapshotId}`;
    
    try {
      // First, check if we need to modify the database schema to add our snapshot store
      const currentVersion = dbConn.version;
      
      // Close current database connection as we need to upgrade
      dbConn.close();
      
      // Open with a higher version to trigger onupgradeneeded
      const openRequest = state._indexedDB.open(state.path, currentVersion + 1);
      
      openRequest.onupgradeneeded = (event) => {
        const db = event.target.result;
        
        // Create a new object store for this snapshot
        if (!db.objectStoreNames.contains(snapshotStoreName)) {
          db.createObjectStore(snapshotStoreName);
        }
      };
      
      // Wait for the upgrade to complete
      await new Promise((resolve, reject) => {
        openRequest.onsuccess = (event) => {
          state._db = event.target.result;
          resolve();
        };
        
        openRequest.onerror = (event) => {
          reject(new Error(`Failed to upgrade database for snapshot: ${event.target.error.message}`));
        };
      });
      
      // Now that we have the snapshot store, let's populate it with modified keys
      // We don't actually copy data now - we just create the empty store
      // Data will be copied on-demand when values are modified
      
      // Record this snapshot in the state for future reference
      if (!state._snapshots) {
        state._snapshots = new Map();
      }
      
      state._snapshots.set(this._snapshotId, {
        id: this._snapshotId,
        cfName,
        storeName: snapshotStoreName,
        createdAt: Date.now()
      });
      
      // Create a handle similar to RocksDB's snapshot handle
      this._handle = { id: this._snapshotId, storeName: snapshotStoreName };
      
    } catch (err) {
      console.error('Error initializing snapshot store:', err);
    }
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
   * Get a value from the snapshot
   * @param {*} key - The key to get
   * @returns {Promise<*>} The value or null if not found
   */
  async getValue(key) {
    if (!this._initialized || !this._handle) {
      await this._init();
    }
    
    if (!this.db._state || !this.db._state._db) {
      return null;
    }
    
    const dbConn = this.db._state._db;
    const snapshotStoreName = this._handle.storeName;
    const cfName = this._getCfName();
    
    try {
      // First, check if we have this key in the snapshot store
      // (meaning it was modified after the snapshot was created)
      const transaction = dbConn.transaction([snapshotStoreName], 'readonly');
      const snapshotStore = transaction.objectStore(snapshotStoreName);
      
      // Try to get from snapshot store first
      const snapshotValue = await new Promise((resolve) => {
        const request = snapshotStore.get(key);
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => resolve(undefined);
      });
      
      // If we have a record in the snapshot store, return it
      // A special undefined marker means the key was deleted after snapshot creation
      if (snapshotValue === '_SNAPSHOT_DELETED_') {
        return null;
      }
      
      if (snapshotValue !== undefined) {
        return snapshotValue;
      }
      
      // If not found in snapshot store, fall back to the main store
      // which will have the value as it was when the snapshot was created
      const mainTransaction = dbConn.transaction([cfName], 'readonly');
      const mainStore = mainTransaction.objectStore(cfName);
      
      return await new Promise((resolve) => {
        const request = mainStore.get(key);
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => resolve(null);
      });
    } catch (err) {
      console.error('Error getting value from snapshot:', err);
      return null;
    }
  }
  
  /**
   * Check if the snapshot has a value for a key
   * @param {*} key - The key to check
   * @returns {Promise<boolean>} True if the key exists in the snapshot
   */
  async hasValue(key) {
    const value = await this.getValue(key);
    return value !== null && value !== undefined;
  }
  
  /**
   * Increment the reference count
   */
  _ref() {
    this._refCount++;
  }
  
  /**
   * Decrement the reference count
   */
  _unref() {
    if (--this._refCount <= 0) {
      this._cleanup();
    }
  }
  
  /**
   * Clean up snapshot resources
   * @private
   */
  async _cleanup() {
    if (!this._handle || !this.db._state || !this.db._state._db) {
      return;
    }
    
    try {
      // Note: In a real implementation, we might want to delete the snapshot store
      // However, this requires another schema upgrade which can be complex
      // For simplicity, we'll just mark this snapshot as inactive
      
      const state = this.db._state;
      if (state._snapshots) {
        state._snapshots.delete(this._snapshotId);
      }
      
      this._handle = null;
    } catch (err) {
      console.error('Error cleaning up snapshot:', err);
    }
  }
}

/**
 * This static method should be called when a key is modified in the database
 * It will copy the current value to all active snapshots before modification
 * @param {object} state - The database state
 * @param {string} cfName - Column family name
 * @param {*} key - The key being modified
 * @param {*} oldValue - The value before modification (or null for deletion)
 */
Snapshot.recordPreModificationValue = async function(state, cfName, key, oldValue) {
  if (!state || !state._snapshots || state._snapshots.size === 0) {
    return; // No active snapshots, nothing to do
  }
  
  if (!state._db) {
    return; // No database connection
  }
  
  // For each active snapshot, record the current value before it's changed
  for (const snapshot of state._snapshots.values()) {
    if (snapshot.cfName !== cfName) {
      continue; // Skip snapshots for other column families
    }
    
    try {
      const transaction = state._db.transaction([snapshot.storeName], 'readwrite');
      const store = transaction.objectStore(snapshot.storeName);
      
      // When a key is deleted, we store a special marker
      const valueToStore = oldValue === null ? '_SNAPSHOT_DELETED_' : oldValue;
      
      // Only store in the snapshot if we don't already have a record for this key
      const existingRequest = store.get(key);
      
      existingRequest.onsuccess = (event) => {
        // If no record exists yet in this snapshot, save the pre-modification value
        if (event.target.result === undefined) {
          store.put(valueToStore, key);
        }
      };
    } catch (err) {
      console.error(`Error recording pre-modification value for snapshot ${snapshot.id}:`, err);
    }
  }
};

export default Snapshot; 