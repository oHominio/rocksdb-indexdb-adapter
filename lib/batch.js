import c from 'compact-encoding'
import { Snapshot } from './snapshot.js'

// Import IDBKeyRange for range operations
const IDBKeyRange = typeof window !== 'undefined' 
  ? window.IDBKeyRange 
  : (typeof global !== 'undefined' ? global.IDBKeyRange : null);

/**
 * Base batch class for reading from and writing to IndexedDB
 */
export class Batch {
  /**
   * Create a new batch
   * @param {object} db - Database session
   * @param {object} options - Batch options
   */
  constructor(db, options = {}) {
    const {
      write = false,
      autoDestroy = false
    } = options;

    this.db = db;
    this.write = write;
    this.destroyed = false;
    this.autoDestroy = autoDestroy;
    this._promises = new Map();
    this._pendingOps = new Map();

    // Reference the database to prevent it from closing during batch operations
    if (db._ref) db._ref();
  }

  /**
   * Ensure batch is ready for operations
   * @returns {Promise<void>} Promise that resolves when batch is ready
   */
  async ready() {
    // For the IndexedDB adapter, we just need to check if the database is ready
    if (this.db._state && this.db._state.ready) {
      try {
        await this.db._state.ready();
      } catch (err) {
        console.error('Error in batch ready:', err);
      }
    }
    return Promise.resolve();
  }

  /**
   * Reuse an existing batch with new settings
   * @param {object} db - Database session
   * @param {object} options - Batch options
   */
  _reuse(db, options = {}) {
    this.db = db;
    this.write = !!options.write;
    this.destroyed = false;
    this.autoDestroy = !!options.autoDestroy;

    this._promises.clear();
    this._pendingOps.clear();

    // Reference the database to prevent it from closing during batch operations
    if (db._ref) db._ref();
  }

  /**
   * Get a value by key
   * @param {*} key - Key to get
   * @returns {Promise<*>} Promise that resolves with the value
   */
  async get(key) {
    if (this.destroyed) {
      return null;
    }

    // Encode key for storage and lookup
    const encodedKey = this._encodeKey(key);
    
    // First check if we have this key in pending operations
    const pendingOp = this._pendingOps.get(encodedKey);
    if (pendingOp) {
      // If we have a DELETE operation, return null
      if (pendingOp.type === 'delete') {
        return null;
      }
      
      // If we have a PUT operation, return the value
      if (pendingOp.type === 'put') {
        return this._decodeValue(pendingOp.value);
      }
    }
    
    // Only try database - with timeout protection
    try {
      const result = await Promise.race([
        this._executeGet(encodedKey),
        new Promise((resolve) => setTimeout(() => {
          console.warn('Get operation timed out');
          resolve(null);
        }, 1000))
      ]);
      return result;
    } catch (err) {
      console.error('Error in get operation:', err);
      return null;
    }
  }

  /**
   * Execute a get operation against the database
   * @private
   * @param {*} encodedKey - Encoded key to get
   * @returns {Promise<*>} Promise that resolves with the decoded value
   */
  async _executeGet(encodedKey) {
    // If no database connection, return null
    if (!this.db._state || !this.db._state._db) {
      return null;
    }

    // Check if database is suspended
    if (this.db._state._suspended) {
      return null;
    }

    // Get column family name
    const cfName = this._getCfName();

    try {
      // Open transaction
      const transaction = this.db._state._db.transaction([cfName], 'readonly');
      const store = transaction.objectStore(cfName);

      // Direct approach to get value
      return new Promise((resolve) => {
        const request = store.get(encodedKey);
        
        request.onsuccess = (event) => {
          resolve(this._decodeValue(event.target.result));
        };
        
        request.onerror = () => {
          resolve(null);
        };
      });
    } catch (err) {
      console.error('Error executing get:', err);
      return null;
    }
  }

  /**
   * Put a key-value pair
   * @param {*} key - Key to put
   * @param {*} value - Value to put
   * @returns {Promise<void>} Promise that resolves when put completes
   */
  put(key, value) {
    if (this.destroyed) {
      return Promise.resolve();
    }

    if (!this.write) {
      return Promise.resolve();
    }

    // Encode key and value
    const encodedKey = this._encodeKey(key);
    const encodedValue = this._encodeValue(value);

    // Store in pending operations
    this._pendingOps.set(encodedKey, {
      type: 'put',
      value: encodedValue
    });

    // Create a promise for this operation
    const promise = new Promise(resolve => {
      this._promises.set(encodedKey, { resolve });
    });

    // Auto-flush if requested
    if (this.autoDestroy) {
      this.flush().catch(() => {});
    }

    return promise;
  }

  /**
   * Delete a key-value pair
   * @param {*} key - Key to delete
   * @returns {Promise<void>} Promise that resolves when delete completes
   */
  delete(key) {
    if (this.destroyed) {
      return Promise.resolve();
    }

    if (!this.write) {
      return Promise.resolve();
    }

    // Encode key
    const encodedKey = this._encodeKey(key);

    // Store in pending operations
    this._pendingOps.set(encodedKey, {
      type: 'delete'
    });

    // Create a promise for this operation
    const promise = new Promise(resolve => {
      this._promises.set(encodedKey, { resolve });
    });

    // Auto-flush if requested
    if (this.autoDestroy) {
      this.flush().catch(() => {});
    }

    return promise;
  }

  /**
   * Delete a range of keys
   * @param {*} start - Start key
   * @param {*} end - End key
   * @returns {Promise<void>} Promise that resolves when delete range completes
   */
  async deleteRange(start, end) {
    if (this.destroyed) {
      return Promise.resolve();
    }

    if (!this.write) {
      return Promise.resolve();
    }

    // Encode keys
    const encodedStart = this._encodeKey(start);
    const encodedEnd = this._encodeKey(end);

    // Try to do the actual database operation - with timeout
    try {
      await Promise.race([
        this._executeDeleteRange(encodedStart, encodedEnd),
        new Promise((resolve) => setTimeout(() => {
          console.warn('DeleteRange operation timed out');
          resolve();
        }, 2000))
      ]);
    } catch (err) {
      console.error('Error in deleteRange:', err);
    }

    return Promise.resolve();
  }

  /**
   * Execute delete range on the database
   * @private
   * @param {*} encodedStart - Start key
   * @param {*} encodedEnd - End key 
   */
  async _executeDeleteRange(encodedStart, encodedEnd) {
    if (!this.db._state || !this.db._state._db) {
      return;
    }

    try {
      // Get column family name
      const cfName = this._getCfName();

      // Simplified approach - just try to get all keys and delete them
      const transaction = this.db._state._db.transaction([cfName], 'readwrite');
      const store = transaction.objectStore(cfName);
      
      // Create a range
      let range;
      try {
        range = IDBKeyRange.bound(encodedStart, encodedEnd, false, true);
      } catch (err) {
        console.error('Error creating range:', err);
        return;
      }
      
      // Directly delete using cursor
      return new Promise((resolve) => {
        const request = store.openCursor(range);
        
        request.onsuccess = (event) => {
          const cursor = event.target.result;
          if (cursor) {
            cursor.delete();
            cursor.continue();
          }
        };
        
        transaction.oncomplete = () => {
          resolve();
        };
        
        transaction.onerror = () => {
          resolve();
        };
        
        transaction.onabort = () => {
          resolve();
        };
      });
    } catch (err) {
      console.error('Error in deleteRange:', err);
    }
  }

  /**
   * Destroy this batch instance
   */
  destroy() {
    if (this.destroyed) return;

    this.destroyed = true;
    
    // Resolve any pending promises
    for (const [key, promiseObj] of this._promises.entries()) {
      if (promiseObj && promiseObj.resolve) {
        promiseObj.resolve();
      }
    }
    
    this._promises.clear();
    this._pendingOps.clear();

    // Unreference the database
    if (this.db && this.db._unref) {
      this.db._unref();
    }

    // Free the batch for reuse
    if (this.db && this.db._state) {
      this.db._state.freeBatch(this, this.write);
    }
  }

  /**
   * Try to flush without waiting for completion
   * For compatibility with older code
   */
  tryFlush() {
    this.flush().catch(() => {});
  }

  /**
   * Flush pending operations to database
   * @returns {Promise<void>} Promise that resolves when flush completes
   */
  async flush() {
    if (this.destroyed) {
      return Promise.resolve();
    }

    if (this._pendingOps.size === 0) {
      return Promise.resolve();
    }

    try {
      // Execute the actual flush operation - with timeout protection
      await Promise.race([
        this._executeFlush(),
        new Promise((_, reject) => setTimeout(() => {
          reject(new Error('Flush operation timed out'));
        }, 5000))
      ]);
      
      // Clear operations after successful flush
      this._pendingOps.clear();
      this._promises.clear();
      
      // If autoDestroy is true, destroy this batch after flushing
      if (this.autoDestroy) {
        this.destroy();
      }
    } catch (err) {
      console.error('Error in flush:', err);
      throw err;
    }
  }

  /**
   * Execute a put operation with actual database write
   * @private
   * @param {Map<string, object>} operations - Map of operations to execute
   * @returns {Promise<void>} Promise that resolves when put completes
   */
  async _executeFlush() {
    // If no pending operations, nothing to do
    if (this._pendingOps.size === 0) {
      return;
    }

    // If no database connection, nothing to do
    if (!this.db._state || !this.db._state._db) {
      return;
    }

    // Check if database is suspended
    if (this.db._state._suspended) {
      throw new Error('Database is suspended');
    }

    // Get column family name
    const cfName = this._getCfName();

    try {
      // Create a transaction
      const transaction = this.db._state._db.transaction([cfName], 'readwrite');
      const store = transaction.objectStore(cfName);

      // Process each pending operation
      const operations = Array.from(this._pendingOps.entries());
      
      // First, read all existing values that will be modified
      // This is needed to preserve snapshot consistency
      if (this.db._state._snapshots && this.db._state._snapshots.size > 0) {
        for (const [key, op] of operations) {
          try {
            // Read current value before modification
            const request = store.get(key);
            
            request.onsuccess = async (event) => {
              const currentValue = event.target.result;
              // Record pre-modification value for all active snapshots
              await Snapshot.recordPreModificationValue(
                this.db._state,
                cfName,
                key,
                currentValue
              );
            };
          } catch (err) {
            console.error('Error reading value for snapshot:', err);
          }
        }
      }
      
      // Now process the actual operations
      for (const [key, op] of operations) {
        // Get promise resolver for this operation
        const promiseItem = this._promises.get(key);

        try {
          if (op.type === 'put') {
            store.put(op.value, key);
          } else if (op.type === 'delete') {
            store.delete(key);
          }

          // Resolve the promise
          if (promiseItem) {
            promiseItem.resolve();
          }
        } catch (err) {
          console.error(`Error in ${op.type} operation:`, err);
          
          // Reject the promise
          if (promiseItem && promiseItem.reject) {
            promiseItem.reject(err);
          }
        }
      }

      // Complete transaction
      return new Promise((resolve, reject) => {
        transaction.oncomplete = () => {
          resolve();
        };

        transaction.onerror = (event) => {
          console.error('Transaction error:', event.target.error);
          reject(event.target.error);
        };
      });
    } catch (err) {
      console.error('Error in batch flush:', err);
      throw err;
    }
  }

  /**
   * Get the column family name
   * @private
   * @returns {string} The column family name
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
   * Encode a key for storage
   * @private
   * @param {*} key - Key to encode
   * @returns {*} Encoded key
   */
  _encodeKey(key) {
    if (key === null || key === undefined) {
      return null;
    }

    try {
      if (this.db._keyEncoding) {
        if (typeof this.db._keyEncoding === 'string') {
          // String-based encoding
          if (this.db._keyEncoding === 'utf8' || this.db._keyEncoding === 'json') {
            return key;
          }
          // Default to buffer for other string encodings
          return Buffer.isBuffer(key) ? key : Buffer.from(key);
        } else {
          // compact-encoding
          return c.encode(this.db._keyEncoding, key);
        }
      }

      // If no encoding specified, just use the key as is for IndexedDB
      // But convert Buffer to string for IndexedDB compatibility
      if (Buffer.isBuffer(key)) {
        return key.toString('hex');
      }

      return key;
    } catch (err) {
      console.error('Error encoding key:', err);
      return String(key);
    }
  }

  /**
   * Encode a value for storage
   * @private
   * @param {*} value - Value to encode
   * @returns {*} Encoded value
   */
  _encodeValue(value) {
    if (value === null || value === undefined) {
      return null;
    }

    try {
      if (this.db._valueEncoding) {
        if (typeof this.db._valueEncoding === 'string') {
          // String-based encoding
          if (this.db._valueEncoding === 'utf8' || this.db._valueEncoding === 'json') {
            return value;
          }
          // Default to buffer for other string encodings
          return Buffer.isBuffer(value) ? value : Buffer.from(value);
        } else {
          // compact-encoding
          return c.encode(this.db._valueEncoding, value);
        }
      }

      // If no encoding specified, just use the value as is for IndexedDB
      // But convert Buffer to string for IndexedDB compatibility
      if (Buffer.isBuffer(value)) {
        return value.toString('hex');
      }

      return value;
    } catch (err) {
      console.error('Error encoding value:', err);
      return String(value);
    }
  }

  /**
   * Decode a value from storage
   * @private
   * @param {*} value - Value to decode
   * @returns {*} Decoded value
   */
  _decodeValue(value) {
    if (value === null || value === undefined) {
      return null;
    }

    try {
      if (this.db._valueEncoding) {
        if (typeof this.db._valueEncoding === 'string') {
          // String-based encoding
          if (this.db._valueEncoding === 'utf8') {
            return value;
          } else if (this.db._valueEncoding === 'json') {
            return value;
          }
          // Default to Buffer for other string encodings
          return Buffer.from(value);
        } else {
          // compact-encoding
          return c.decode(this.db._valueEncoding, value);
        }
      }

      // If no encoding specified, default to Buffer for compatibility with RocksDB
      return Buffer.from(String(value));
    } catch (err) {
      console.error('Error decoding value:', err);
      return Buffer.from(String(value || ''));
    }
  }

  /**
   * Put a key-value pair without waiting for completion
   * @param {*} key - Key to put
   * @param {*} value - Value to put
   */
  tryPut(key, value) {
    if (this.destroyed) {
      return;
    }

    if (!this.write) {
      return;
    }

    // Encode key and value
    const encodedKey = this._encodeKey(key);
    const encodedValue = this._encodeValue(value);

    // Store in pending operations
    this._pendingOps.set(encodedKey, {
      type: 'put',
      value: encodedValue
    });

    // Auto-flush if requested
    if (this.autoDestroy) {
      this.tryFlush();
    }
  }

  /**
   * Delete a key-value pair without waiting for completion
   * @param {*} key - Key to delete
   */
  tryDelete(key) {
    if (this.destroyed) {
      return;
    }

    if (!this.write) {
      return;
    }

    // Encode key
    const encodedKey = this._encodeKey(key);

    // Store in pending operations
    this._pendingOps.set(encodedKey, {
      type: 'delete'
    });

    // Auto-flush if requested
    if (this.autoDestroy) {
      this.tryFlush();
    }
  }

  /**
   * Delete a range of keys without waiting for completion
   * @param {*} start - Start key
   * @param {*} end - End key
   */
  tryDeleteRange(start, end) {
    if (this.destroyed) {
      return;
    }

    if (!this.write) {
      return;
    }

    // Execute delete range without waiting
    this.deleteRange(start, end);
  }
}

export default Batch; 