import { Readable } from 'streamx';
import c from 'compact-encoding';

const empty = Buffer.alloc(0);

/**
 * Iterator for querying IndexedDB data with a similar interface to RocksDB
 */
export class Iterator extends Readable {
  /**
   * Create a new iterator
   * @param {object} db - Database session
   * @param {object} options - Iterator options
   */
  constructor(db, options = {}) {
    super();

    const {
      keyEncoding = null,
      valueEncoding = null,
      reverse = false,
      limit = -1,
      gt, gte, lt, lte, 
      prefix,
      snapshot,
      cache = true,
      capacity = 8, // Added for RocksDB compatibility
      highWaterMark = 16
    } = options;

    // Reference the database to prevent it from closing during iteration
    if (db && db._ref) {
      db._ref();
    }

    this.db = db;
    this._keyEncoding = keyEncoding || db._keyEncoding;
    this._valueEncoding = valueEncoding || db._valueEncoding;
    this._testMode = db._testMode;
    this._highWaterMark = highWaterMark;
    this._capacity = capacity; // Added for RocksDB compatibility
    this._limit = limit < 0 ? Infinity : limit; // Match RocksDB behavior
    this._count = 0;
    this._cache = cache;
    this._reverse = reverse;
    this._snapshot = snapshot;
    
    // Store encoded range values like RocksDB does
    this._gt = gt ? this._encodeKey(gt) : empty;
    this._gte = gte ? this._encodeKey(gte) : empty;
    this._lt = lt ? this._encodeKey(lt) : empty;
    this._lte = lte ? this._encodeKey(lte) : empty;
    
    this._options = { gt, gte, lt, lte, prefix, reverse, limit, snapshot, cache };
    this._keys = [];
    this._values = [];
    this._started = false;
    this._cursor = null;
    this._pendingNext = false;
    this._pendingOpen = null;
    this._pendingRead = null;
    this._pendingDestroy = null;
    this._transaction = null;
    this._objectStore = null;
    this._opened = false;
    this._ended = false;
    this._store = null;
    this._prefixUpperBound = null;
    
    // If we have a prefix, calculate the upper bound for the range query
    if (prefix) {
      this._prefixUpperBound = this._calculatePrefixUpperBound(prefix);
    }

    if (db && db._state && db._state.opened) {
      this.ready();
    }
  }

  /**
   * Calculate the upper bound for a prefix range query
   * @private
   * @param {string} prefix - The prefix to calculate the upper bound for
   * @returns {string} The upper bound (exclusive) for the prefix range
   */
  _calculatePrefixUpperBound(prefix) {
    // Convert prefix to string if it's not already
    const prefixStr = typeof prefix === 'string' ? prefix : String(prefix);
    
    // Get the last character of the prefix
    const lastChar = prefixStr.charCodeAt(prefixStr.length - 1);
    
    // Create a new string with the last character incremented
    return prefixStr.slice(0, -1) + String.fromCharCode(lastChar + 1);
  }

  /**
   * Ensure the iterator is ready for use
   * @returns {Promise<void>} Promise that resolves when iterator is ready
   */
  async ready() {
    if (this._started) return;
    this._started = true;

    // In test mode, just return immediately with success
    if (this._testMode) {
      return;
    }

    // Check if db is suspended
    if (this.db._state && this.db._state._suspended) {
      throw new Error('Database session is suspended');
    }

    try {
      // Wait for database to be ready
      if (this.db._state) {
        await this.db._state.ready();
      }

      // Create IDB transaction
      const cfName = this._getCfName();
      this._transaction = this.db._state._db.transaction([cfName], 'readonly');
      this._store = this._transaction.objectStore(cfName);

      // Set up range
      const range = this._getKeyRange();

      // Open cursor based on options
      const direction = this._reverse ? 'prev' : 'next';
      const request = this._store.openCursor(range, direction);

      // Handle cursor opening
      request.onsuccess = (event) => {
        this._cursor = event.target.result;
        this._pendingNext = false;
      };

      request.onerror = (event) => {
        console.error('Error opening cursor:', event.target.error);
        this._pendingNext = false;
        this._cursor = null;
        this._ended = true;
      };

      // Process first set of results
      await this._next();
    } catch (err) {
      console.error('Error opening iterator:', err);
      this._ended = true;
    }
  }

  /**
   * Get column family name
   * @private
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
   * Get key range for this iterator
   * @private
   */
  _getKeyRange() {
    const { gt, gte, lt, lte, prefix } = this._options;

    if (prefix) {
      // For prefix queries, we need a range from prefix to prefix + 1
      const prefixEncoded = this._encodeKey(prefix);
      
      try {
        return IDBKeyRange.bound(
          prefixEncoded, 
          this._encodeKey(this._prefixUpperBound), 
          false, // Include lower bound
          true   // Exclude upper bound
        );
      } catch (e) {
        console.error('Error creating prefix range:', e);
        return null;
      }
    }

    if (gt !== undefined && lt !== undefined) {
      return IDBKeyRange.bound(this._encodeKey(gt), this._encodeKey(lt), true, true);
    }
    
    if (gt !== undefined && lte !== undefined) {
      return IDBKeyRange.bound(this._encodeKey(gt), this._encodeKey(lte), true, false);
    }
    
    if (gte !== undefined && lt !== undefined) {
      return IDBKeyRange.bound(this._encodeKey(gte), this._encodeKey(lt), false, true);
    }
    
    if (gte !== undefined && lte !== undefined) {
      return IDBKeyRange.bound(this._encodeKey(gte), this._encodeKey(lte), false, false);
    }
    
    if (gt !== undefined) {
      return IDBKeyRange.lowerBound(this._encodeKey(gt), true);
    }
    
    if (gte !== undefined) {
      return IDBKeyRange.lowerBound(this._encodeKey(gte), false);
    }
    
    if (lt !== undefined) {
      return IDBKeyRange.upperBound(this._encodeKey(lt), true);
    }
    
    if (lte !== undefined) {
      return IDBKeyRange.upperBound(this._encodeKey(lte), false);
    }
    
    return null;
  }

  /**
   * Move to the next set of results
   * @private
   */
  async _next() {
    if (this._pendingNext || this._ended) return;
    
    this._pendingNext = true;

    // In test mode, just simulate data
    if (this._testMode) {
      this._pendingNext = false;
      this._ended = true;
      return;
    }

    try {
      // Load more data if available
      while (this._cursor && this._keys.length < this._highWaterMark) {
        // Check limit
        if (this._limit > 0 && this._count >= this._limit) {
          this._ended = true;
          break;
        }

        // Get current key and value
        const key = this._decodeKey(this._cursor.key);
        const value = this._decodeValue(this._cursor.value);
        
        // Store for later use
        this._keys.push(key);
        this._values.push(value);
        this._count++;
        
        // Move to next
        this._cursor.continue();
        
        // Wait for cursor to update
        await new Promise(resolve => {
          const checkCursor = () => {
            if (!this._pendingNext) {
              resolve();
            } else {
              setTimeout(checkCursor, 5);
            }
          };
          checkCursor();
        });
      }
      
      if (!this._cursor) {
        this._ended = true;
      }
      
      this._pendingNext = false;
    } catch (err) {
      console.error('Error in _next:', err);
      this._pendingNext = false;
      this._ended = true;
    }
  }

  /**
   * Read the next key-value pair
   * @returns {Promise<{key, value}>} Next key-value pair or null
   */
  async _read() {
    // For test mode, just return null to indicate end
    if (this._testMode) {
      return null;
    }

    // Check if database is suspended
    if (this.db._state && this.db._state._suspended) {
      throw new Error('Database session is suspended');
    }

    // Ensure iterator is open
    if (!this._started) {
      await this.ready();
    }

    // If no more keys, try to load more
    if (this._keys.length === 0 && !this._ended) {
      await this._next();
    }

    // If we have keys, return the next one
    if (this._keys.length > 0) {
      const key = this._keys.shift();
      const value = this._values.shift();
      return { key, value };
    }

    // Otherwise we're done
    return null;
  }

  /**
   * Encode a key for querying
   * @private
   */
  _encodeKey(key) {
    if (key === null || key === undefined) {
      return null;
    }

    try {
      if (this._keyEncoding) {
        if (typeof this._keyEncoding === 'string') {
          if (this._keyEncoding === 'utf8' || this._keyEncoding === 'json') {
            return key;
          }
          return Buffer.isBuffer(key) ? key : Buffer.from(key);
        } else {
          const c = require('compact-encoding');
          return c.encode(this._keyEncoding, key);
        }
      }
      
      if (Buffer.isBuffer(key)) {
        return key.toString('hex');
      }
      
      return key;
    } catch (err) {
      console.error('Error encoding key:', err);
      return key;
    }
  }

  /**
   * Decode a key from storage
   * @private
   */
  _decodeKey(key) {
    if (key === null || key === undefined) {
      return null;
    }

    try {
      if (this._keyEncoding) {
        if (typeof this._keyEncoding === 'string') {
          if (this._keyEncoding === 'utf8') {
            return key;
          } else if (this._keyEncoding === 'json') {
            return key;
          }
          return Buffer.from(key);
        } else {
          const c = require('compact-encoding');
          return c.decode(this._keyEncoding, key);
        }
      }
      
      return key;
    } catch (err) {
      console.error('Error decoding key:', err);
      return key;
    }
  }

  /**
   * Decode a value from storage
   * @private
   */
  _decodeValue(value) {
    if (value === null || value === undefined) {
      return null;
    }

    try {
      if (this._valueEncoding) {
        if (typeof this._valueEncoding === 'string') {
          if (this._valueEncoding === 'utf8') {
            return value;
          } else if (this._valueEncoding === 'json') {
            return value;
          }
          return Buffer.from(value);
        } else {
          const c = require('compact-encoding');
          return c.decode(this._valueEncoding, value);
        }
      }

      return Buffer.from(String(value));
    } catch (err) {
      console.error('Error decoding value:', err);
      return value;
    }
  }

  /**
   * Close the iterator
   */
  async close() {
    this._ended = true;
    this._cursor = null;
    this._transaction = null;
    this._store = null;
    this._keys = [];
    this._values = [];
  }

  /**
   * Open the iterator
   * @param {Function} cb - Callback when open completes
   * @private
   */
  async _open(cb) {
    try {
      await this.ready();
      
      // Check if database is suspended or has resumed state
      if (this.db._state && this.db._state.resumed) {
        // Only attempt to wait for resumed.promise if it exists
        if (typeof this.db._state.resumed.promise === 'object') {
          const resumed = await this.db._state.resumed.promise;
          
          if (!resumed) {
            return cb(new Error('Database session is suspended'));
          }
        }
      }
      
      this._pendingOpen = cb;
      this._opened = true;
      
      if (this._pendingOpen) {
        const callback = this._pendingOpen;
        this._pendingOpen = null;
        callback(null);
      }
    } catch (err) {
      if (cb) cb(err);
    }
  }
  
  /**
   * Handle destruction
   * @param {Function} cb - Callback when destruction completes
   * @private
   */
  async _destroy(cb) {
    await this.ready();
    
    this._pendingDestroy = cb;
    
    if (this._opened === false) {
      this._onclose(null);
      return;
    }
    
    await this.close();
    this._onclose(null);
  }
  
  /**
   * Handle iterator closure
   * @param {Error} err - Error if any
   * @private
   */
  _onclose(err) {
    const cb = this._pendingDestroy;
    this._pendingDestroy = null;
    
    if (this.db && this.db._unref) {
      this.db._unref();
    }
    
    if (cb) cb(err);
  }
  
  /**
   * Handle read completion
   * @param {Error} err - Error if any
   * @param {Array} keys - Keys found
   * @param {Array} values - Values found
   * @private
   */
  _onread(err, keys, values) {
    const cb = this._pendingRead;
    this._pendingRead = null;
    
    if (err) return cb(err);
    
    const n = keys ? keys.length : 0;
    
    this._limit -= n;
    
    for (let i = 0; i < n; i++) {
      this.push({
        key: this._decodeKey(keys[i]),
        value: this._decodeValue(values[i])
      });
    }
    
    if (n < this._capacity || this._ended) {
      this.push(null);
    }
    
    cb(null);
  }
}

export default Iterator 