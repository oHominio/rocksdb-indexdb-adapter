import c from "compact-encoding";

// Browser-compatible Readable stream implementation
class Readable {
  constructor(options = {}) {
    this._reading = false;
    this._destroyed = false;
    this._ended = false;
    this._autoDestroy = !!options.autoDestroy;
    this._highWaterMark = options.highWaterMark || 16;
    this._ondrain = null;
    this._onfinish = null;
    this._onclose = null;
    this._pendingDestroy = null;
    this._pendingRead = null;
    this._pendingOpen = null;
    this._opened = false;
  }

  _read() {} // To be overridden by subclasses

  async read() {
    if (this._reading || this._ended) return null;
    this._reading = true;

    try {
      await this._read();
    } catch (err) {
      this.destroy(err);
    }

    this._reading = false;
    return null;
  }

  push(data) {
    if (this._destroyed) return;
    if (data === null) {
      this._ended = true;
      if (this._ondrain) this._ondrain();
      if (this._autoDestroy) this.destroy();
      if (this._onfinish) this._onfinish();
      return;
    }
    return true;
  }

  destroy(err) {
    if (this._destroyed) return;
    this._destroyed = true;
    this._ended = true;

    if (this._pendingDestroy) {
      const callback = this._pendingDestroy;
      this._pendingDestroy = null;
      callback(err || null);
    }

    if (this._onclose) {
      this._onclose(err || null);
    }
  }

  on(event, fn) {
    if (event === "drain") this._ondrain = fn;
    else if (event === "finish") this._onfinish = fn;
    else if (event === "close") this._onclose = fn;
    return this;
  }

  off(event, fn) {
    if (event === "drain" && this._ondrain === fn) this._ondrain = null;
    else if (event === "finish" && this._onfinish === fn) this._onfinish = null;
    else if (event === "close" && this._onclose === fn) this._onclose = null;
    return this;
  }

  once(event, fn) {
    const onEvent = (...args) => {
      this.off(event, onEvent);
      fn(...args);
    };
    return this.on(event, onEvent);
  }
}

const empty = Buffer.alloc(0);

/**
 * Helper function to create buffer from string
 * @param {string} str - String to convert
 * @returns {Buffer} Buffer
 */
function bufferFrom(str) {
  return Buffer.from(str);
}

/**
 * Iterator for querying IndexedDB data with a similar interface to RocksDB
 */
export class Iterator extends Readable {
  /**
   * Create a new Iterator for accessing data
   * @constructor
   * @param {object} db - The database instance
   * @param {object} options - Iterator options
   * @returns {Iterator} A new Iterator instance
   */
  constructor(db, options = {}) {
    super();

    const {
      keyEncoding = null,
      valueEncoding = null,
      reverse = false,
      limit = -1,
      gt,
      gte,
      lt,
      lte,
      prefix,
      snapshot,
      cache = true,
      capacity = 8,
      highWaterMark = 16,
    } = options;

    // Reference the database to prevent it from closing during iteration
    if (db && db._ref) {
      db._ref();
    }

    this.db = db;
    this._keyEncoding = keyEncoding || db._keyEncoding;
    this._valueEncoding = valueEncoding || db._valueEncoding;
    this._highWaterMark = highWaterMark;
    this._capacity = capacity;
    this._limit = limit < 0 ? Infinity : limit;
    this._reverse = reverse;

    this._options = options;
    this._started = false;
    this._destroyed = false;
    this._allEntries = null;
    this._entryIndex = 0;

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
    const prefixStr = typeof prefix === "string" ? prefix : String(prefix);

    // Get the last character of the prefix
    const lastChar = prefixStr.charCodeAt(prefixStr.length - 1);

    // Create a new string with the last character incremented
    return prefixStr.slice(0, -1) + String.fromCharCode(lastChar + 1);
  }

  /**
   * Start the iterator.
   * This initializes the iterator and makes it ready to use.
   * @returns {Promise<void>} Promise that resolves when the iterator is ready
   */
  async ready() {
    if (this.destroyed) throw new Error("Iterator is destroyed");
    if (this._started) return;

    // Mark that we've started
    this._started = true;

    // Increment reference counter
    if (this.db && this.db._ref) {
      this.db._ref();
    }

    // Initialize the snapshot if using one
    if (this.db._snapshot && !this.db._snapshot._initialized) {
      await this.db._snapshot._init();
    }

    // Open a transaction and get entries
    await this._openCursor();

    return;
  }

  /**
   * Open a cursor to iterate over the keys
   * @private
   * @returns {Promise<void>} Promise that resolves when the cursor is ready
   */
  async _openCursor() {
    try {
      // Ensure database is ready
      await this.db._state.ready();

      // Get the database and store
      const db = this.db._state._db;
      if (!db) throw new Error("Database is closed");

      // Get the store name
      const storeName = this._getCfName();

      // Create key range based on options
      const keyRange = this._getKeyRange();

      // Load all entries at once for better IndexedDB performance
      await this._loadEntriesAhead(db, storeName, keyRange);
    } catch (err) {
      console.error("Error opening cursor:", err);
      this._error = err;
    }
  }

  /**
   * Load all entries ahead of time
   * @private
   * @param {IDBDatabase} db - The IndexedDB database
   * @param {string} storeName - The name of the object store
   * @param {IDBKeyRange} keyRange - The key range to query
   * @returns {Promise<void>} Promise that resolves when entries are loaded
   */
  async _loadEntriesAhead(db, storeName, keyRange) {
    // Load entries into an array for better browser compat
    this._allEntries = [];
    this._entryIndex = 0;

    try {
      // Determine which store to use - if we have a snapshot, use its store
      const snapshot = this._options.snapshot;
      const useSnapshotStore = snapshot && this._options.snapshotStoreName;
      const targetStore = useSnapshotStore
        ? this._options.snapshotStoreName
        : storeName;

      // Create a new transaction for reading
      const transaction = db.transaction([targetStore], "readonly");
      const store = transaction.objectStore(targetStore);

      // Use a promise to handle the async cursor
      await new Promise((resolve, reject) => {
        try {
          // Direction based on reverse option
          const direction = this._reverse ? "prev" : "next";

          // Open the cursor with our range
          const request = store.openCursor(keyRange, direction);

          request.onsuccess = (event) => {
            const cursor = event.target.result;

            if (cursor) {
              // Stop if we've hit the limit
              if (this._limit > 0 && this._allEntries.length >= this._limit) {
                resolve();
                return;
              }

              // Store key and value
              this._allEntries.push({
                key: cursor.key,
                value: cursor.value,
              });

              // Continue to next entry
              cursor.continue();
            } else {
              // No more entries
              resolve();
            }
          };

          request.onerror = (event) => {
            reject(event.target.error);
          };
        } catch (err) {
          reject(err);
        }
      });

      // Sort entries if needed
      this._sortEntries();

      // Convert entries to the correct format with encoding
      for (let i = 0; i < this._allEntries.length; i++) {
        const entry = this._allEntries[i];

        // Apply encoding to keys and values
        if (this._keyEncoding || this._valueEncoding) {
          this._allEntries[i] = {
            key: this._decodeKey(entry.key),
            value: this._decodeValue(entry.value),
          };
        } else {
          // Ensure consistent Buffer format
          this._allEntries[i] = {
            key: Buffer.isBuffer(entry.key)
              ? entry.key
              : bufferFrom(String(entry.key)),
            value: Buffer.isBuffer(entry.value)
              ? entry.value
              : bufferFrom(String(entry.value)),
          };
        }
      }
    } catch (err) {
      console.error("Error loading entries:", err);
      this._error = err;
    }
  }

  /**
   * Sort entries based on iterator options
   * @private
   */
  _sortEntries() {
    if (this._allEntries.length <= 1) return;

    this._allEntries.sort((a, b) => {
      // Convert both keys to strings for consistent comparison
      const aStr = String(a.key);
      const bStr = String(b.key);

      // Sort based on direction
      if (this._reverse) {
        return bStr.localeCompare(aStr);
      } else {
        return aStr.localeCompare(bStr);
      }
    });
  }

  /**
   * Get the iterator value as an async iterator
   * This allows using for-await-of syntax with the iterator
   * @returns {AsyncIterator} Async iterator interface
   */
  [Symbol.asyncIterator]() {
    const self = this;

    return {
      async next() {
        // Initialize if needed
        if (!self._started) {
          try {
            await self.ready();
          } catch (err) {
            console.error("Failed to initialize iterator:", err);
            return { done: true };
          }
        }

        try {
          // Check if we're at the end of entries
          if (
            !self._allEntries ||
            self._entryIndex >= self._allEntries.length
          ) {
            return { done: true };
          }

          // Get the current entry and advance index
          const entry = self._allEntries[self._entryIndex++];

          // Format and return the result with proper decoding
          const decodedKey = self._decodeKey(entry.key);
          const decodedValue = self._decodeValue(entry.value);

          return {
            done: false,
            value: {
              key: decodedKey,
              value: decodedValue,
            },
          };
        } catch (err) {
          console.error("Error in iterator next:", err);
          return { done: true };
        }
      },
    };
  }

  /**
   * Get encoding options from the database
   * @private
   */
  _setupEncodings() {
    // Get encodings from database or options
    this._keyEncoding = this._options.keyEncoding || this.db._keyEncoding;
    this._valueEncoding = this._options.valueEncoding || this.db._valueEncoding;

    // Add helper method for consistent buffer conversion
    this._ensureBuffer = (value) => {
      if (value === null || value === undefined) return null;
      return Buffer.isBuffer(value) ? value : Buffer.from(String(value));
    };
  }

  /**
   * Decode a key from storage format based on encoding setting
   * @private
   * @param {*} key - The key to decode
   * @returns {*} The decoded key
   */
  _decodeKey(key) {
    if (key === null || key === undefined) {
      return null;
    }

    // Setup encodings if not done already
    if (!this._keyEncoding && this.db) {
      this._setupEncodings();
    }

    // Handle different encoding types
    if (this._keyEncoding === "utf8" || this._keyEncoding === "ascii") {
      // Return string for string encodings
      return typeof key === "string"
        ? key
        : Buffer.from(key).toString(this._keyEncoding);
    } else if (this._keyEncoding === "json") {
      // Parse JSON if needed
      if (typeof key === "string") {
        try {
          return JSON.parse(key);
        } catch (e) {
          // If it's not valid JSON, return as is
          return key;
        }
      }
      return key;
    } else if (
      this._keyEncoding === "binary" ||
      this._keyEncoding === "buffer"
    ) {
      // Convert to Buffer for binary encodings
      return Buffer.isBuffer(key) ? key : Buffer.from(String(key));
    }

    // For custom encodings with decode method (like compact-encoding)
    if (typeof this._keyEncoding === "object" && this._keyEncoding.decode) {
      try {
        return this._keyEncoding.decode(
          Buffer.isBuffer(key) ? key : Buffer.from(String(key))
        );
      } catch (err) {
        console.error("Error decoding key with custom encoding:", err);
      }
    }

    // Default to buffer for any other case
    return Buffer.isBuffer(key) ? key : Buffer.from(String(key));
  }

  /**
   * Decode a value from storage format based on encoding setting
   * @private
   * @param {*} value - The value to decode
   * @returns {*} The decoded value
   */
  _decodeValue(value) {
    if (value === null || value === undefined) {
      return null;
    }

    // Setup encodings if not done already
    if (!this._valueEncoding && this.db) {
      this._setupEncodings();
    }

    // Handle different encoding types
    if (this._valueEncoding === "utf8" || this._valueEncoding === "ascii") {
      // Return string for string encodings
      return typeof value === "string"
        ? value
        : Buffer.from(value).toString(this._valueEncoding);
    } else if (this._valueEncoding === "json") {
      // Parse JSON if needed
      if (typeof value === "string") {
        try {
          return JSON.parse(value);
        } catch (e) {
          // If it's not valid JSON, return as is
          return value;
        }
      }
      return value;
    } else if (
      this._valueEncoding === "binary" ||
      this._valueEncoding === "buffer"
    ) {
      // Convert to Buffer for binary encodings
      return Buffer.isBuffer(value) ? value : Buffer.from(String(value));
    }

    // For custom encodings with decode method (like compact-encoding)
    if (typeof this._valueEncoding === "object" && this._valueEncoding.decode) {
      try {
        return this._valueEncoding.decode(
          Buffer.isBuffer(value) ? value : Buffer.from(String(value))
        );
      } catch (err) {
        console.error("Error decoding value with custom encoding:", err);
      }
    }

    // Default to buffer for any other case
    return Buffer.isBuffer(value) ? value : Buffer.from(String(value));
  }

  /**
   * Get column family name
   * @private
   */
  _getCfName() {
    if (!this.db || !this.db._columnFamily) {
      return "default";
    }

    return typeof this.db._columnFamily === "string"
      ? this.db._columnFamily
      : this.db._columnFamily.name || "default";
  }

  /**
   * Get key range for this iterator
   * @private
   * @returns {IDBKeyRange|null} Key range for IndexedDB query
   */
  _getKeyRange() {
    const { gt, gte, lt, lte, prefix } = this._options;

    let lowerBound = undefined;
    let upperBound = undefined;
    let lowerExclusive = false;
    let upperExclusive = false;

    // Set bounds based on prefix if specified
    if (prefix) {
      lowerBound = prefix;
      lowerExclusive = false;

      // Calculate upper bound for prefix: prefix + next char
      const prefixStr = String(prefix);
      const lastChar = prefixStr.charCodeAt(prefixStr.length - 1);
      upperBound = prefixStr.slice(0, -1) + String.fromCharCode(lastChar + 1);
      upperExclusive = true;
    } else {
      // Set bounds based on gt/gte/lt/lte
      if (gt !== undefined) {
        lowerBound = gt;
        lowerExclusive = true;
      } else if (gte !== undefined) {
        lowerBound = gte;
        lowerExclusive = false;
      }

      if (lt !== undefined) {
        upperBound = lt;
        upperExclusive = true;
      } else if (lte !== undefined) {
        upperBound = lte;
        upperExclusive = false;
      }
    }

    // Create IDBKeyRange based on bounds
    try {
      if (lowerBound !== undefined && upperBound !== undefined) {
        return IDBKeyRange.bound(
          lowerBound,
          upperBound,
          lowerExclusive,
          upperExclusive
        );
      } else if (lowerBound !== undefined) {
        return IDBKeyRange.lowerBound(lowerBound, lowerExclusive);
      } else if (upperBound !== undefined) {
        return IDBKeyRange.upperBound(upperBound, upperExclusive);
      }
    } catch (e) {
      console.error("Error creating key range:", e);
    }

    // No range specified
    return null;
  }

  /**
   * Encode a value using the specified encoding
   * @private
   * @param {*} value - The value to encode
   * @returns {*} The encoded value
   */
  _encodeValue(value) {
    if (value == null) return null;

    if (
      this._valueEncoding === "utf8" ||
      this._valueEncoding === "json" ||
      this._valueEncoding === "ascii"
    ) {
      return value.toString();
    }

    if (
      this._valueEncoding &&
      typeof this._valueEncoding === "object" &&
      this._valueEncoding.encode
    ) {
      const buf = Buffer.alloc(8192); // pre-allocate
      const state = { start: 0, end: buf.length, buffer: buf };
      this._valueEncoding.encode(value, state);
      return Buffer.from(state.buffer.subarray(0, state.start));
    }

    // For other cases, make sure we return a Buffer as expected by tests
    return Buffer.isBuffer(value) ? value : bufferFrom(String(value));
  }

  /**
   * Encode a key for querying
   * @private
   * @param {*} key - The key to encode
   * @returns {*} The encoded key
   */
  _encodeKey(key) {
    if (key === null || key === undefined) {
      return null;
    }

    try {
      // Handle compact-encoding types
      if (typeof this._keyEncoding === "object" && this._keyEncoding.encode) {
        // Using compact-encoding
        const buf = Buffer.alloc(
          this._keyEncoding.encodingLength
            ? this._keyEncoding.encodingLength(key)
            : 8192
        ); // pre-allocate reasonable buffer
        const state = { start: 0, end: buf.length, buffer: buf };
        this._keyEncoding.encode(key, state);
        return Buffer.from(state.buffer.subarray(0, state.start));
      }

      // Handle string encodings
      if (
        this._keyEncoding === "utf8" ||
        this._keyEncoding === "json" ||
        this._keyEncoding === "ascii"
      ) {
        return typeof key === "string" ? key : key.toString(this._keyEncoding);
      }

      // Handle binary/buffer encodings
      if (this._keyEncoding === "binary" || this._keyEncoding === "buffer") {
        return Buffer.isBuffer(key) ? key : Buffer.from(String(key));
      }

      // Default behavior
      if (Buffer.isBuffer(key)) {
        return key;
      }

      return String(key);
    } catch (err) {
      console.error("Error encoding key:", err);
      return key;
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
        if (typeof this.db._state.resumed.promise === "object") {
          const resumed = await this.db._state.resumed.promise;

          if (!resumed) {
            return cb(new Error("Database session is suspended"));
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
        value: this._decodeValue(values[i]),
      });
    }

    if (n < this._capacity || this._ended) {
      this.push(null);
    }

    cb(null);
  }
}

export default Iterator;
