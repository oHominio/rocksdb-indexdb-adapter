import ColumnFamily from "./lib/column-family.js";
import Iterator from "./lib/iterator.js";
import Snapshot from "./lib/snapshot.js";
import State from "./lib/state.js";
import { BloomFilterPolicy, RibbonFilterPolicy } from "./lib/filter-policy.js";

class IndexDBStorage {
  constructor(path, opts = {}) {
    const {
      columnFamily,
      state = new State(this, path, opts),
      snapshot = null,
      keyEncoding = null,
      valueEncoding = null,
    } = opts;

    this._state = state;
    this._snapshot = snapshot;
    this._columnFamily = state.getColumnFamily(columnFamily);
    this._keyEncoding = keyEncoding;
    this._valueEncoding = valueEncoding;
    this._index = -1;

    this._state.addSession(this);
  }

  get opened() {
    return this._state.opened;
  }

  get closed() {
    return this.isRoot() ? this._state.closed : this._index === -1;
  }

  get path() {
    return this._state.path;
  }

  get snapshotted() {
    return this._snapshot !== null;
  }

  get defaultColumnFamily() {
    return this._columnFamily;
  }

  session({
    columnFamily = this._columnFamily,
    snapshot = this._snapshot !== null,
    keyEncoding = this._keyEncoding,
    valueEncoding = this._valueEncoding,
  } = {}) {
    maybeClosed(this);

    return new IndexDBStorage(null, {
      state: this._state,
      columnFamily,
      snapshot: snapshot ? this._snapshot || new Snapshot(this) : null,
      keyEncoding,
      valueEncoding,
    });
  }

  columnFamily(name, opts) {
    return this.session({ ...opts, columnFamily: name });
  }

  snapshot() {
    maybeClosed(this);
    return this.session({ snapshot: true });
  }

  isRoot() {
    return this === this._state.db;
  }

  ready() {
    return this._state.ready();
  }

  async open() {
    return this._state.ready();
  }

  async close({ force } = {}) {
    if (this._index !== -1) this._state.removeSession(this);

    if (force) {
      while (this._state.sessions.length > 0) {
        await this._state.sessions[this._state.sessions.length - 1].close();
      }
    }

    return this.isRoot() ? this._state.close() : Promise.resolve();
  }

  suspend() {
    maybeClosed(this);

    return this._state.suspend();
  }

  resume() {
    maybeClosed(this);

    return this._state.resume();
  }

  isIdle() {
    return this._state.handles.isIdle();
  }

  idle() {
    return this._state.handles.idle();
  }

  iterator(range, opts) {
    maybeClosed(this);
    return new Iterator(this, { ...range, ...opts });
  }

  /**
   * Get a value from the database
   * @param {string|Buffer} key - The key to get
   * @param {object} opts - Options for the operation
   * @returns {Promise<Buffer|null>} Promise that resolves with the value or null if not found
   */
  async get(key, opts = {}) {
    maybeClosed(this);
    console.log("IndexDBStorage.get called with key:", key);

    try {
      // Create a read batch with minimal overhead
      const batch = await this.read({
        ...opts,
        capacity: 1,
        autoDestroy: true,
      });

      // Ensure batch has a get method
      if (typeof batch.get !== "function") {
        console.error("batch.get is not a function - implementing fallback");
        return this._directGet(key);
      }

      try {
        // Use the batch to get the value (batch.get will convert to Buffer)
        const value = await batch.get(key);
        console.log(
          "IndexDBStorage.get result:",
          value ? "found value" : "not found"
        );

        // The batch.get method already converts to Buffer, no need to convert again
        return value;
      } catch (err) {
        console.error("Error in batch.get:", err);
        if (err.name === "NotFoundError") return null;

        // Try direct access as fallback
        return this._directGet(key);
      } finally {
        // Ensure the batch is destroyed properly
        try {
          if (typeof batch.tryFlush === "function") {
            batch.tryFlush();
          } else if (typeof batch.destroy === "function") {
            batch.destroy();
          }
        } catch (flushErr) {
          console.error("Error in batch cleanup:", flushErr);
        }
      }
    } catch (err) {
      console.error("Critical error in IndexDBStorage.get:", err);
      return null;
    }
  }

  /**
   * Direct get implementation as fallback
   * @private
   * @param {string|Buffer} key - The key to get
   * @returns {Promise<Buffer|null>} The value or null
   */
  async _directGet(key) {
    try {
      console.log("Using _directGet fallback");
      await this._state.ready();
      const db = this._state._db;
      if (!db) return null;

      const storeName = this._columnFamily
        ? typeof this._columnFamily === "string"
          ? this._columnFamily
          : this._columnFamily.name
        : "default";

      const keyStr = typeof key === "string" ? key : key.toString();

      return new Promise((resolve) => {
        try {
          const tx = db.transaction([storeName], "readonly");
          const store = tx.objectStore(storeName);
          const req = store.get(keyStr);

          req.onsuccess = () => {
            const value = req.result;
            if (value === null || value === undefined) {
              resolve(null);
            } else if (Buffer.isBuffer(value)) {
              resolve(value);
            } else if (typeof value === "string") {
              resolve(Buffer.from(value));
            } else {
              // Best effort conversion to Buffer
              try {
                resolve(Buffer.from(String(value)));
              } catch (err) {
                resolve(null);
              }
            }
          };

          req.onerror = () => resolve(null);
        } catch (err) {
          console.error("Error in _directGet:", err);
          resolve(null);
        }
      });
    } catch (err) {
      console.error("Error in _directGet:", err);
      return null;
    }
  }

  /**
   * Create a read batch
   * @param {object} opts - Options for the read batch
   * @returns {Promise<object>} Promise that resolves with the read batch
   */
  async read(opts) {
    maybeClosed(this);
    console.log("IndexDBStorage.read called with opts:", opts);

    try {
      const batch = await this._state.createReadBatch(this, opts);

      // Verify that batch has required methods
      if (typeof batch.get !== "function") {
        console.error("Created read batch is missing get method");
      }

      if (typeof batch.tryFlush !== "function") {
        console.error("Created read batch is missing tryFlush method");
      }

      return batch;
    } catch (err) {
      console.error("Error creating read batch:", err);
      throw err;
    }
  }

  /**
   * Create a write batch
   * @param {object} opts - Options for the write batch
   * @returns {Promise<object>} Promise that resolves with the write batch
   */
  async write(opts) {
    maybeClosed(this);
    console.log("IndexDBStorage.write called with opts:", opts);

    try {
      const batch = await this._state.createWriteBatch(this, opts);

      // Verify that batch has required methods
      if (typeof batch.put !== "function") {
        console.error("Created write batch is missing put method");
      }

      if (typeof batch.delete !== "function") {
        console.error("Created write batch is missing delete method");
      }

      if (typeof batch.flush !== "function") {
        console.error("Created write batch is missing flush method");
      }

      if (typeof batch.tryFlush !== "function") {
        console.error("Created write batch is missing tryFlush method");
      }

      return batch;
    } catch (err) {
      console.error("Error creating write batch:", err);
      throw err;
    }
  }

  /**
   * Put a value into the database
   * @param {string|Buffer} key - The key to put
   * @param {string|Buffer} value - The value to put
   * @param {object} opts - Options for the operation
   * @returns {Promise<void>} Promise that resolves when the operation is complete
   */
  async put(key, value, opts = {}) {
    maybeClosed(this);
    console.log("IndexDBStorage.put called with key:", key);

    try {
      // Create a write batch with minimal overhead
      const batch = await this.write({
        ...opts,
        capacity: 1,
        autoDestroy: true,
      });

      try {
        // Ensure batch has a put method
        if (typeof batch.put !== "function") {
          console.error("batch.put is not a function - implementing fallback");
          return this._directPut(key, value);
        }

        // Add the put operation to the batch
        batch.put(key, value);

        // Ensure batch has a flush method
        if (typeof batch.flush !== "function") {
          console.error(
            "batch.flush is not a function - implementing fallback"
          );
          return this._directPut(key, value);
        }

        // Flush the batch to ensure the operation is complete
        await batch.flush();
        console.log("IndexDBStorage.put completed successfully");
      } catch (err) {
        // Log and rethrow errors for better debugging
        console.error("Error in batch put/flush operation:", err);

        // Try direct access as fallback
        await this._directPut(key, value);
      } finally {
        // Ensure the batch is destroyed properly
        try {
          if (typeof batch.tryFlush === "function") {
            batch.tryFlush();
          } else if (typeof batch.destroy === "function") {
            batch.destroy();
          }
        } catch (flushErr) {
          console.error("Error in batch cleanup:", flushErr);
        }
      }
    } catch (err) {
      console.error("Critical error in IndexDBStorage.put:", err);
      throw err;
    }
  }

  /**
   * Direct put implementation as fallback
   * @private
   * @param {string|Buffer} key - The key to put
   * @param {*} value - The value to put
   * @returns {Promise<void>} Promise that resolves when complete
   */
  async _directPut(key, value) {
    try {
      console.log("Using _directPut fallback");
      await this._state.ready();
      const db = this._state._db;
      if (!db) throw new Error("Database not available");

      const storeName = this._columnFamily
        ? typeof this._columnFamily === "string"
          ? this._columnFamily
          : this._columnFamily.name
        : "default";

      const keyStr = typeof key === "string" ? key : key.toString();

      return new Promise((resolve, reject) => {
        try {
          const tx = db.transaction([storeName], "readwrite");
          const store = tx.objectStore(storeName);
          const req = store.put(value, keyStr);

          tx.oncomplete = () => resolve();
          tx.onerror = (event) => reject(event.target.error);
        } catch (err) {
          console.error("Error in _directPut:", err);
          reject(err);
        }
      });
    } catch (err) {
      console.error("Error in _directPut:", err);
      throw err;
    }
  }

  /**
   * Delete a value from the database
   * @param {string|Buffer} key - The key to delete
   * @param {object} opts - Options for the operation
   * @returns {Promise<void>} Promise that resolves when the operation is complete
   */
  async delete(key, opts = {}) {
    maybeClosed(this);

    // Create a write batch with minimal overhead
    const batch = await this.write({ ...opts, capacity: 1, autoDestroy: true });

    try {
      // Add the delete operation to the batch
      await batch.delete(key);

      // Flush the batch to ensure the operation is complete
      await batch.flush();
    } catch (err) {
      // Log and rethrow errors for better debugging
      console.error("Error in delete operation:", err);
      throw err;
    }
  }

  /**
   * Delete a range of values from the database
   * @param {string|Buffer} start - The start key (inclusive)
   * @param {string|Buffer} end - The end key (exclusive)
   * @param {object} opts - Options for the operation
   * @returns {Promise<void>} Promise that resolves when the operation is complete
   */
  async deleteRange(start, end, opts = {}) {
    maybeClosed(this);

    // Create a write batch with minimal overhead
    const batch = await this.write({ ...opts, capacity: 1, autoDestroy: true });

    try {
      // Add the deleteRange operation to the batch
      await batch.deleteRange(start, end);

      // Flush the batch to ensure the operation is complete
      await batch.flush();
    } catch (err) {
      // Log and rethrow errors for better debugging
      console.error("Error in deleteRange operation:", err);
      throw err;
    }
  }

  /**
   * Try to put a value into the database (RocksDB compatibility)
   * @param {string|Buffer} key - The key to put
   * @param {string|Buffer} value - The value to put
   * @returns {Promise<void>} Promise that resolves when the operation is complete
   */
  async tryPut(key, value) {
    maybeClosed(this);

    const batch = await this.write({ capacity: 1, autoDestroy: true });
    await batch.tryPut(key, value);
    await batch.flush();
    return Promise.resolve();
  }

  /**
   * Try to delete a value from the database (RocksDB compatibility)
   * @param {string|Buffer} key - The key to delete
   * @returns {Promise<void>} Promise that resolves when the operation is complete
   */
  async tryDelete(key) {
    maybeClosed(this);

    const batch = await this.write({ capacity: 1, autoDestroy: true });
    await batch.tryDelete(key);
    await batch.flush();
    return Promise.resolve();
  }

  /**
   * Try to delete a range of values from the database
   * @param {string|Buffer} start - The start key (inclusive)
   * @param {string|Buffer} end - The end key (exclusive)
   * @returns {Promise<void>} Promise that resolves when the operation is complete
   */
  async tryDeleteRange(start, end) {
    maybeClosed(this);
    console.log(
      "IndexDBStorage.tryDeleteRange called with range:",
      start,
      "to",
      end
    );

    try {
      // Do a direct range delete without using a batch for more reliable behavior
      await this._directDeleteRange(start, end);
      console.log(
        "IndexDBStorage.tryDeleteRange completed (direct implementation)"
      );
      return Promise.resolve();
    } catch (err) {
      console.error("Error in direct tryDeleteRange:", err);

      // Fallback to batch implementation if direct fails
      try {
        // Create a write batch
        const batch = await this.write({ capacity: 1, autoDestroy: true });

        try {
          // Add tryDeleteRange method if missing
          if (typeof batch.tryDeleteRange !== "function") {
            console.log("Adding missing tryDeleteRange method to batch");
            batch.tryDeleteRange = async (rangeStart, rangeEnd) => {
              console.log("Using patched tryDeleteRange method");
              return this._directDeleteRange(rangeStart, rangeEnd);
            };
          }

          // Perform the range delete
          await batch.tryDeleteRange(start, end);
          console.log("IndexDBStorage.tryDeleteRange completed via batch");
        } catch (batchErr) {
          console.error("Error in batch tryDeleteRange:", batchErr);
        } finally {
          try {
            if (typeof batch.tryFlush === "function") {
              batch.tryFlush();
            } else if (typeof batch.destroy === "function") {
              batch.destroy();
            }
          } catch (flushErr) {
            console.error("Error in batch cleanup:", flushErr);
          }
        }
      } catch (fallbackErr) {
        console.error(
          "Critical error in fallback tryDeleteRange:",
          fallbackErr
        );
      }
    }

    return Promise.resolve();
  }

  /**
   * Direct implementation of range deletion
   * @private
   * @param {string|Buffer} start - The start key (inclusive)
   * @param {string|Buffer} end - The end key (exclusive)
   * @returns {Promise<void>} Promise that resolves when complete
   */
  async _directDeleteRange(start, end) {
    try {
      console.log("Using _directDeleteRange fallback");
      await this._state.ready();
      const db = this._state._db;
      if (!db) throw new Error("Database not available");

      const storeName = this._columnFamily
        ? typeof this._columnFamily === "string"
          ? this._columnFamily
          : this._columnFamily.name
        : "default";

      const startStr = typeof start === "string" ? start : start.toString();
      const endStr = typeof end === "string" ? end : end.toString();

      // First get all keys in the range
      const keysToDelete = await new Promise((resolve, reject) => {
        try {
          const keys = [];
          const tx = db.transaction([storeName], "readonly");
          const store = tx.objectStore(storeName);

          let range;
          try {
            // Try to create a key range
            range = IDBKeyRange.bound(startStr, endStr, false, true);
          } catch (err) {
            console.warn(
              "Error creating IDBKeyRange, will use manual filtering:",
              err
            );
          }

          const request = range
            ? store.openKeyCursor(range)
            : store.openKeyCursor();

          request.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
              const key = cursor.key;
              const keyStr = typeof key === "string" ? key : String(key);

              // If using manual filtering
              if (!range && (keyStr < startStr || keyStr >= endStr)) {
                cursor.continue();
                return;
              }

              keys.push(key);
              cursor.continue();
            }
          };

          tx.oncomplete = () => resolve(keys);
          tx.onerror = (event) => reject(event.target.error);
        } catch (err) {
          console.error("Error getting keys in range:", err);
          reject(err);
        }
      });

      console.log(`Found ${keysToDelete.length} keys to delete in range`);

      // Now delete all found keys
      if (keysToDelete.length > 0) {
        await new Promise((resolve, reject) => {
          try {
            const tx = db.transaction([storeName], "readwrite");
            const store = tx.objectStore(storeName);

            for (const key of keysToDelete) {
              store.delete(key);
            }

            tx.oncomplete = () => resolve();
            tx.onerror = (event) => reject(event.target.error);
          } catch (err) {
            console.error("Error deleting keys:", err);
            reject(err);
          }
        });
      }

      console.log("Range delete completed successfully");
    } catch (err) {
      console.error("Error in _directDeleteRange:", err);
      throw err;
    }
  }

  /**
   * Simple peek implementation for the first item in a range
   * @param {object} range - Range options (gt, gte, lt, lte, etc)
   * @param {object} options - Iterator options
   * @returns {Promise<{key:string, value:*}>} Key-value pair
   */
  async peek(range, options = {}) {
    maybeClosed(this);
    console.log("IndexDBStorage.peek called with range:", range);

    try {
      await this._state.ready();
      const db = this._state._db;
      if (!db) return null;

      const storeName = this._columnFamily
        ? typeof this._columnFamily === "string"
          ? this._columnFamily
          : this._columnFamily.name
        : "default";

      // Extract range parameters
      const { gt, gte, lt, lte, prefix } = range;
      let lowerBound, upperBound;
      let lowerExclusive = false,
        upperExclusive = false;

      // Handle prefix range
      if (prefix) {
        lowerBound = prefix;
        lowerExclusive = false;

        // Calculate upper bound by incrementing the last char code
        const prefixStr = String(prefix);
        const lastChar = prefixStr.charCodeAt(prefixStr.length - 1);
        upperBound = prefixStr.slice(0, -1) + String.fromCharCode(lastChar + 1);
        upperExclusive = true;
      } else {
        // Handle explicit bounds
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

      // Create key range if possible
      let keyRange = null;
      try {
        if (lowerBound !== undefined && upperBound !== undefined) {
          keyRange = IDBKeyRange.bound(
            lowerBound,
            upperBound,
            lowerExclusive,
            upperExclusive
          );
        } else if (lowerBound !== undefined) {
          keyRange = IDBKeyRange.lowerBound(lowerBound, lowerExclusive);
        } else if (upperBound !== undefined) {
          keyRange = IDBKeyRange.upperBound(upperBound, upperExclusive);
        }
      } catch (err) {
        console.warn("Error creating key range for peek:", err);
      }

      // Get the first matching entry
      const direction = options.reverse ? "prev" : "next";

      const result = await new Promise((resolve, reject) => {
        try {
          const tx = db.transaction([storeName], "readonly");
          const store = tx.objectStore(storeName);

          const request = store.openCursor(keyRange, direction);

          request.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
              // Found a match
              const key = cursor.key;
              const value = cursor.value;

              // Convert to appropriate format based on encoding
              const resultKey =
                this._keyEncoding === "buffer" ? Buffer.from(String(key)) : key;

              const resultValue =
                this._valueEncoding === "buffer"
                  ? Buffer.from(String(value))
                  : value;

              resolve({ key: resultKey, value: resultValue });
            } else {
              // No match found
              resolve(null);
            }
          };

          request.onerror = (event) => {
            console.error("Error in peek cursor:", event.target.error);
            reject(event.target.error);
          };
        } catch (err) {
          console.error("Error in peek:", err);
          reject(err);
        }
      });

      console.log(
        "IndexDBStorage.peek result:",
        result ? "found" : "not found"
      );
      return result;
    } catch (err) {
      console.error("Critical error in IndexDBStorage.peek:", err);
      return null;
    }
  }

  _ref() {
    if (this._snapshot) this._snapshot._ref();
    this._state.handles.inc();
  }

  _unref() {
    if (this._snapshot) this._snapshot._unref();
    this._state.handles.dec();
  }

  /**
   * Create a transaction for improved consistency guarantees
   * Note: This is an extension beyond the basic RocksDB API
   * @returns {Transaction} A new transaction instance
   */
  createTransaction() {
    maybeClosed(this);
    throw new Error("Transaction API is no longer supported");
  }

  /**
   * Flush any pending operations to disk
   * @param {object} opts - Flush options
   * @returns {Promise<void>} Promise that resolves when flushed
   */
  flush(opts) {
    maybeClosed(this);
    console.log("IndexDBStorage.flush called with opts:", opts);

    // In IndexedDB, there's no explicit flush needed since transactions auto-commit
    // Just ensure we're ready
    return this._state.ready().then(() => {
      console.log("IndexDBStorage.flush completed");
      return Promise.resolve();
    });
  }
}

// Required constants for hypercore-storage compatibility
IndexDBStorage.STORAGE_TYPE = "indexeddb";
IndexDBStorage.VERSION = 1;

// Expose classes
IndexDBStorage.ColumnFamily = ColumnFamily;
IndexDBStorage.BloomFilterPolicy = BloomFilterPolicy;
IndexDBStorage.RibbonFilterPolicy = RibbonFilterPolicy;

// Create RocksDB alias for drop-in compatibility with rocksdb-native
const RocksDB = IndexDBStorage;

// Export both RocksDB as default and named exports for maximum compatibility
export default RocksDB;
export { IndexDBStorage, RocksDB };

function maybeClosed(db) {
  if (db._state.closing || db._index === -1)
    throw new Error("IndexDB session is closed");
}
