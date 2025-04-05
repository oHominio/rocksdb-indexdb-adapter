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

    // Create a read batch with minimal overhead
    const batch = await this.read({ ...opts, capacity: 1, autoDestroy: true });

    try {
      // Use the batch to get the value (batch.get will convert to Buffer)
      const value = await batch.get(key);

      // The batch.get method already converts to Buffer, no need to convert again
      return value;
    } catch (err) {
      if (err.name === "NotFoundError") return null;
      throw err;
    } finally {
      // Ensure the batch is destroyed properly
      batch.tryFlush();
    }
  }

  /**
   * Peek at the first result in a given range
   * @param {object} range - Key range
   * @param {object} options - Iterator options
   * @returns {Promise<object|null>} Promise with key-value pair or null
   */
  async peek(range, options = {}) {
    if (this.closed) {
      throw new Error("Database session is closed");
    }

    // Get database and object store
    await this._state.ready();
    const db = this._state._db;
    if (!db) throw new Error("Database is closed");

    // Get the column family / store name
    const storeName = this._columnFamily
      ? typeof this._columnFamily === "string"
        ? this._columnFamily
        : this._columnFamily.name
      : "default";

    // Create a key range for the query
    const { gt, gte, lt, lte, prefix } = range;
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

    // Create the key range
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
    } catch (e) {
      console.error("Error creating key range for peek:", e);
    }

    // Use a direct transaction for better performance than iterator
    try {
      const transaction = db.transaction([storeName], "readonly");
      const store = transaction.objectStore(storeName);

      // Determine direction based on reverse option
      const direction = options.reverse ? "prev" : "next";

      // Use a promise to get the first matching record
      const result = await new Promise((resolve, reject) => {
        const request = store.openCursor(keyRange, direction);

        request.onsuccess = (event) => {
          const cursor = event.target.result;
          if (cursor) {
            // We found a match, return the key-value pair
            const key = cursor.key;
            const value = cursor.value;

            // Convert to buffers if needed based on encoding
            let resultKey = key;
            let resultValue = value;

            // Handle encoding if specified
            if (
              this._keyEncoding === "binary" ||
              this._keyEncoding === "buffer"
            ) {
              resultKey = Buffer.from(String(key));
            }

            if (
              this._valueEncoding === "binary" ||
              this._valueEncoding === "buffer"
            ) {
              resultValue = Buffer.from(String(value));
            }

            resolve({ key: resultKey, value: resultValue });
          } else {
            // No results match the range
            resolve(null);
          }
        };

        request.onerror = (event) => {
          console.error("Error in peek cursor:", event.target.error);
          reject(event.target.error);
        };
      });

      return result;
    } catch (err) {
      console.error("Error in peek operation:", err);
      return null;
    }
  }

  async read(opts) {
    maybeClosed(this);
    return this._state.createReadBatch(this, opts);
  }

  async write(opts) {
    maybeClosed(this);
    return this._state.createWriteBatch(this, opts);
  }

  flush(opts) {
    maybeClosed(this);

    return this._state.flush(this, opts);
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

    // Create a write batch with minimal overhead
    const batch = await this.write({ ...opts, capacity: 1, autoDestroy: true });

    try {
      // Add the put operation to the batch
      await batch.put(key, value);

      // Flush the batch to ensure the operation is complete
      await batch.flush();
    } catch (err) {
      // Log and rethrow errors for better debugging
      console.error("Error in put operation:", err);
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
   * Try to delete a range of values (RocksDB compatibility)
   * @param {string|Buffer} start - The start key (inclusive)
   * @param {string|Buffer} end - The end key (exclusive)
   * @returns {Promise<void>} Promise that resolves when the operation is complete
   */
  async tryDeleteRange(start, end) {
    maybeClosed(this);

    const batch = await this.write({ capacity: 1, autoDestroy: true });
    await batch.tryDeleteRange(start, end);
    await batch.flush();
    return Promise.resolve();
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
