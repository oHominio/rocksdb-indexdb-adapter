import * as c from "compact-encoding";
import { Snapshot } from "./snapshot.js";

// Import IDBKeyRange for range operations
const IDBKeyRange =
  typeof window !== "undefined"
    ? window.IDBKeyRange
    : typeof global !== "undefined"
    ? global.IDBKeyRange
    : null;

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
    const { write = false, autoDestroy = false } = options;

    this.db = db;
    this.write = write;
    this.destroyed = false;
    this.autoDestroy = autoDestroy;
    this._pendingOps = new Map();

    // Reference the database to prevent it from closing during batch operations
    if (db._ref) db._ref();

    // Enhanced debugging/logging
    console.log(
      `Creating new ${write ? "write" : "read"} batch for ${
        db._columnFamily || "default"
      }`
    );

    // IMPORTANT: Always ensure all required methods exist for hypercore-storage
    this._ensureAllMethodsExist();
  }

  // Ensure all required methods exist to avoid "not a function" errors
  _ensureAllMethodsExist() {
    // Ensure all standard methods are available for both read and write batches
    if (typeof this.get !== "function") {
      console.log("Patching missing get method");
      this.get = this.get || this._safeGet.bind(this);
    }

    if (typeof this.tryFlush !== "function") {
      console.log("Patching missing tryFlush method");
      this.tryFlush = this.tryFlush || this._safeTryFlush.bind(this);
    }

    // Write-specific methods
    if (this.write) {
      if (typeof this.put !== "function") {
        console.log("Patching missing put method");
        this.put = this.put || this._safePut.bind(this);
      }

      if (typeof this.delete !== "function") {
        console.log("Patching missing delete method");
        this.delete = this.delete || this._safeDelete.bind(this);
      }

      if (typeof this.flush !== "function") {
        console.log("Patching missing flush method");
        this.flush = this.flush || this._safeFlush.bind(this);
      }

      if (typeof this.deleteRange !== "function") {
        console.log("Patching missing deleteRange method");
        this.deleteRange = this.deleteRange || this._safeDeleteRange.bind(this);
      }

      if (typeof this.tryDeleteRange !== "function") {
        console.log("Patching missing tryDeleteRange method");
        this.tryDeleteRange =
          this.tryDeleteRange || this._safeTryDeleteRange.bind(this);
      }
    }
  }

  // Safe versions of methods that never throw "not a function" errors
  async _safeGet(key) {
    console.log("Using fallback _safeGet method");
    try {
      if (this.destroyed) return null;

      const keyStr =
        typeof key === "string" ? key : Buffer.from(key).toString();

      // Check pending operations
      const pendingOp = this._pendingOps && this._pendingOps.get(keyStr);
      if (pendingOp) {
        if (pendingOp.type === "delete") return null;
        if (pendingOp.type === "put")
          return this._convertToBuffer(pendingOp.value);
      }

      // Get from database
      if (this.db && this.db._state && this.db._state._db) {
        const db = this.db._state._db;
        const store = this._getCfName();
        try {
          const tx = db.transaction([store], "readonly");
          const objectStore = tx.objectStore(store);

          const value = await new Promise((resolve, reject) => {
            const request = objectStore.get(keyStr);
            request.onsuccess = () => resolve(request.result);
            request.onerror = (e) => {
              console.error("Error in _safeGet:", e.target.error);
              resolve(null);
            };
          });

          return this._convertToBuffer(value);
        } catch (err) {
          console.error("Error in _safeGet db access:", err);
          return null;
        }
      }

      return null;
    } catch (err) {
      console.error("Critical error in _safeGet:", err);
      return null;
    }
  }

  _safePut(key, value) {
    console.log("Using fallback _safePut method");
    try {
      if (this.destroyed || !this.write) return this;

      const keyStr =
        typeof key === "string" ? key : Buffer.from(key).toString();

      this._pendingOps.set(keyStr, {
        type: "put",
        key: keyStr,
        value: value,
      });

      return this;
    } catch (err) {
      console.error("Error in _safePut:", err);
      return this;
    }
  }

  _safeDelete(key) {
    console.log("Using fallback _safeDelete method");
    try {
      if (this.destroyed || !this.write) return this;

      const keyStr =
        typeof key === "string" ? key : Buffer.from(key).toString();

      this._pendingOps.set(keyStr, {
        type: "delete",
        key: keyStr,
      });

      return this;
    } catch (err) {
      console.error("Error in _safeDelete:", err);
      return this;
    }
  }

  _safeDeleteRange(start, end) {
    console.log("Using fallback _safeDeleteRange method");
    try {
      if (this.destroyed || !this.write) return this;

      // Create range operation
      this._rangeOperations = this._rangeOperations || [];

      const startStr =
        typeof start === "string" ? start : Buffer.from(start).toString();
      const endStr =
        typeof end === "string" ? end : Buffer.from(end).toString();

      this._rangeOperations.push({
        type: "deleteRange",
        start: startStr,
        end: endStr,
      });

      return this;
    } catch (err) {
      console.error("Error in _safeDeleteRange:", err);
      return this;
    }
  }

  async _safeFlush() {
    console.log("Using fallback _safeFlush method");
    try {
      if (this.destroyed) return;

      // Process pending operations
      if (this._pendingOps && this._pendingOps.size > 0) {
        await this._processPendingOps();
      }

      // Process range operations
      if (this._rangeOperations && this._rangeOperations.length > 0) {
        const db = this.db._state._db;
        const storeName = this._getCfName();

        for (const op of this._rangeOperations) {
          if (op.type === "deleteRange") {
            await this._processRangeDelete(db, storeName, op.start, op.end);
          }
        }

        this._rangeOperations = [];
      }
    } catch (err) {
      console.error("Error in _safeFlush:", err);
    }
  }

  async _safeTryFlush() {
    console.log("Using fallback _safeTryFlush method");
    try {
      if (this.destroyed) return;

      if (this.write) {
        // For write batches, start the flush without waiting
        this._safeFlush().catch((err) => {
          console.error("Error in _safeTryFlush for write batch:", err);
        });
      }

      // For read batches, just destroy
      if (!this.write && typeof this.destroy === "function") {
        try {
          this.destroy();
        } catch (destroyErr) {
          console.error(
            "Error destroying read batch in _safeTryFlush:",
            destroyErr
          );
        }
      }
    } catch (err) {
      console.error("Error in _safeTryFlush:", err);

      // Attempt to destroy on error
      try {
        if (typeof this.destroy === "function") {
          this.destroy();
        }
      } catch (destroyErr) {
        console.error("Error destroying batch in _safeTryFlush:", destroyErr);
      }
    }

    return Promise.resolve();
  }

  // Override the existing tryFlush with enhanced error handling
  async tryFlush() {
    try {
      console.log(`${this.write ? "WriteBatch" : "ReadBatch"}.tryFlush called`);

      if (this.destroyed) {
        console.log("Batch already destroyed in tryFlush");
        return Promise.resolve();
      }

      if (this.db._state && this.db._state._suspended) {
        console.log("Database suspended, skipping tryFlush");
        return Promise.resolve();
      }

      if (this.write) {
        // For write batches, start the flush without waiting
        this.flush().catch((err) => {
          console.error("Error in tryFlush for write batch:", err);
        });
      } else {
        // For read batches, just destroy
        this.destroy();
      }

      return Promise.resolve();
    } catch (err) {
      console.error("Critical error in tryFlush:", err);

      // Try to recover
      try {
        this.destroy();
      } catch (destroyErr) {
        console.error("Error destroying batch in tryFlush:", destroyErr);
      }

      return Promise.resolve();
    }
  }

  // Override _reuse to ensure methods are patched
  _reuse(db, options = {}) {
    this.db = db;
    this.write = !!options.write;
    this.destroyed = false;
    this.autoDestroy = !!options.autoDestroy;
    this._pendingOps = new Map();

    // Reference the database to prevent it from closing during batch operations
    if (db._ref) db._ref();

    // Ensure all methods exist on reuse
    this._ensureAllMethodsExist();
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
        console.error("Error in batch ready:", err);
      }
    }
    return Promise.resolve();
  }

  /**
   * Get a value from the batch or database
   * @param {string|Buffer} key - Key to get
   * @returns {Promise<*>} Promise with the value or null if not exists
   */
  async get(key) {
    if (this.destroyed) throw new Error("Batch is destroyed");

    const keyStr = typeof key === "string" ? key : Buffer.from(key).toString();

    // Check the pending operations first for a potential match
    const pendingOp = this._pendingOps && this._pendingOps.get(keyStr);
    if (pendingOp) {
      if (pendingOp.type === "delete") return null;
      if (pendingOp.type === "put")
        return this._convertToBuffer(pendingOp.value);
    }

    try {
      // Get the database and store
      await this.db._state.ready();
      const db = this.db._state._db;
      if (!db) throw new Error("Database not available");

      const store = this._getCfName();

      // Execute the get operation
      const value = await new Promise((resolve, reject) => {
        // Use a read transaction for better performance
        const tx = db.transaction([store], "readonly");
        const objectStore = tx.objectStore(store);

        const request = objectStore.get(keyStr);

        request.onsuccess = () => {
          const result = request.result;
          resolve(result);
        };

        request.onerror = (event) => {
          console.error("Get request error:", event.target.error);
          reject(event.target.error);
        };
      });

      // Convert result to Buffer for compatibility with RocksDB
      return this._convertToBuffer(value);
    } catch (err) {
      console.error("Error in batch get:", err);
      return null;
    }
  }

  /**
   * Convert a value to Buffer if needed for RocksDB compatibility
   * @private
   * @param {*} value - The value to convert
   * @returns {Buffer|null} Converted value
   */
  _convertToBuffer(value) {
    // Handle null values
    if (value === null || value === undefined) {
      return null;
    }

    // Handle objects with _type property
    if (value && typeof value === "object" && value._type === "string") {
      return Buffer.from(value.data);
    }

    // Handle versioned values
    if (
      value &&
      typeof value === "object" &&
      value.v !== undefined &&
      value.data !== undefined
    ) {
      return Buffer.from(value.data);
    }

    // If it's already a Buffer, return it
    if (Buffer.isBuffer(value)) {
      return value;
    }

    // Convert strings and other types to Buffer
    return Buffer.from(String(value));
  }

  /**
   * Put a key-value pair into the batch
   * @param {string|Buffer} key - Key to put
   * @param {*} value - Value to put
   * @returns {Batch} This batch instance
   */
  put(key, value) {
    if (this.destroyed) {
      return this;
    }

    if (!this.write) {
      throw new Error("Cannot write to a read batch");
    }

    // Convert key to string for IndexedDB
    const keyStr = typeof key === "string" ? key : Buffer.from(key).toString();

    // Add to pending operations
    this._pendingOps.set(keyStr, {
      type: "put",
      key: keyStr,
      value: value,
    });

    return this;
  }

  /**
   * Try to put a key-value pair
   * This method matches RocksDB's API
   * @param {string|Buffer} key - Key to put
   * @param {*} value - Value to put
   * @returns {Promise<void>} Promise that resolves when the operation is complete
   */
  tryPut(key, value) {
    if (this.destroyed) {
      return Promise.resolve();
    }

    if (!this.write) {
      throw new Error("Cannot write to a read batch");
    }

    // Handle Buffer keys and values properly for View.flush compatibility
    let processedKey = key;
    let processedValue = value;

    // Handle Buffer keys
    if (Buffer.isBuffer(key)) {
      // Convert to string for IndexedDB
      processedKey = key.toString();
    }

    // Handle Buffer values - store as Buffer so we can retrieve them correctly
    if (Buffer.isBuffer(value)) {
      // Keep as Buffer for consistent retrieval
      processedValue = value;
    }

    // Add to pending operations
    this._pendingOps.set(
      typeof processedKey === "string" ? processedKey : processedKey.toString(),
      {
        type: "put",
        key: processedKey,
        value: processedValue,
      }
    );

    return Promise.resolve();
  }

  /**
   * Delete a key-value pair
   * @param {*} key - Key to delete
   * @returns {Batch} This batch instance
   */
  delete(key) {
    if (this.destroyed) {
      return this;
    }

    if (!this.write) {
      throw new Error("Cannot write to a read batch");
    }

    // Convert key to string for IndexedDB
    const keyStr = typeof key === "string" ? key : Buffer.from(key).toString();

    // Add to pending operations
    this._pendingOps.set(keyStr, {
      type: "delete",
      key: keyStr,
    });

    return this;
  }

  /**
   * Try to delete a key-value pair
   * This method matches RocksDB's API
   * @param {*} key - Key to delete
   * @returns {Promise<void>} Promise that resolves when the operation is complete
   */
  tryDelete(key) {
    this.delete(key);
    return Promise.resolve();
  }

  /**
   * Delete a range of key-value pairs
   * @param {*} start - Start key (inclusive)
   * @param {*} end - End key (exclusive)
   * @returns {Batch} This batch instance
   */
  deleteRange(start, end) {
    if (this.destroyed) {
      return this;
    }

    if (!this.write) {
      throw new Error("Cannot write to a read batch");
    }

    // Convert keys to strings for IndexedDB
    const startStr =
      typeof start === "string" ? start : Buffer.from(start).toString();
    const endStr = typeof end === "string" ? end : Buffer.from(end).toString();

    // Add a special operation for range deletion
    this._rangeOperations = this._rangeOperations || [];
    this._rangeOperations.push({
      type: "deleteRange",
      start: startStr,
      end: endStr,
    });

    // For immediate effect, we'll start the range delete process right away
    // This is required for proper test compatibility with the RocksDB version
    if (this.db && this.db._state && this.db._state._db) {
      this._simulateRangeDelete(startStr, endStr).catch((err) => {
        console.error("Error in deleteRange:", err);
      });
    }

    return this;
  }

  /**
   * Simulate immediate range delete by marking keys in the range as deleted
   * @private
   * @param {string} start - Start key (inclusive)
   * @param {string} end - End key (exclusive)
   */
  async _simulateRangeDelete(start, end) {
    try {
      if (this.db && this.db._state && this.db._state._db) {
        const db = this.db._state._db;
        const storeName = this._getCfName();

        await this._processRangeDelete(db, storeName, start, end);
      }
    } catch (err) {
      console.error("Error in _simulateRangeDelete:", err);
    }
  }

  /**
   * Process a range delete operation
   * @private
   * @param {object} db - Database instance
   * @param {string} storeName - Name of the store/column family
   * @param {string|Buffer} start - Start key of the range (inclusive)
   * @param {string|Buffer} end - End key of the range (exclusive)
   * @returns {Promise<void>} A promise that resolves when the range delete completes
   */
  async _processRangeDelete(db, storeName, start, end) {
    try {
      // Convert Buffer to string if needed for key comparison
      const startKey =
        typeof start === "string" ? start : start.toString("utf8");
      const endKey = typeof end === "string" ? end : end.toString("utf8");

      console.log(`Processing range delete from "${startKey}" to "${endKey}"`);

      // First, get all keys in the range using a read transaction
      const readTx = db.transaction([storeName], "readonly");
      const readStore = readTx.objectStore(storeName);

      // We'll collect all keys in the range that need to be deleted
      let keysToDelete = [];

      try {
        // Try to use IDBKeyRange - may fail with certain key formats
        const range = IDBKeyRange.bound(startKey, endKey, false, true); // inclusive start, exclusive end

        keysToDelete = await new Promise((resolve, reject) => {
          const keys = [];
          const request = readStore.openKeyCursor(range);

          request.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
              keys.push(cursor.key);
              cursor.continue();
            }
          };

          readTx.oncomplete = () => resolve(keys);
          readTx.onerror = (event) => {
            console.error("Error in range cursor:", event.target.error);
            reject(event.target.error);
          };
        });
      } catch (err) {
        console.error(
          "Error using IDBKeyRange, falling back to manual scan:",
          err
        );

        // Fall back to a manual scan of all keys
        keysToDelete = await new Promise((resolve, reject) => {
          const keys = [];
          const request = readStore.openCursor();

          request.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
              const key = cursor.key;
              const keyStr = typeof key === "string" ? key : key.toString();

              // Check if the key is in the range: startKey (inclusive) to endKey (exclusive)
              if (keyStr >= startKey && keyStr < endKey) {
                keys.push(key);
              }
              cursor.continue();
            }
          };

          readTx.oncomplete = () => resolve(keys);
          readTx.onerror = (event) => {
            console.error("Error in manual scan:", event.target.error);
            reject(event.target.error);
          };
        });
      }

      // Log the keys we found for debugging
      if (keysToDelete.length > 0) {
        console.log(
          `Found ${keysToDelete.length} keys to delete in range:`,
          keysToDelete
        );

        // Delete all found keys in a single write transaction
        const writeTx = db.transaction([storeName], "readwrite");
        const writeStore = writeTx.objectStore(storeName);

        // Delete each key one by one in the transaction
        for (const key of keysToDelete) {
          writeStore.delete(key);
        }

        // Wait for the transaction to complete
        await new Promise((resolve, reject) => {
          writeTx.oncomplete = () => resolve();
          writeTx.onerror = (event) => {
            console.error(
              "Error in range delete transaction:",
              event.target.error
            );
            reject(event.target.error);
          };
        });
      } else {
        console.log("No keys found in range to delete");
      }

      return;
    } catch (err) {
      console.error("Error in _processRangeDelete:", err);
      throw err;
    }
  }

  /**
   * Destroy this batch instance
   */
  destroy() {
    if (this.destroyed) return;

    this.destroyed = true;
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
   * Get a current version identifier for a key
   * @private
   * @param {string} key - The key to get version for
   * @returns {Promise<number>} The version identifier
   */
  async _getCurrentVersion(key) {
    try {
      // We'll use transaction time as version
      return Date.now();
    } catch (err) {
      console.error("Error getting key version:", err);
      return Date.now();
    }
  }

  /**
   * Apply put operation with version check
   * @private
   * @param {IDBObjectStore} store - The object store
   * @param {string} key - The key to put
   * @param {*} value - The value to put
   * @param {number} version - The version to check against
   */
  _putWithVersionCheck(store, key, value, version) {
    // In this implementation, we don't actually wrap the values with version info
    // to maintain compatibility with the original RocksDB interface
    store.put(value, key);
  }

  /**
   * Apply delete operation with version check
   * @private
   * @param {IDBObjectStore} store - The object store
   * @param {string} key - The key to delete
   * @param {number} version - The version to check against
   */
  _deleteWithVersionCheck(store, key, version) {
    // Simple delete without version check for now
    store.delete(key);
  }

  /**
   * Flush batched write operations to the database
   * @returns {Promise<void>} Promise that resolves when all operations are flushed
   */
  async flush() {
    if (this.destroyed) {
      return;
    }

    if (this.db._state._suspended) {
      // Cannot flush when suspended
      return;
    }

    try {
      // Wait for db to be ready
      await this.db._state.ready();

      if (!this._pendingOps || this._pendingOps.size === 0) {
        if (!this._rangeOperations || this._rangeOperations.length === 0) {
          // No operations to flush
          return;
        }
      }

      // Process all operations
      await this._processPendingOps();

      // Process range operations if any
      if (this._rangeOperations && this._rangeOperations.length > 0) {
        const db = this.db._state._db;
        const storeName = this._getCfName();

        for (const op of this._rangeOperations) {
          if (op.type === "deleteRange") {
            await this._processRangeDelete(db, storeName, op.start, op.end);
          }
        }

        // Clear range operations
        this._rangeOperations = [];
      }
    } catch (err) {
      console.error("Error in flush:", err);
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
      return "default";
    }

    return typeof this.db._columnFamily === "string"
      ? this.db._columnFamily
      : this.db._columnFamily.name || "default";
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
        if (typeof this.db._keyEncoding === "string") {
          // String-based encoding
          if (
            this.db._keyEncoding === "utf8" ||
            this.db._keyEncoding === "json"
          ) {
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
        return key.toString("hex");
      }

      return key;
    } catch (err) {
      console.error("Error encoding key:", err);
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
      // Convert value to a storable format for IndexedDB
      // Buffer values need special handling
      if (Buffer.isBuffer(value)) {
        // Store as array for IndexedDB compatibility
        return Array.from(value);
      }

      // Handle value encoding if specified
      if (this.db._valueEncoding) {
        if (typeof this.db._valueEncoding === "string") {
          // String-based encoding
          if (
            this.db._valueEncoding === "utf8" ||
            this.db._valueEncoding === "json"
          ) {
            return value;
          }
          // Default to buffer-like array for other string encodings
          return Buffer.isBuffer(value)
            ? Array.from(value)
            : Array.from(Buffer.from(value));
        } else {
          // compact-encoding
          return c.encode(this.db._valueEncoding, value);
        }
      }

      // For consistent behavior with RocksDB, store strings in a way
      // that allows us to convert back to Buffer on retrieval
      if (typeof value === "string") {
        // Mark as a string value that should be converted to Buffer on retrieval
        return { _type: "string", value };
      }

      // For other types, preserve as is
      return value;
    } catch (err) {
      console.error("Error encoding value:", err);
      return value;
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
      // Check if value is an object with v and data properties (versioned)
      if (
        value &&
        typeof value === "object" &&
        "v" in value &&
        "data" in value
      ) {
        // Extract the actual data from the versioned value
        value = value.data;
      }

      // Check if this is a marked string value
      if (value && typeof value === "object" && value._type === "string") {
        // Convert string values to Buffer for consistency with RocksDB
        return Buffer.from(value.value);
      }

      // For values encoded as arrays (Buffer data)
      if (Array.isArray(value)) {
        return Buffer.from(value);
      }

      // For string values, convert to Buffer to match RocksDB behavior
      if (typeof value === "string") {
        return Buffer.from(value);
      }

      // Handle value encoding if specified
      if (this.db._valueEncoding) {
        if (typeof this.db._valueEncoding === "string") {
          // String-based encoding
          if (
            this.db._valueEncoding === "utf8" ||
            this.db._valueEncoding === "json"
          ) {
            return value;
          }

          // Default to buffer for other string encodings
          if (Array.isArray(value)) {
            return Buffer.from(value);
          }

          return Buffer.from(String(value));
        } else {
          // compact-encoding
          return c.decode(this.db._valueEncoding, value);
        }
      }

      // Default case: best effort to return Buffer-like values
      if (value !== null && value !== undefined) {
        try {
          return Buffer.from(String(value));
        } catch (err) {
          return value;
        }
      }

      return value;
    } catch (err) {
      console.error("Error decoding value:", err);
      return null;
    }
  }

  /**
   * Record pre-modification value if needed for test snapshots
   * @private
   * @param {*} key - The key being modified
   */
  async _recordPreModificationValueIfNeeded(key) {
    // With our new snapshot implementation, we don't need to record pre-modification values
    // as each snapshot already maintains its own separate copy of data
    return;
  }

  /**
   * Process pending operations
   * @private
   * @returns {Promise<void>} Promise that resolves when operations are processed
   */
  async _processPendingOps() {
    if (!this._pendingOps || this._pendingOps.size === 0) {
      return;
    }

    try {
      // Create a transaction for all pending operations
      const db = this.db._state._db;
      const storeName = this._getCfName();

      const tx = db.transaction([storeName], "readwrite");
      const store = tx.objectStore(storeName);

      // Process each operation
      for (const op of this._pendingOps.values()) {
        if (op.type === "put") {
          store.put(op.value, op.key);
        } else if (op.type === "delete") {
          store.delete(op.key);
        }
      }

      // Wait for transaction to complete
      await new Promise((resolve, reject) => {
        tx.oncomplete = resolve;
        tx.onerror = (event) => {
          console.error("Error in batch transaction:", event.target.error);
          reject(event.target.error);
        };
      });

      // Clear pending operations
      this._pendingOps.clear();
    } catch (err) {
      console.error("Error processing pending operations:", err);
      throw err;
    }
  }

  /**
   * Try to delete a range of keys
   * This method matches RocksDB's API
   * @param {*} start - Start key (inclusive)
   * @param {*} end - End key (exclusive)
   * @returns {Promise<void>} Promise that resolves when the operation is complete
   */
  async tryDeleteRange(start, end) {
    console.log("Using actual tryDeleteRange method");
    try {
      if (this.destroyed || !this.write) {
        return Promise.resolve();
      }

      this.deleteRange(start, end);
      return Promise.resolve();
    } catch (err) {
      console.error("Error in tryDeleteRange:", err);
      return Promise.resolve();
    }
  }

  /**
   * Safe implementation of tryDeleteRange
   * @private
   * @param {*} start - Start key
   * @param {*} end - End key
   * @returns {Promise<void>} Promise that resolves immediately
   */
  async _safeTryDeleteRange(start, end) {
    console.log("Using fallback _safeTryDeleteRange method");
    try {
      if (this.destroyed || !this.write) {
        return Promise.resolve();
      }

      // Create range operation
      this._rangeOperations = this._rangeOperations || [];

      const startStr = typeof start === "string" ? start : start.toString();
      const endStr = typeof end === "string" ? end : end.toString();

      this._rangeOperations.push({
        type: "deleteRange",
        start: startStr,
        end: endStr,
      });

      // Since this is a "try" method, we should try to execute it immediately
      if (this.db && this.db._state && this.db._state._db) {
        const db = this.db._state._db;
        const storeName = this._getCfName
          ? this._getCfName()
          : this.db._columnFamily || "default";

        this._processRangeDelete(db, storeName, startStr, endStr).catch(
          (err) => {
            console.error("Error in _safeTryDeleteRange range delete:", err);
          }
        );
      }

      return Promise.resolve();
    } catch (err) {
      console.error("Error in _safeTryDeleteRange:", err);
      return Promise.resolve();
    }
  }
}

export default Batch;
