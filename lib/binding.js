// IndexedDB-based implementation of the RocksDB binding interface

// Use the native IndexedDB in the browser
let idb = typeof window !== "undefined" ? window.indexedDB : null;

// Track database connections by path
const dbConnections = new Map();
// Track suspended databases by path
const suspendedDatabases = new Map();
const DB_VERSION = 1;

// Add a new map to track pending flush operations during suspension
const pendingFlushOperations = new Map();

// Main binding interface that mimics the RocksDB native bindings
const binding = {
  // For testing - allows injecting fake IndexedDB
  _setIndexedDB(indexedDB) {
    idb = indexedDB;
  },

  // For state management - allows accessing the connections map
  _getDbConnections() {
    return dbConnections;
  },

  // Initialize a new database instance
  init(
    readOnly,
    createIfMissing,
    createMissingColumnFamilies,
    maxBackgroundJobs,
    bytesPerSync,
    maxOpenFiles,
    useDirectReads
  ) {
    return {
      readOnly,
      createIfMissing,
      createMissingColumnFamilies,
      columnFamilies: new Map(),
      snapshots: new Map(),
      nextSnapshotId: 1,
      path: null,
      db: null,
      suspended: false,
      closing: false,
      closed: false,
      iterators: new Set(),
      id: Math.random().toString(36).substring(2, 10), // Unique ID for each database instance
    };
  },

  // Open the database
  open(handle, state, path, columnFamilyHandles, req, callback) {
    if (!idb) {
      callback("IndexedDB not available");
      return req;
    }

    // Ensure handle has an ID
    if (!handle.id) {
      handle.id = Math.random().toString(36).substring(2, 10);
    }

    handle.path = path;

    // Check if this path is suspended by another instance
    if (suspendedDatabases.has(path)) {
      const suspendedInfo = suspendedDatabases.get(path);
      // If this is not the same handle that suspended it, we're taking ownership
      if (suspendedInfo.handleId !== handle.id) {
        console.log(
          `Database ${path} claimed by ${handle.id}, was suspended by ${suspendedInfo.handleId}`
        );
        // Mark that we've claimed this path from another instance
        suspendedDatabases.set(path, {
          ...suspendedInfo,
          claimed: true,
        });
      }
    }

    // Open IndexedDB connection
    const request = idb.open(path, DB_VERSION);

    request.onupgradeneeded = (event) => {
      const db = event.target.result;

      // Create object stores for each column family
      columnFamilyHandles.forEach((cfHandle) => {
        if (!db.objectStoreNames.contains(cfHandle.name)) {
          db.createObjectStore(cfHandle.name);
        }
      });
    };

    request.onsuccess = (event) => {
      const db = event.target.result;
      handle.db = db;

      // Initialize column families
      columnFamilyHandles.forEach((cfHandle) => {
        handle.columnFamilies.set(cfHandle.name, cfHandle);
      });

      // Store this connection
      dbConnections.set(path, {
        db: db,
        handleId: handle.id,
        timestamp: Date.now(),
      });

      callback(null);
    };

    request.onerror = (event) => {
      callback(event.target.error.message);
    };

    return req;
  },

  // Close the database
  close(handle, req, callback) {
    // Set flags that this DB is closing/closed
    handle.closing = true;

    // Reject all active iterators
    if (handle.iterators) {
      for (const iterator of handle.iterators) {
        iterator.isReleased = true;
      }
      handle.iterators.clear();
    }

    // Important: check if this handle claimed a suspended database path
    for (const [path, info] of suspendedDatabases.entries()) {
      if (info.claimed && path === handle.path) {
        // Update the claimed status to false
        suspendedDatabases.set(path, {
          ...info,
          claimed: false,
        });
        console.log(
          `Database ${path} released by ${handle.id}, original owner can resume`
        );
      }
    }

    // Reject any pending flush operations
    if (pendingFlushOperations.has(handle.path)) {
      const pendingOps = pendingFlushOperations.get(handle.path);
      console.log(
        `Rejecting ${pendingOps.length} pending flush operations for ${handle.path}`
      );

      // Reject each callback immediately
      for (let i = 0; i < pendingOps.length; i++) {
        pendingOps[i]("Database closed before flush could complete");
      }

      // Clear the pending operations
      pendingFlushOperations.delete(handle.path);
    }

    if (handle.db) {
      handle.db.close();

      // Remove from connection tracking
      dbConnections.delete(handle.path);
      handle.db = null;
    }

    handle.closed = true;
    callback(null);
    return req;
  },

  // Flush data (IndexedDB is immediately persistent, but we need to handle suspension correctly)
  flush(handle, columnFamilyHandle, req, callback) {
    // Check if suspended - queue the flush to be executed after resume
    if (handle.suspended) {
      // Store the callback in our pending operations list
      if (!pendingFlushOperations.has(handle.path)) {
        pendingFlushOperations.set(handle.path, []);
      }

      pendingFlushOperations.get(handle.path).push(callback);
      console.log(`Flush operation queued for ${handle.path} while suspended`);
      return req;
    }

    // Check if closing or closed
    if (handle.closing || handle.closed || !handle.db) {
      callback("Database is closed or closing");
      return req;
    }

    // Execute callback immediately (synchronously)
    callback(null);
    return req;
  },

  // Create a read batch operation
  readInit() {
    return { operations: [], results: [] };
  },

  // Prepare buffer for read operations
  readBuffer(handle, capacity) {
    return { capacity };
  },

  // Execute read operations with snapshot support
  read(
    dbHandle,
    batchHandle,
    operations,
    snapshotHandle,
    batchInstance,
    callback
  ) {
    const results = [];
    const errors = [];
    const db = dbHandle.db;

    if (!db) {
      operations.forEach(() => {
        errors.push("Database is closed");
        results.push(null);
      });

      callback(errors, results);
      return;
    }

    let completed = 0;

    operations.forEach((op, i) => {
      const storeName = op.columnFamily.name;

      // Check if we should use snapshot data
      if (
        snapshotHandle &&
        snapshotHandle.id &&
        dbHandle.snapshots.has(snapshotHandle.id)
      ) {
        const snapshot = dbHandle.snapshots.get(snapshotHandle.id);
        if (snapshot && snapshot.data && snapshot.data.has(storeName)) {
          // Use snapshot data instead of live data
          const snapshotData = snapshot.data.get(storeName);
          const key = op.key.toString();

          if (snapshotData.has(key)) {
            // Get the value from snapshot
            const value = snapshotData.get(key);
            // Ensure it's returned as a Buffer
            results[i] = Buffer.isBuffer(value)
              ? value
              : Buffer.from(value || "");
          } else {
            results[i] = null;
          }

          errors[i] = null;
          completed++;
          if (completed === operations.length) {
            callback(errors, results);
          }
          return;
        }
      }

      // Fall back to live data
      const transaction = db.transaction([storeName], "readonly");
      const store = transaction.objectStore(storeName);

      const request = store.get(op.key.toString());

      request.onsuccess = (event) => {
        results[i] = event.target.result
          ? Buffer.from(event.target.result)
          : null;
        errors[i] = null;

        completed++;
        if (completed === operations.length) {
          callback(errors, results);
        }
      };

      request.onerror = (event) => {
        results[i] = null;
        errors[i] = event.target.error.message;

        completed++;
        if (completed === operations.length) {
          callback(errors, results);
        }
      };
    });
  },

  // Create a write batch operation
  writeInit() {
    return { operations: [] };
  },

  // Prepare buffer for write operations
  writeBuffer(handle, capacity) {
    return { capacity };
  },

  // Execute write operations
  write(dbHandle, batchHandle, operations, batchInstance, callback) {
    const db = dbHandle.db;
    const errors = [];

    if (!db) {
      operations.forEach(() => {
        errors.push("Database not open");
      });

      callback(errors);
      return;
    }

    if (dbHandle.readOnly) {
      operations.forEach(() => {
        errors.push("Not supported operation in read only mode");
      });

      callback(errors);
      return;
    }

    // Group operations by column family for transaction efficiency
    const operationsByStore = new Map();

    operations.forEach((op, i) => {
      const storeName = op.columnFamily.name;
      if (!operationsByStore.has(storeName)) {
        operationsByStore.set(storeName, []);
      }
      operationsByStore.get(storeName).push({ op, index: i });
    });

    // Create one transaction per store
    const storeNames = Array.from(operationsByStore.keys());
    const transaction = db.transaction(storeNames, "readwrite");

    transaction.oncomplete = () => {
      callback(errors);
    };

    transaction.onerror = (event) => {
      // Fill all errors
      operations.forEach((_, i) => {
        if (!errors[i]) errors[i] = event.target.error.message;
      });

      callback(errors);
    };

    // Process each store's operations
    for (const [storeName, ops] of operationsByStore.entries()) {
      const store = transaction.objectStore(storeName);

      for (const { op, index } of ops) {
        errors[index] = null;

        try {
          if (op.type === "put") {
            store.put(op.value, op.key.toString());
          } else if (op.type === "del") {
            store.delete(op.key.toString());
          } else if (op.type === "delRange") {
            // For delete range, we need to get all keys in the range first
            const keyRange = IDBKeyRange.bound(
              op.start.toString(),
              op.end.toString(),
              false, // Include lower bound
              true // Exclude upper bound
            );

            const cursorRequest = store.openCursor(keyRange);

            cursorRequest.onsuccess = (event) => {
              const cursor = event.target.result;
              if (cursor) {
                cursor.delete();
                cursor.continue();
              }
            };
          }
        } catch (err) {
          errors[index] = err.message;
        }
      }
    }
  },

  // Create a new snapshot
  snapshotInit() {
    return {
      id: Math.random().toString(36).substring(2, 10), // Generate unique ID
      data: new Map(),
      timestamp: Date.now(),
    };
  },

  // Capture snapshot data
  async snapshotGet(dbHandle, handle) {
    if (!handle.id) {
      handle.id = ++dbHandle.snapshotCounter || 1;
    }

    // Clear any existing data
    handle.data.clear();

    // Store reference in DB handle
    dbHandle.snapshots.set(handle.id, handle);

    const db = dbHandle.db;
    if (!db) {
      console.error("SnapshotGet: Database is not open");
      dbHandle.snapshots.delete(handle.id);
      return;
    }

    // Get all column family names
    const cfNames = Array.from(dbHandle.columnFamilies.keys());
    if (cfNames.length === 0) return;

    try {
      // Create ONE transaction for ALL object stores to ensure atomicity
      const transaction = db.transaction(cfNames, "readonly");

      // For each column family, capture its data atomically within this transaction
      for (const cfName of cfNames) {
        // Create a Map to store the snapshot data for this column family
        const cfData = new Map();
        handle.data.set(cfName, cfData);

        // Get all keys and values in one atomic operation
        const store = transaction.objectStore(cfName);

        // Get all keys
        const keysPromise = new Promise((resolve, reject) => {
          const req = store.getAllKeys();
          req.onsuccess = (e) => resolve(e.target.result);
          req.onerror = (e) => reject(e.target.error);
        });

        // Get all values
        const valuesPromise = new Promise((resolve, reject) => {
          const req = store.getAll();
          req.onsuccess = (e) => resolve(e.target.result);
          req.onerror = (e) => reject(e.target.error);
        });

        // Wait for both keys and values to be fetched
        const [keys, values] = await Promise.all([keysPromise, valuesPromise]);

        // Store each key-value pair in the snapshot data
        for (let i = 0; i < keys.length; i++) {
          const keyStr = keys[i].toString();
          const value = values[i];
          // Ensure values are stored as Buffers
          cfData.set(
            keyStr,
            Buffer.isBuffer(value) ? value : Buffer.from(value || "")
          );
        }
      }

      // Wait for transaction to complete to ensure all data is captured
      await new Promise((resolve, reject) => {
        transaction.oncomplete = resolve;
        transaction.onerror = (e) => reject(e.target.error);
      });
    } catch (err) {
      console.error("SnapshotGet: Failed to capture snapshot:", err);
      handle.data.clear();
      dbHandle.snapshots.delete(handle.id);
    }
  },

  snapshotRelease(dbHandle, handle) {
    if (handle && handle.id) {
      dbHandle.snapshots.delete(handle.id);
    }
  },

  // Iterator management
  iteratorInit(dbHandle, cfHandle, options = {}) {
    // Extract options with defaults
    const {
      gt = null,
      gte = null,
      lt = null,
      lte = null,
      reverse = false,
      limit = 0,
      keyEncoding = null,
      valueEncoding = null,
      snapshot = null,
      prefix = null,
    } = options;

    // Ensure database is open
    assertOpen(dbHandle, "IteratorInit");

    // Generate a unique ID for this iterator
    const id = getRandomId();

    // Store iterator state
    _iterators[id] = {
      dbHandle,
      cfHandle,
      options: {
        ...options,
        keyEncoding,
        valueEncoding,
        snapshot: snapshot ? snapshot._handle || null : null,
      },
      gt:
        gt !== null
          ? keyEncoding
            ? c.encode(keyEncoding, gt)
            : Buffer.from(gt)
          : null,
      gte:
        gte !== null
          ? keyEncoding
            ? c.encode(keyEncoding, gte)
            : Buffer.from(gte)
          : null,
      lt:
        lt !== null
          ? keyEncoding
            ? c.encode(keyEncoding, lt)
            : Buffer.from(lt)
          : null,
      lte:
        lte !== null
          ? keyEncoding
            ? c.encode(keyEncoding, lte)
            : Buffer.from(lte)
          : null,
      prefix:
        prefix !== null
          ? keyEncoding
            ? c.encode(keyEncoding, prefix)
            : Buffer.from(prefix)
          : null,
      currentKey: null,
      iterating: false,
    };

    return id;
  },

  iteratorSeek(handle, target) {
    return new Promise(async (resolve, reject) => {
      if (handle.isReleased) {
        reject(new Error("Iterator is released"));
        return;
      }

      // Check for database closure
      if (
        handle.db.suspended &&
        (handle.db.closing || handle.db.closed || !handle.db.db)
      ) {
        reject(new Error("Database was closed during suspension"));
        return;
      }

      // Set seeking flag and reset state
      handle._seeking = true;
      handle._count = 0;
      handle._first = true;
      handle._cursor = null;
      handle._snapshotData = null;
      handle._snapshotIndex = 0;
      if (handle._nextPromise) {
        // If a previous next() was pending, reject it as seek invalidates it
        handle._nextPromise.reject(
          new Error("Iterator seek invalidated pending next()")
        );
        handle._nextPromise = null;
      }

      try {
        // If using a snapshot, prepare the snapshot data
        if (handle.snapshotId) {
          const snapshot = handle.db.snapshots.get(handle.snapshotId);
          if (
            snapshot &&
            snapshot.data &&
            snapshot.data.has(handle.columnFamily.name)
          ) {
            const cfData = snapshot.data.get(handle.columnFamily.name);

            // Convert Map to Array and sort
            handle._snapshotData = Array.from(cfData.entries())
              .map(([key, value]) => ({ key: key, value: value })) // Already buffers
              .sort((a, b) => {
                // Compare keys directly (assuming they are strings/buffers)
                const keyA = Buffer.isBuffer(a.key)
                  ? a.key
                  : Buffer.from(a.key);
                const keyB = Buffer.isBuffer(b.key)
                  ? b.key
                  : Buffer.from(b.key);
                const cmp = Buffer.compare(keyA, keyB);
                return handle.reverse ? -cmp : cmp;
              });
          } else {
            // Snapshot exists but no data for this CF, or snapshot missing
            handle._snapshotData = []; // Ensure it's an empty array
          }
        } else {
          // Not using snapshot, prepare for live cursor iteration
          // The actual cursor opening will happen in the first iteratorNext call
        }

        // Resolve immediately, next() will do the work
        handle._seeking = false;
        resolve();
      } catch (err) {
        handle._seeking = false;
        reject(err);
      }
    });
  },

  iteratorNext(handle) {
    return new Promise((resolve, reject) => {
      // Helper to resolve/reject and clear promise, ensuring it's called only once
      let finished = false;
      const finish = (action, value) => {
        if (finished || !handle._nextPromise) return;
        finished = true;
        if (action === "resolve") handle._nextPromise.resolve(value);
        else handle._nextPromise.reject(value);
        handle._nextPromise = null;
      };

      // Prevent concurrent next() calls
      if (handle._nextPromise) {
        console.warn(
          "[Iterator Binding] Concurrent next() call detected, returning existing promise."
        );
        return handle._nextPromise.promise.then(resolve).catch(reject);
      }

      // Store the promise resolvers for the current call
      let currentPromise = {};
      const promise = new Promise((res, rej) => {
        currentPromise.resolve = res;
        currentPromise.reject = rej;
      });
      handle._nextPromise = {
        promise,
        resolve: currentPromise.resolve,
        reject: currentPromise.reject,
      };

      const proceed = async () => {
        try {
          if (handle.isReleased) {
            throw new Error("Iterator is released");
          }

          if (
            handle.db.suspended &&
            (handle.db.closing || handle.db.closed || !handle.db.db)
          ) {
            throw new Error("Database was closed during suspension");
          }

          if (handle._count >= handle.limit) {
            finish("resolve", { key: null, value: null });
            return;
          }

          // --- Snapshot Iteration ---
          if (handle.snapshotId) {
            if (
              !handle._snapshotData ||
              handle._snapshotIndex >= handle._snapshotData.length
            ) {
              finish("resolve", { key: null, value: null });
              return;
            }
            const entry = handle._snapshotData[handle._snapshotIndex];
            handle._snapshotIndex++;
            handle._count++;
            // Assume snapshot data is already correct Buffer format from snapshotGet
            const decodedKey = handle.keyEncoding
              ? c.decode(handle.keyEncoding, entry.key)
              : entry.key;
            const decodedValue = handle.valueEncoding
              ? c.decode(handle.valueEncoding, entry.value)
              : entry.value;
            finish("resolve", { key: decodedKey, value: decodedValue });
            return;
          }

          // --- Live Data Iteration (Cursor) ---
          const db = handle.db.db;
          if (!db) {
            throw new Error("Database is closed");
          }

          const storeName = handle.columnFamily.name;
          const direction = handle.reverse ? "prev" : "next";

          const processCursorResult = (cursor) => {
            handle._cursor = cursor; // Store current cursor
            if (!cursor) {
              finish("resolve", { key: null, value: null }); // End of iteration
              return;
            }

            handle._count++;
            const key = cursor.key;
            const value = cursor.value;
            let keyBuffer, valueBuffer;

            // Ensure key/value are Buffers
            try {
              keyBuffer = Buffer.isBuffer(key) ? key : Buffer.from(key);
              valueBuffer = Buffer.isBuffer(value)
                ? value
                : Buffer.from(value || ""); // Handle null/undefined value
            } catch (bufferErr) {
              throw new Error(
                `Failed to convert cursor key/value to buffer: ${bufferErr.message}`
              );
            }

            // Decode key/value
            let decodedKey, decodedValue;
            try {
              decodedKey = handle.keyEncoding
                ? c.decode(handle.keyEncoding, keyBuffer)
                : keyBuffer;
              decodedValue = handle.valueEncoding
                ? c.decode(handle.valueEncoding, valueBuffer)
                : valueBuffer;
            } catch (decodeErr) {
              throw new Error(
                `Failed to decode key/value: ${decodeErr.message}`
              );
            }

            finish("resolve", { key: decodedKey, value: decodedValue });
            // Note: We resolve here, the cursor needs explicit .continue() on the *next* call
          };

          // If we have an existing cursor, continue it
          if (handle._cursor) {
            // Wrap continue in a request-like structure for error handling consistency
            try {
              // Create a temporary promise for the continue operation
              const continuePromise = new Promise(
                (continueResolve, continueReject) => {
                  // Define temporary handlers for this specific continue call
                  const onSuccess = (event) => {
                    processCursorResult(event.target.result);
                    continueResolve(); // Resolve the continue promise
                  };
                  const onError = (event) => {
                    continueReject(
                      event.target.error || new Error("Cursor continue failed")
                    );
                  };

                  // Assign temporary handlers
                  handle._cursorRequest.onsuccess = onSuccess;
                  handle._cursorRequest.onerror = onError;

                  // Call continue
                  handle._cursor.continue();
                }
              );
              await continuePromise; // Wait for continue to finish
            } catch (continueErr) {
              finish("reject", continueErr); // Reject the main promise if continue fails
            }
            return; // Finished processing continue
          }

          // If it's the first call or cursor was exhausted, open a new one
          if (handle._first) {
            handle._first = false;

            const transaction = db.transaction([storeName], "readonly");
            const store = transaction.objectStore(storeName);
            let keyRange = null;

            try {
              const lower = handle.gte ?? handle.gt;
              const upper = handle.lte ?? handle.lt;
              const lowerOpen = handle.gt !== null && handle.gt !== undefined;
              const upperOpen = handle.lt !== null && handle.lt !== undefined;

              // Convert keys to appropriate type for IDBKeyRange if needed (assuming string/Buffer)
              const lowerKey = Buffer.isBuffer(lower)
                ? lower.toString()
                : lower;
              const upperKey = Buffer.isBuffer(upper)
                ? upper.toString()
                : upper;

              if (lowerKey !== undefined && upperKey !== undefined) {
                keyRange = IDBKeyRange.bound(
                  lowerKey,
                  upperKey,
                  lowerOpen,
                  upperOpen
                );
              } else if (lowerKey !== undefined) {
                keyRange = IDBKeyRange.lowerBound(lowerKey, lowerOpen);
              } else if (upperKey !== undefined) {
                keyRange = IDBKeyRange.upperBound(upperKey, upperOpen);
              }
            } catch (e) {
              throw new Error(
                `Invalid key format for IDBKeyRange: ${e.message}`
              );
            }

            handle._cursorRequest = store.openCursor(keyRange, direction);

            handle._cursorRequest.onerror = (event) => {
              finish(
                "reject",
                event.target.error || new Error("Failed to open cursor")
              );
            };

            handle._cursorRequest.onsuccess = (event) => {
              try {
                processCursorResult(event.target.result);
              } catch (processErr) {
                finish("reject", processErr);
              }
            };
          } else {
            // Should have been handled by the handle._cursor check above
            // If we reach here, it means the cursor ended previously
            finish("resolve", { key: null, value: null });
          }
        } catch (err) {
          finish("reject", err);
        }
      };

      proceed();

      promise.then(resolve).catch(reject);
    });
  },

  iteratorRelease(handle) {
    // Remove from tracked iterators
    if (handle.db && handle.db.iterators) {
      handle.db.iterators.delete(handle);
    }

    // Reject any pending next() promise
    if (handle._nextPromise) {
      handle._nextPromise.reject(new Error("Iterator released"));
      handle._nextPromise = null;
    }

    // Mark as released and clear internal state
    handle.isReleased = true;
    handle._cursor = null;
    handle._cursorRequest = null;
    handle._snapshotData = null;
  },

  // Suspend operations
  suspend(handle, req, callback) {
    // Already suspended
    if (handle.suspended) {
      callback(null);
      return req;
    }

    // Store this handle's unique ID so we can track ownership
    if (!handle.id) {
      handle.id = Math.random().toString(36).substring(2, 10);
    }

    // Record this handle as the owner of the suspended database
    suspendedDatabases.set(handle.path, {
      handleId: handle.id,
      timestamp: Date.now(),
      claimed: false,
    });

    console.log(`Database ${handle.path} suspended by ${handle.id}`);

    handle.suspended = true;
    callback(null);
    return req;
  },

  // Resume operations
  resume(handle, req, callback) {
    // Not suspended
    if (!handle.suspended) {
      callback(null);
      return req;
    }

    // Check if this database path was claimed by another instance during suspension
    const suspendedInfo = suspendedDatabases.get(handle.path);
    if (suspendedInfo) {
      // Only reject if it's still claimed
      if (suspendedInfo.claimed === true) {
        callback("Database was opened by another instance during suspension");
        return req;
      }
    } else {
      // The entry might have been removed completely (should not happen)
      console.log(
        `Warning: resume called but no suspended entry found for ${handle.path}`
      );
    }

    // Check if closing or closed
    if (handle.closing || handle.closed || !handle.db) {
      callback("Database is closed or closing");
      return req;
    }

    // Execute any pending flush operations - SYNCHRONOUSLY
    if (pendingFlushOperations.has(handle.path)) {
      const pendingOps = pendingFlushOperations.get(handle.path);
      console.log(
        `Executing ${pendingOps.length} pending flush operations for ${handle.path}`
      );

      // Execute each callback immediately
      for (let i = 0; i < pendingOps.length; i++) {
        pendingOps[i](null);
      }

      // Clear the pending operations
      pendingFlushOperations.delete(handle.path);
    }

    // Remove from suspended databases
    suspendedDatabases.delete(handle.path);

    handle.suspended = false;
    callback(null);
    return req;
  },
};

export default binding;
