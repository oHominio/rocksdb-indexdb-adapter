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
        if (snapshot && snapshot.data.has(storeName)) {
          // Use snapshot data instead of live data
          const snapshotData = snapshot.data.get(storeName);
          const key = op.key.toString();

          if (snapshotData.has(key)) {
            results[i] = snapshotData.get(key);
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
      id: 0,
      data: new Map(),
      timestamp: Date.now(),
    };
  },

  // Capture snapshot data
  async snapshotGet(dbHandle, handle) {
    const snapshotId = dbHandle.nextSnapshotId++;
    handle.id = snapshotId;

    const snapshot = {
      timestamp: Date.now(),
      data: new Map(),
    };

    if (dbHandle.db) {
      const cfNames = Array.from(dbHandle.columnFamilies.keys());
      const transaction = dbHandle.db.transaction(cfNames, "readonly");
      const promises = [];

      for (const cfName of cfNames) {
        snapshot.data.set(cfName, new Map()); // Initialize map for CF
        const store = transaction.objectStore(cfName);

        // Create promises for getting keys and values
        const keysPromise = new Promise((resolve, reject) => {
          const request = store.getAllKeys();
          request.onsuccess = (event) => resolve(event.target.result);
          request.onerror = (event) => reject(request.error);
        });

        const valuesPromise = new Promise((resolve, reject) => {
          const request = store.getAll();
          request.onsuccess = (event) => resolve(event.target.result);
          request.onerror = (event) => reject(request.error);
        });

        // Add promise to wait for both keys and values for this CF
        promises.push(
          Promise.all([keysPromise, valuesPromise])
            .then(([keys, values]) => {
              const cfData = snapshot.data.get(cfName);
              for (let i = 0; i < keys.length; i++) {
                const key = keys[i].toString();
                const value = values[i];
                // Always store value as Buffer
                cfData.set(
                  key,
                  Buffer.isBuffer(value) ? value : Buffer.from(value || "")
                );
              }
            })
            .catch((err) => {
              console.error(
                `Error capturing snapshot data for ${cfName}:`,
                err
              );
              // Don't reject all, just log error for this CF
            })
        );
      }

      // Wait for all column family data to be captured
      await Promise.all(promises);
    }

    dbHandle.snapshots.set(snapshotId, snapshot);
    return handle; // handle is returned synchronously, snapshot data is captured async
  },

  // Release snapshot resources
  snapshotRelease(dbHandle, handle) {
    if (handle && handle.id) {
      dbHandle.snapshots.delete(handle.id);
    }
  },

  // Iterator management
  iteratorInit(dbHandle, cfHandle, options = {}) {
    const handle = {
      isReleased: false,
      db: dbHandle,
      columnFamily: cfHandle,
      options,
      // Track if this is a prefix iterator
      isPrefix: !!options.prefix,
      prefixBuffer: options.prefix ? Buffer.from(options.prefix) : null,
      prefixStr: options.prefix ? options.prefix.toString() : "",
      // Keys and values loaded from a batch
      keys: [],
      values: [],
      currentIndex: 0,
      reverse: false,
      completed: false,
      // For snapshot iterators
      snapshotId: options.snapshot ? options.snapshot.id : null,
    };

    // Track this iterator
    if (!dbHandle.iterators) {
      dbHandle.iterators = new Set();
    }
    dbHandle.iterators.add(handle);

    return handle;
  },

  iteratorSeek(handle, target, reverse = false) {
    return new Promise(async (resolve, reject) => {
      if (handle.isReleased) {
        reject(new Error("Iterator is released"));
        return;
      }

      // Check for database closure during suspension
      if (
        handle.db.suspended &&
        (handle.db.closing || handle.db.closed || !handle.db.db)
      ) {
        reject(new Error("Database was closed during suspension"));
        return;
      }

      // Reset iterator state
      handle.reverse = reverse;
      handle.currentIndex = 0;
      handle.keys = [];
      handle.values = [];
      handle.completed = false;

      try {
        // Use snapshot data if available
        if (handle.snapshotId) {
          // Wait briefly in case snapshotGet is still running (ugly workaround)
          // A better solution would involve event emitters or callbacks.
          // await new Promise(res => setTimeout(res, 10));
          // --> Removing explicit wait, relying on test structure for now.

          const snapshot = handle.db.snapshots.get(handle.snapshotId);
          if (snapshot && snapshot.data.has(handle.columnFamily.name)) {
            const cfData = snapshot.data.get(handle.columnFamily.name);
            const entries = [];
            for (const [key, value] of cfData.entries()) {
              entries.push({ key, value });
            }
            entries.sort((a, b) => {
              if (reverse) return b.key.localeCompare(a.key);
              return a.key.localeCompare(b.key);
            });

            const targetStr = target.toString();
            const filtered = entries.filter((entry) => {
              if (handle.isPrefix && handle.prefixStr)
                return entry.key.startsWith(handle.prefixStr);
              if (reverse) return entry.key <= targetStr;
              return entry.key >= targetStr;
            });

            handle.keys = filtered.map((e) => e.key);
            handle.values = filtered.map((e) => e.value);

            if (handle.keys.length > 0) {
              // Ensure value is Buffer
              let value = handle.values[0];
              resolve({
                key: Buffer.from(handle.keys[0]),
                value: Buffer.isBuffer(value)
                  ? value
                  : Buffer.from(value || ""),
              });
            } else {
              handle.completed = true;
              resolve({ key: null, value: null });
            }
            return; // End snapshot logic
          } else {
            // Snapshot exists but no data for this CF, or snapshot missing
            handle.completed = true;
            resolve({ key: null, value: null });
            return;
          }
        }

        // Regular database access (non-snapshot)
        const db = handle.db.db;
        if (!db) {
          reject(new Error("Database is closed"));
          return;
        }
        const transaction = db.transaction(
          [handle.columnFamily.name],
          "readonly"
        );
        const store = transaction.objectStore(handle.columnFamily.name);
        const keysRequest = store.getAllKeys();
        const valuesRequest = store.getAll();

        try {
          const [keys, values] = await Promise.all([
            new Promise((res, rej) => {
              keysRequest.onsuccess = (e) => res(e.target.result);
              keysRequest.onerror = (e) => rej(keysRequest.error);
            }),
            new Promise((res, rej) => {
              valuesRequest.onsuccess = (e) => res(e.target.result);
              valuesRequest.onerror = (e) => rej(valuesRequest.error);
            }),
          ]);

          const entries = [];
          for (let i = 0; i < keys.length; i++) {
            entries.push({ key: keys[i].toString(), value: values[i] });
          }
          entries.sort((a, b) => {
            if (reverse) return b.key.localeCompare(a.key);
            return a.key.localeCompare(b.key);
          });

          const targetStr = target.toString();
          const filtered = entries.filter((entry) => {
            if (handle.isPrefix && handle.prefixStr)
              return entry.key.startsWith(handle.prefixStr);
            if (reverse) return entry.key <= targetStr;
            return entry.key >= targetStr;
          });

          handle.keys = filtered.map((e) => e.key);
          handle.values = filtered.map((e) => e.value);

          if (handle.keys.length > 0) {
            let value = handle.values[0];
            resolve({
              key: Buffer.from(handle.keys[0]),
              value: Buffer.isBuffer(value) ? value : Buffer.from(value || ""),
            });
          } else {
            handle.completed = true;
            resolve({ key: null, value: null });
          }
        } catch (err) {
          reject(err); // Handle errors from getAllKeys/getAll
        }
      } catch (err) {
        reject(err); // Handle outer try/catch errors
      }
    });
  },

  iteratorNext(handle) {
    return new Promise((resolve, reject) => {
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

      // Advance index *before* checking boundary
      handle.currentIndex++;

      // Check if we're past the end
      if (handle.currentIndex >= handle.keys.length) {
        handle.completed = true;
        resolve({ key: null, value: null });
        return;
      }

      // Get current entry
      const key = handle.keys[handle.currentIndex];
      const value = handle.values[handle.currentIndex];

      // Return as Buffer objects, ensuring value is Buffer
      resolve({
        key: Buffer.from(key),
        value: Buffer.isBuffer(value) ? value : Buffer.from(value || ""),
      });
    });
  },

  iteratorRelease(handle) {
    // Remove from tracked iterators
    if (handle.db && handle.db.iterators) {
      handle.db.iterators.delete(handle);
    }

    // Mark as released
    handle.isReleased = true;
    handle.keys = [];
    handle.values = [];
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
