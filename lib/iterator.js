import { Readable } from "streamx";
import * as c from "compact-encoding";
import binding from "./binding.js";

const empty = Buffer.alloc(0);

// Default timeout for auto-cleanup (ms)
const ITERATOR_TIMEOUT = 60000; // Standard timeout for production use

// Debug function
function debug(...args) {
  if (process.env.DEBUG_ITERATOR) {
    console.log("[ITERATOR DEBUG]", ...args);
  }
}

// Environment detection
const isBrowser =
  typeof window !== "undefined" && typeof process === "undefined";

/**
 * Cross-platform resource tracker for iterators
 */
export const ResourceTracker = {
  _registry: new Map(), // Maps IDs to iterators or weak references

  // Register an iterator
  register(iterator) {
    const id = iterator._id;

    if (isBrowser && typeof WeakRef !== "undefined") {
      // Use WeakRef in modern browsers to avoid memory leaks
      this._registry.set(id, new WeakRef(iterator));
    } else {
      // Directly track in Node.js or older browsers
      this._registry.set(id, {
        ref: iterator,
        timestamp: Date.now(),
      });
    }

    debug(
      `ResourceTracker: Registered iterator ${id} (total: ${this._registry.size})`
    );
    return id;
  },

  // Unregister an iterator
  unregister(id) {
    const result = this._registry.delete(id);
    debug(
      `ResourceTracker: Unregistered iterator ${id} (total: ${this._registry.size})`
    );
    return result;
  },

  // Get all active iterators
  getActive() {
    const active = [];

    for (const [id, entry] of this._registry.entries()) {
      if (entry instanceof WeakRef) {
        const iterator = entry.deref();
        if (iterator) {
          active.push(iterator);
        } else {
          // Clean up if the WeakRef is dead
          this._registry.delete(id);
        }
      } else if (entry && entry.ref) {
        active.push(entry.ref);
      }
    }

    return active;
  },

  // Get count of active iterators
  get size() {
    return this._registry.size;
  },

  // Run periodic cleanup
  runCleanup() {
    debug(
      `ResourceTracker: Running cleanup, before: ${this._registry.size} tracked iterators`
    );

    // Clean up any orphaned WeakRefs
    if (typeof WeakRef !== "undefined") {
      for (const [id, entry] of this._registry.entries()) {
        if (entry instanceof WeakRef && !entry.deref()) {
          this._registry.delete(id);
        }
      }
    }

    // For regular refs, check if they're too old
    const now = Date.now();
    for (const [id, entry] of this._registry.entries()) {
      if (
        entry &&
        entry.timestamp &&
        now - entry.timestamp > ITERATOR_TIMEOUT
      ) {
        // Try to destroy if we still have access to the iterator
        if (entry.ref && !entry.ref._destroyed) {
          try {
            debug(`ResourceTracker: Auto-destroying stale iterator ${id}`);
            entry.ref.destroy(
              new Error("Iterator auto-cleanup triggered by ResourceTracker")
            );
          } catch (e) {
            // Ignore errors when cleaning up
          }
        }
        this._registry.delete(id);
      }
    }

    debug(
      `ResourceTracker: Cleanup complete, after: ${this._registry.size} tracked iterators`
    );
  },

  // Force cleanup all tracked iterators
  forceCleanupAll() {
    debug(`ResourceTracker: Force cleaning ${this._registry.size} iterators`);

    const active = this.getActive();
    for (const iterator of active) {
      try {
        if (!iterator._destroyed) {
          debug(`ResourceTracker: Force destroying iterator ${iterator._id}`);
          iterator.destroy(
            new Error("Iterator force-destroyed by ResourceTracker")
          );
        }
      } catch (e) {
        // Ignore errors during forced cleanup
      }
    }

    this._registry.clear();
  },
};

// Set up periodic cleanup
const cleanupInterval =
  typeof setInterval !== "undefined"
    ? setInterval(() => ResourceTracker.runCleanup(), 30000)
    : null;

// Setup cleanup on process exit for Node.js
if (!isBrowser && typeof process !== "undefined") {
  process.on("exit", () => {
    debug("Process exit event received");
    if (cleanupInterval) clearInterval(cleanupInterval);
    ResourceTracker.forceCleanupAll();
  });
}

/**
 * RocksDBIterator implements a Readable stream interface for iterating over data in RocksDB.
 * This implementation has been hardened for production use with better state management.
 */
class RocksDBIterator extends Readable {
  constructor(db, opts = {}) {
    const {
      gt = null,
      gte = null,
      lt = null,
      lte = null,
      reverse = false,
      limit = Infinity,
      capacity = 8,
    } = opts;

    super();

    this._id = Math.random().toString(36).substring(2, 10);
    this._createdAt = Date.now();

    debug(`Creating iterator ${this._id} with opts:`, {
      gt,
      gte,
      lt,
      lte,
      reverse,
      limit,
    });

    // Register with cross-platform resource tracker
    ResourceTracker.register(this);

    db._ref();

    this._db = db;

    this._gt = gt ? this._encodeKey(gt) : empty;
    this._gte = gte ? this._encodeKey(gte) : empty;
    this._lt = lt ? this._encodeKey(lt) : empty;
    this._lte = lte ? this._encodeKey(lte) : empty;

    this._reverse = reverse;
    this._limit = limit < 0 ? Infinity : limit;
    this._capacity = Math.min(capacity, 20); // Limit batch size to avoid overloading
    this._opened = false;

    // Simplified state tracking
    this._pendingOperations = new Set();
    this._handle = null;
    this._endReached = false;
    this._currentIndex = 0;
    this._filteredEntries = null;
    this._entryCount = 0;
    this._destroyed = false;
    this._iterationComplete = false;
    this._isIterating = false;
    this._firstFetched = false;

    // Set up auto-cleanup with standard timeout
    this._cleanupTimer = setTimeout(() => {
      debug(`Auto-cleanup timer fired for iterator ${this._id}`);
      if (!this._destroyed) {
        this.destroy(
          new Error("Iterator auto-cleanup triggered after timeout")
        );
      }
    }, ITERATOR_TIMEOUT);

    // Only initialize if database is already open
    if (this._db._state.opened === true) {
      this._initialize().catch((err) => {
        debug(`Error in initialize() for iterator ${this._id}:`, err);
        this.destroy(err);
      });
    }

    // Ensure finalization happens in environments with GC
    if (typeof FinalizationRegistry !== "undefined") {
      const registry = new FinalizationRegistry((id) => {
        debug(`Finalizing iterator ${id} through garbage collection`);
        ResourceTracker.unregister(id);
      });

      registry.register(this, this._id);
    }
  }

  /**
   * Initialize the iterator
   */
  async _initialize() {
    debug(`Iterator ${this._id}: initialize()`);
    if (this._handle !== null || this._destroyed) return;

    try {
      // If database isn't open, wait for it
      if (this._db._state.opened === false) {
        debug(`Iterator ${this._id}: Waiting for database to open`);
        await this._db._state.ready();
      }

      if (this._destroyed) {
        debug(`Iterator ${this._id}: Destroyed during initialization`);
        return;
      }

      debug(`Iterator ${this._id}: Creating iterator handle`);
      this._handle = binding.iteratorInit(
        this._db._state._handle,
        this._db._columnFamily
      );
      debug(`Iterator ${this._id}: Handle created successfully`);

      // Pre-load and filter data for faster iteration
      await this._openAndPreloadData();
    } catch (err) {
      debug(`Iterator ${this._id}: Error in initialize():`, err);
      this.destroy(err);
      throw err;
    }
  }

  /**
   * Open iterator and preload data with range filtering
   */
  async _openAndPreloadData() {
    if (this._destroyed) {
      debug(`Iterator ${this._id}: Destroyed before opening`);
      return;
    }

    try {
      debug(`Iterator ${this._id}: Opening iterator`);

      // Mark I/O operation
      this._db._state.io.inc();
      this._pendingOperations.add("open");

      try {
        // Determine seek target based on range
        let seekTarget = Buffer.from("");

        if (this._gte.length > 0) {
          seekTarget = this._gte;
        } else if (this._gt.length > 0) {
          seekTarget = this._gt;
        }

        debug(
          `Iterator ${this._id}: Seeking to:`,
          seekTarget.toString(),
          `(reverse: ${
            this._reverse
          }, gt: ${this._gt.toString()}, gte: ${this._gte.toString()}, ` +
            `lt: ${this._lt.toString()}, lte: ${this._lte.toString()})`
        );

        // Seek to the appropriate position
        await binding.iteratorSeek(this._handle, seekTarget, this._reverse);
        this._opened = true;
      } finally {
        this._pendingOperations.delete("open");
        this._db._state.io.dec();
      }
    } catch (err) {
      debug(`Iterator ${this._id}: Error opening iterator:`, err);
      throw err;
    }
  }

  /**
   * Read implementation for Readable stream
   */
  async _read(cb) {
    debug(`Iterator ${this._id}: _read()`);

    // Safety check for destroyed iterators
    if (this._destroyed) {
      debug(`Iterator ${this._id}: Destroyed, can't read`);
      process.nextTick(() => cb(new Error("Iterator is destroyed")));
      return;
    }

    // Prevent concurrent reads
    if (this._isIterating) {
      debug(`Iterator ${this._id}: Already iterating, deferring read`);
      process.nextTick(() => cb(null));
      return;
    }

    // Check if database is suspended and closing/closed - do this early
    if (this._db._state._suspended === true) {
      const dbClosed =
        this._db._state.closed ||
        this._db._state.closing ||
        (this._db._state._handle && !this._db._state._handle.db);

      if (dbClosed) {
        debug(
          `Iterator ${this._id}: Database is closed/closing while suspended`
        );
        const error = new Error("Database was closed during suspension");
        this.destroy(error);
        process.nextTick(() => cb(error));
        return;
      }
    }

    this._isIterating = true;

    try {
      // If iteration is already complete, just return null
      if (this._iterationComplete) {
        this._isIterating = false;
        debug(`Iterator ${this._id}: Iteration already complete`);
        this.push(null);
        return cb(null);
      }

      // Initialize if needed
      if (!this._opened) {
        try {
          debug(`Iterator ${this._id}: Initializing for first read`);
          await this._initialize();

          // Check if destroyed during initialization
          if (this._destroyed) {
            debug(`Iterator ${this._id}: Destroyed during initialization`);
            this._isIterating = false;
            return cb(
              new Error("Iterator was destroyed during initialization")
            );
          }
        } catch (err) {
          this._isIterating = false;
          debug(`Iterator ${this._id}: Failed to initialize:`, err);
          return cb(err);
        }
      }

      // Return null if initialization failed
      if (!this._handle) {
        debug(`Iterator ${this._id}: No handle after initialization`);
        this._isIterating = false;
        this._iterationComplete = true;
        this.push(null);
        return cb(null);
      }

      try {
        // Get current entry from binding layer
        let entry = null;

        // If first entry we already have it from seek
        if (this._currentIndex === 0 && !this._firstFetched) {
          this._firstFetched = true;

          if (this._handle.keys && this._handle.keys.length > 0) {
            debug(`Iterator ${this._id}: Using first entry from seek`);
            entry = {
              key: this._handle.keys[0],
              value: this._handle.values[0],
            };
          } else {
            debug(
              `Iterator ${this._id}: No entries from initial seek, ending stream`
            );
            this._iterationComplete = true;
            this.push(null);
            this._isIterating = false;
            return cb(null);
          }
        } else {
          // Get next entry from binding
          debug(`Iterator ${this._id}: Fetching next entry`);
          entry = await binding.iteratorNext(this._handle);
        }

        // Process the entry if we have one
        if (entry && entry.key !== null) {
          debug(`Iterator ${this._id}: Processing entry with key:`, entry.key);

          // Apply range filters
          let skip = false;
          const keyStr =
            typeof entry.key === "string" ? entry.key : entry.key.toString();

          // Check boundaries
          if (this._gt.length > 0) {
            const gtStr = this._gt.toString();
            if (keyStr <= gtStr) {
              debug(
                `Iterator ${this._id}: Key ${keyStr} <= gt bound ${gtStr}, skipping`
              );
              skip = true;
            }
          } else if (this._gte.length > 0) {
            const gteStr = this._gte.toString();
            if (keyStr < gteStr) {
              debug(
                `Iterator ${this._id}: Key ${keyStr} < gte bound ${gteStr}, skipping`
              );
              skip = true;
            }
          }

          if (this._lt.length > 0) {
            const ltStr = this._lt.toString();
            if (keyStr >= ltStr) {
              debug(
                `Iterator ${this._id}: Key ${keyStr} >= lt bound ${ltStr}, skipping`
              );
              skip = true;
            }
          } else if (this._lte.length > 0) {
            const lteStr = this._lte.toString();
            if (keyStr > lteStr) {
              debug(
                `Iterator ${this._id}: Key ${keyStr} > lte bound ${lteStr}, skipping`
              );
              skip = true;
            }
          }

          // Only process if not skipped
          if (!skip) {
            try {
              // Important: Make sure entry.key and entry.value are properly converted to Buffer
              const keyBuffer = Buffer.isBuffer(entry.key)
                ? entry.key
                : Buffer.from(
                    typeof entry.key === "string"
                      ? entry.key
                      : String(entry.key)
                  );

              const valueBuffer = Buffer.isBuffer(entry.value)
                ? entry.value
                : Buffer.from(
                    typeof entry.value === "string"
                      ? entry.value
                      : String(entry.value)
                  );

              // Decode the entry with proper encoding
              const decodedKey = this._decodeKey(keyBuffer);
              const decodedValue = this._decodeValue(valueBuffer);

              // Push to stream
              this._entryCount++;
              debug(`Iterator ${this._id}: Pushing entry #${this._entryCount}`);

              this.push({
                key: decodedKey,
                value: decodedValue,
              });

              // Check limit
              if (this._limit < Infinity && this._entryCount >= this._limit) {
                debug(
                  `Iterator ${this._id}: Reached limit of ${this._limit}, ending stream`
                );
                this._iterationComplete = true;
                this.push(null);
              }
            } catch (decodeErr) {
              debug(
                `Iterator ${this._id}: Error decoding entry: ${decodeErr.message}`
              );
              // Continue with iteration - just skip this entry
            }
          } else {
            // If skipped due to range check, end iteration
            debug(
              `Iterator ${this._id}: Entry outside range bounds, ending stream`
            );
            this._iterationComplete = true;
            this.push(null);
          }
        } else {
          // No more entries
          debug(`Iterator ${this._id}: No more entries, ending stream`);
          this._iterationComplete = true;
          this.push(null);
        }

        this._isIterating = false;
        cb(null);
      } catch (err) {
        debug(`Iterator ${this._id}: Error during iteration: ${err.message}`);
        this._isIterating = false;
        cb(err);
      }
    } catch (err) {
      debug(`Iterator ${this._id}: Uncaught error in _read():`, err);
      this._isIterating = false;
      return cb(err);
    }
  }

  /**
   * Encode a key for storage
   */
  _encodeKey(k) {
    const encoding =
      this._db && this._db._keyEncoding ? this._db._keyEncoding : null;
    if (encoding) return c.encode(encoding, k);
    if (typeof k === "string") return Buffer.from(k);
    if (Buffer.isBuffer(k)) return k;
    return Buffer.from(String(k)); // Fallback
  }

  /**
   * Decode a key from storage
   */
  _decodeKey(b) {
    const encoding =
      this._db && this._db._keyEncoding ? this._db._keyEncoding : null;
    if (encoding && Buffer.isBuffer(b)) {
      try {
        return c.decode(encoding, b);
      } catch (e) {
        debug(
          `Iterator ${this._id}: Failed to decode key with provided encoding:`,
          e
        );
        // Fallback to buffer if decoding fails
        return b;
      }
    }
    return b; // Return buffer if no encoding
  }

  /**
   * Decode a value from storage
   */
  _decodeValue(b) {
    const encoding =
      this._db && this._db._valueEncoding ? this._db._valueEncoding : null;
    if (encoding && Buffer.isBuffer(b)) {
      try {
        return c.decode(encoding, b);
      } catch (e) {
        debug(
          `Iterator ${this._id}: Failed to decode value with provided encoding:`,
          e
        );
        // Fallback to buffer if decoding fails
        return b;
      }
    }
    return b; // Return buffer if no encoding
  }

  /**
   * Destroy implementation for cleanup
   */
  _destroy(err, cb) {
    debug(`Iterator ${this._id}: _destroy()`);

    // Create a safe callback function
    const safeCallback =
      typeof cb === "function"
        ? cb
        : (err) => {
            if (err)
              debug(
                `Iterator ${this._id}: Error in default destroy callback:`,
                err
              );
          };

    // Prevent multiple destroys
    if (this._destroyed) {
      debug(`Iterator ${this._id}: Already destroyed`);
      return safeCallback(null);
    }

    // Unregister from resource tracker
    ResourceTracker.unregister(this._id);

    this._destroyed = true;

    // Clear cleanup timer
    if (this._cleanupTimer) {
      clearTimeout(this._cleanupTimer);
      this._cleanupTimer = null;
    }

    // Check if DB was closed during suspension
    const dbClosedDuringSuspension =
      this._db._state._suspended &&
      (this._db._state.closed ||
        this._db._state.closing ||
        !this._db._state._handle.db);

    // If pending operations are in progress, handle appropriately
    if (this._pendingOperations.size > 0) {
      debug(
        `Iterator ${this._id}: Waiting for ${this._pendingOperations.size} operations to complete before destroying`
      );

      if (dbClosedDuringSuspension) {
        // If DB was closed during suspension, reject all pending operations
        debug(
          `Iterator ${this._id}: DB closed during suspension, rejecting all pending operations`
        );
        for (const op of this._pendingOperations) {
          if (typeof op.reject === "function") {
            op.reject(new Error("Database was closed during suspension"));
          }
        }
        this._pendingOperations.clear();
      } else {
        // Set a short timeout to allow pending operations to finish
        setTimeout(() => {
          // Force clear any remaining operations
          if (this._pendingOperations.size > 0) {
            debug(
              `Iterator ${this._id}: Force clearing ${this._pendingOperations.size} pending operations`
            );
            this._pendingOperations.clear();
          }
        }, 50);
      }
    }

    // Always ensure iteration completes
    if (!this._iterationComplete) {
      this._iterationComplete = true;
      try {
        debug(`Iterator ${this._id}: Ending stream during destroy`);
        this.push(null);
      } catch (e) {
        debug(`Iterator ${this._id}: Error ending stream during destroy:`, e);
        // Ignore errors while destroying
      }
    }

    // Clean up resources with the safe callback
    this._cleanupResources(safeCallback);
  }

  /**
   * Clean up resources during destroy
   */
  _cleanupResources(cb) {
    debug(`Iterator ${this._id}: Cleaning up resources`);

    // Ensure cb is always a function
    const safeCallback =
      typeof cb === "function"
        ? cb
        : (err) => {
            if (err)
              debug(
                `Iterator ${this._id}: Error in default resource cleanup callback:`,
                err
              );
          };

    try {
      // Release iterator handle
      if (this._handle) {
        debug(`Iterator ${this._id}: Releasing iterator handle`);

        try {
          // Only release if the database is not closed during suspension
          if (
            this._db._state._suspended &&
            (this._db._state.closed ||
              this._db._state.closing ||
              !this._db._state._handle.db)
          ) {
            debug(
              `Iterator ${this._id}: Database closed during suspension, skipping iterator release`
            );
            // Mark the handle as released directly
            this._handle.isReleased = true;
          } else {
            this._db._state.io.inc();
            binding.iteratorRelease(this._handle);
            this._db._state.io.dec();
          }
        } catch (err) {
          debug(`Iterator ${this._id}: Error releasing handle:`, err);
          // Continue with cleanup despite errors
        }

        this._handle = null;
      }

      // Clear references to release memory
      this._filteredEntries = null;
      this._pendingOperations.clear();

      // Release DB reference
      debug(`Iterator ${this._id}: Releasing DB reference`);
      this._db._unref();

      safeCallback(null);
    } catch (err) {
      debug(`Iterator ${this._id}: Error during cleanup:`, err);
      safeCallback(null); // Still return success to ensure cleanup completes
    }
  }

  get [Symbol.asyncIterator]() {
    const iterator = this;
    return () => {
      let reading = false;
      let destroyed = false;
      let error = null;
      let checkTimer = null;

      // Function to check if database is closed during suspension
      const checkDbClosedDuringSuspension = () => {
        if (!iterator._db || !iterator._db._state) return false;

        const state = iterator._db._state;
        if (state._suspended) {
          if (
            state.closed ||
            state.closing ||
            !state._handle ||
            !state._handle.db
          ) {
            debug(
              `Iterator ${iterator._id}: Database was closed during suspension (async check)`
            );
            error = new Error("Database was closed during suspension");
            iterator.destroy(error);
            return true;
          }
        }
        return false;
      };

      const drain = () => {
        if (reading || destroyed) return Promise.resolve({ done: true });

        // Check for closed database during suspension immediately
        if (checkDbClosedDuringSuspension()) {
          return Promise.reject(error);
        }

        reading = true;

        return new Promise((resolve, reject) => {
          // Set up immediate rejection if database is closed during suspension
          const checkInterval = setInterval(() => {
            if (checkDbClosedDuringSuspension()) {
              clearInterval(checkInterval);
              reading = false;
              reject(error);
            }
          }, 10);

          const cleanup = () => {
            clearInterval(checkInterval);
            iterator.removeListener("error", onError);
            iterator.removeListener("data", onData);
            iterator.removeListener("end", onEnd);
          };

          const onError = (err) => {
            cleanup();
            reading = false;
            error = err;
            reject(err);
          };

          const onData = (data) => {
            cleanup();
            reading = false;
            resolve({ value: data, done: false });
          };

          const onEnd = () => {
            cleanup();
            reading = false;
            resolve({ done: true });
          };

          iterator.once("error", onError);
          iterator.once("data", onData);
          iterator.once("end", onEnd);

          // One final check before resuming
          if (checkDbClosedDuringSuspension()) {
            cleanup();
            reading = false;
            reject(error);
            return;
          }

          iterator.resume();
        });
      };

      return {
        next() {
          if (error) return Promise.reject(error);
          if (destroyed) return Promise.resolve({ done: true });
          if (checkDbClosedDuringSuspension()) {
            return Promise.reject(error);
          }
          return drain();
        },
        return() {
          destroyed = true;
          return Promise.resolve({ done: true });
        },
      };
    };
  }
}

export default RocksDBIterator;
