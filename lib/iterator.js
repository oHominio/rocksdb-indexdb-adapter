import binding from "./binding.js";
import * as c from "compact-encoding";

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

// NOTE: ResourceTracker is removed for simplicity in this refactor,
// assuming db.close() handles iterator cleanup via binding.close()

/**
 * RocksDBIterator implements a cross-platform compatible iterator for IndexedDB
 * that works in both browser and Node.js environments.
 */
class RocksDBIterator {
  constructor(db, opts = {}) {
    this._id = Math.random().toString(36).substring(2, 10);
    debug(`Creating iterator ${this._id} with opts:`, opts);

    this._db = db;
    this._options = opts || {};
    this._handle = null;
    this._destroyed = false;
    this._pendingNext = null;
    this._ended = false;
    this._initialized = false;
    this._limit = opts.limit || Infinity;
    this._count = 0;
    this._entries = [];
    this._currentIndex = 0;
    this._allDataLoaded = false;
    this._waitingForResume = false;

    // Reference counting
    db._ref();

    // Pass encodings from session if not provided in opts
    this._options.keyEncoding = this._options.keyEncoding || db._keyEncoding;
    this._options.valueEncoding =
      this._options.valueEncoding || db._valueEncoding;

    // Initialize the iterator immediately, but don't block constructor
    this._initPromise = this._initialize();
  }

  /**
   * Initialize the iterator handle with binding layer and preload all data
   * to avoid async issues with brittle test framework
   */
  async _initialize() {
    if (this._handle || this._destroyed) return;

    try {
      // Wait for DB to open if needed
      if (this._db._state.opened === false) {
        await this._db._state.ready();
      }

      // Check if destroyed during waiting
      if (this._destroyed) return;

      // Create the iterator handle
      this._handle = binding.iteratorInit(
        this._db._state._handle,
        this._db._columnFamily,
        this._options
      );

      // Seek to initialize position
      await binding.iteratorSeek(
        this._handle,
        this._options.gte || this._options.gt || ""
      );

      // Check if DB is suspended before preloading
      if (this._db._state._handle.suspended) {
        this._waitingForResume = true;
        debug(`Iterator ${this._id}: Database suspended, waiting for resume`);

        // Register a resume listener
        this._resumePromise = new Promise((resolve) => {
          // Store the original resume method
          const originalResume = this._db._state.resume;

          // Override the resume method to also resolve our promise
          this._db._state.resume = async function (...args) {
            const result = await originalResume.apply(this, args);
            resolve();
            // Restore original method after first call
            this.resume = originalResume;
            return result;
          };
        });
      } else {
        // Preload data immediately if not suspended
        await this._preloadAllData();
      }

      this._initialized = true;
      debug(`Iterator ${this._id}: Initialized`);
    } catch (err) {
      debug(`Iterator ${this._id}: Initialization failed:`, err);
      this.destroy(err);
    }
  }

  /**
   * Preload all data for this iterator to avoid async issues with test framework
   */
  async _preloadAllData() {
    try {
      const entries = [];
      let lastKey = null;

      // Continue fetching until we reach the end or limit
      while (entries.length < this._limit) {
        const entry = await binding.iteratorNext(this._handle);

        // If no more entries, break
        if (!entry || entry.key === null) {
          break;
        }

        // Store the entry
        entries.push({
          key: entry.key,
          value: entry.value,
        });

        // Update last key
        lastKey = entry.key;
      }

      // Store the entries
      this._entries = entries;
      this._allDataLoaded = true;
      this._waitingForResume = false;

      debug(`Iterator ${this._id}: Preloaded ${entries.length} entries`);
    } catch (err) {
      debug(`Iterator ${this._id}: Error preloading data:`, err);
      throw err;
    }
  }

  /**
   * Get the next item from the iterator
   */
  async next() {
    // Return done if already destroyed or ended
    if (this._destroyed || this._ended) {
      debug(`Iterator ${this._id}: Already destroyed or ended`);
      return { done: true };
    }

    try {
      // Wait for initialization if needed
      if (!this._initialized) {
        await this._initPromise;
      }

      // If waiting for resume, wait for it
      if (this._waitingForResume) {
        debug(`Iterator ${this._id}: Waiting for database to resume`);
        await this._resumePromise;

        // Load data after resume completes
        if (!this._allDataLoaded) {
          await this._preloadAllData();
        }
      }

      // Check again if destroyed or ended during initialization or resume
      if (this._destroyed || this._ended) {
        debug(
          `Iterator ${this._id}: Destroyed or ended during initialization or resume`
        );
        return { done: true };
      }

      // If we've reached the end of our preloaded data
      if (this._currentIndex >= this._entries.length) {
        this._ended = true;
        debug(
          `Iterator ${this._id}: Reached end of entries (${this._entries.length})`
        );
        return { done: true };
      }

      // Get the entry at current index and advance
      const entry = this._entries[this._currentIndex++];
      debug(
        `Iterator ${this._id}: Returning entry ${this._currentIndex - 1} of ${
          this._entries.length
        }`
      );

      // Return the entry
      return {
        done: false,
        value: {
          key: entry.key,
          value: entry.value,
        },
      };
    } catch (err) {
      debug(`Iterator ${this._id}: Error during next():`, err);
      this.destroy(err);
      throw err;
    }
  }

  /**
   * Release iterator resources
   */
  destroy(err) {
    if (this._destroyed) return;

    debug(
      `Iterator ${this._id}: Destroying`,
      err ? `with error: ${err.message}` : ""
    );
    this._destroyed = true;
    this._ended = true;

    // Release the binding handle
    if (this._handle) {
      try {
        binding.iteratorRelease(this._handle);
        debug(`Iterator ${this._id}: Released handle`);
      } catch (releaseErr) {
        debug(`Iterator ${this._id}: Error releasing handle:`, releaseErr);
      }
      this._handle = null;
    }

    // Unref the DB
    if (this._db) {
      this._db._unref();
      this._db = null;
    }

    // Clear entries to free memory
    this._entries = [];
  }

  /**
   * Required method to make it an async iterator
   */
  [Symbol.asyncIterator]() {
    return {
      // Use arrow functions to keep 'this' context
      next: async () => {
        try {
          return await this.next();
        } catch (err) {
          // If an error occurs, we end the iteration and rethrow
          this._ended = true;
          throw err;
        }
      },
      // Always destroy the iterator when the async iteration is cancelled
      return: async () => {
        debug(`Iterator ${this._id}: AsyncIterator.return() called`);
        this.destroy();
        return { done: true };
      },
    };
  }
}

export default RocksDBIterator;
