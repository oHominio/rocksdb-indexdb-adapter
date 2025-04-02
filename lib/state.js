/**
 * State class for managing database state
 */
export class State {
  constructor(db, path, options = {}) {
    const {
      maxBatchReuse = 64, // Match RocksDB's MAX_BATCH_REUSE
      columnFamilies = [],
      readOnly = false,
      createIfMissing = true,
      createMissingColumnFamilies = true
    } = options;

    this.db = db;
    this.path = path;
    this.opened = false;
    this.opening = false;
    this.closed = false;
    this.closing = false;
    this._suspended = false;
    this._suspendCallback = null;
    this._suspending = null;
    this._resuming = null;
    this._db = null;
    this.handles = new Handles();
    this.io = new Handles(); // Add IO counter like RocksDB
    this.sessions = [];
    this._readBatches = [];
    this._writeBatches = [];
    this.MAX_BATCH_REUSE = maxBatchReuse;
    this.deferSnapshotInit = true; // Add for RocksDB compatibility
    this.resumed = null; // Add for compatibility with resume handling

    // Event listeners
    this._eventListeners = new Map();
    
    // IndexedDB specific
    this._indexedDB = globalThis.indexedDB;
    this._IDBKeyRange = globalThis.IDBKeyRange;
    
    // Make sure we have IndexedDB available
    if (!this._indexedDB) {
      throw new Error('IndexedDB not available');
    }
    
    // Initialize column families more like RocksDB
    this._dbVersion = 1;
    this._columnFamilies = new Map();
    
    // Default column family
    const defaultCF = { name: 'default', _handle: { name: 'default' } };
    this._columnFamilies.set('default', defaultCF);

    // Add column families from options
    if (columnFamilies && Array.isArray(columnFamilies)) {
      for (const cf of columnFamilies) {
        const cfName = typeof cf === 'string' ? cf : cf.name;
        if (!this._columnFamilies.has(cfName)) {
          const columnFamily = typeof cf === 'string' 
            ? { name: cf, _handle: { name: cf } }
            : cf;
          this._columnFamilies.set(cfName, columnFamily);
        }
      }
    }

    // Track batch objects for reuse
    this._readBatches = [];
    this._writeBatches = [];
    this._columnsFlushed = false; // Add for compatibility
  }

  /**
   * Add event listener
   * @param {string} event - Event name
   * @param {Function} listener - Event listener
   * @returns {State} This instance for chaining
   */
  on(event, listener) {
    if (!this._eventListeners.has(event)) {
      this._eventListeners.set(event, []);
    }
    this._eventListeners.get(event).push(listener);
    return this;
  }

  /**
   * Remove event listener
   * @param {string} event - Event name
   * @param {Function} listener - Event listener
   * @returns {State} This instance for chaining
   */
  off(event, listener) {
    if (!this._eventListeners.has(event)) return this;
    
    const listeners = this._eventListeners.get(event);
    const index = listeners.indexOf(listener);
    
    if (index !== -1) {
      listeners.splice(index, 1);
    }
    
    return this;
  }

  /**
   * Emit an event
   * @param {string} event - Event name
   * @param {...*} args - Event arguments
   */
  emit(event, ...args) {
    if (!this._eventListeners.has(event)) return;
    
    const listeners = this._eventListeners.get(event);
    for (const listener of listeners) {
      try {
        listener(...args);
      } catch (err) {
        console.error(`Error in event listener for ${event}:`, err);
      }
    }
  }

  /**
   * Add a session to track
   * @param {*} session - Session to track
   */
  addSession(session) {
    session._index = this.sessions.length;
    this.sessions.push(session);
    
    // Initialize snapshot if present
    if (session._snapshot) {
      session._snapshot._ref();
    }
  }

  /**
   * Remove a session from tracking
   * @param {*} session - Session to remove
   */
  removeSession(session) {
    const idx = session._index;
    if (idx === -1) return;

    session._index = -1;
    if (idx === this.sessions.length - 1) {
      this.sessions.pop();
    } else {
      const last = this.sessions.pop();
      last._index = idx;
      this.sessions[idx] = last;
    }
    
    // Clean up snapshot if present
    if (session._snapshot) {
      // Ensure the snapshot is fully unreferenced
      while (session._snapshot.refCount > 0) {
        session._snapshot._unref();
      }
    }
  }

  /**
   * Create a read batch
   * @param {object} db - Database session
   * @param {object} options - Batch options
   * @returns {Promise<Batch>} Read batch
   */
  async createReadBatch(db, options) {
    // Reuse existing batch if available
    if (this._readBatches.length > 0) {
      const batch = this._readBatches.pop();
      batch._reuse(db, { ...options, write: false });
      return batch;
    }
    
    // Import dynamically to avoid circular dependencies
    const { Batch } = await import('./batch.js');
    return new Batch(db, { ...options, write: false });
  }

  /**
   * Create a write batch
   * @param {object} db - Database session
   * @param {object} options - Batch options
   * @returns {Promise<Batch>} Write batch
   */
  async createWriteBatch(db, options) {
    // Reuse existing batch if available
    if (this._writeBatches.length > 0) {
      const batch = this._writeBatches.pop();
      batch._reuse(db, { ...options, write: true });
      return batch;
    }
    
    // Import dynamically to avoid circular dependencies
    const { Batch } = await import('./batch.js');
    return new Batch(db, { ...options, write: true });
  }

  /**
   * Free a batch for reuse
   * @param {Batch} batch - Batch to free
   * @param {boolean} writable - Whether batch is writable
   */
  freeBatch(batch, writable) {
    const queue = writable ? this._writeBatches : this._readBatches;
    if (queue.length >= this.MAX_BATCH_REUSE) return;
    queue.push(batch);
  }

  /**
   * Get or create a column family
   * @param {string|object} name - Column family name or object
   * @returns {object} Column family object
   */
  getColumnFamily(name) {
    if (!name) return this._columnFamilies.get('default');
    
    if (typeof name === 'string') {
      if (this._columnsFlushed) {
        const col = this.getColumnFamilyByName(name);
        if (col === null) throw new Error('Unknown column family');
        return col;
      }
      
      return this.upsertColumnFamily(name);
    }
    
    return name;
  }

  /**
   * Get column family by name
   * @param {string} name - Column family name
   * @returns {object|null} Column family object or null if not found
   */
  getColumnFamilyByName(name) {
    if (this._columnFamilies.has(name)) {
      return this._columnFamilies.get(name);
    }
    return null;
  }

  /**
   * Upsert column family
   * @param {string|object} c - Column family name or object
   * @returns {object} Column family object
   */
  upsertColumnFamily(c) {
    if (typeof c === 'string') {
      let col = this.getColumnFamilyByName(c);
      if (col) return col;
      
      col = { name: c, _handle: { name: c } };
      this._columnFamilies.set(c, col);
      return col;
    }

    if (this._columnFamilies.has(c.name)) return c;
    
    this._columnFamilies.set(c.name, c);
    return c;
  }

  /**
   * Initialize and open the database
   * @returns {Promise<void>} Promise that resolves when database is ready
   */
  async ready() {
    if (this.opened) return;
    if (this.opening) return this.opening;
    
    this.opening = this._open();
    await this.opening;
    this.opening = null;
    this.opened = true;
    
    this.deferSnapshotInit = false;
    
    // Initialize snapshots if any sessions have them
    for (const session of this.sessions) {
      if (session._snapshot) session._snapshot._init();
    }
  }

  /**
   * Open the IndexedDB database
   * @private
   * @returns {Promise<void>} Promise that resolves when database is open
   */
  async _open() {
    await Promise.resolve(); // Allow column families to populate if on-demand
    
    this._columnsFlushed = true;
    
    if (!this._indexedDB) {
      throw new Error('IndexedDB not available');
    }
    
    // Construct database name from path
    const dbName = this.path.replace(/[^a-zA-Z0-9]/g, '_');
    
    try {
      // Open the database
      const db = await new Promise((resolve, reject) => {
        const request = this._indexedDB.open(dbName, this._dbVersion);
        
        request.onerror = (event) => {
          const error = event.target.error || new Error('Unknown database error');
          console.error(`Failed to open database: ${error.message}`);
          reject(error);
        };
        
        request.onblocked = (event) => {
          console.warn(`Database '${dbName}' blocked, trying to close previous connections`);
        };
        
        request.onsuccess = (event) => {
          resolve(event.target.result);
        };
        
        request.onupgradeneeded = (event) => {
          const db = event.target.result;
          
          // Create object stores for all column families
          for (const [cfName, cf] of this._columnFamilies.entries()) {
            if (!db.objectStoreNames.contains(cfName)) {
              try {
                console.log(`Creating object store '${cfName}' in database '${dbName}'`);
                db.createObjectStore(cfName);
              } catch (err) {
                console.warn(`Error creating object store ${cfName}:`, err.message);
              }
            }
          }
        };
      });
      
      // Store the database connection
      this._db = db;
      
      // Listen for database close event
      db.onversionchange = () => {
        console.log(`Database '${dbName}' version changed, closing connection`);
        db.close();
        this._db = null;
      };
      
      console.log(`Successfully set up database '${dbName}'`);
      return db;
    } catch (err) {
      console.error(`Error opening database '${dbName}':`, err);
      throw err;
    }
  }

  /**
   * Close the database
   * @returns {Promise<void>} Promise that resolves when database is closed
   */
  async _close() {
    if (this.resumed) this.resumed.resolve(false);
    
    // Wait for all operations to complete
    while (!this.io.isIdle()) await this.io.idle();
    while (!this.handles.isIdle()) await this.handles.idle();
    
    // Close all sessions
    for (let i = this.sessions.length - 1; i >= 0; i--) {
      await this.sessions[i].close();
    }
    
    // Close the database connection
    if (this._db) {
      this._db.close();
      this._db = null;
    }
  }

  /**
   * Flush any pending database operations
   * @param {object} db - Database session
   * @param {object} opts - Flush options
   * @returns {Promise<void>} Promise that resolves when flush completes
   */
  async flush(db, opts) {
    if (this.opened === false) await this.ready();
    
    this.io.inc();
    
    if (this.resumed !== null) {
      const resumed = await this.resumed.promise;
      
      if (!resumed) {
        this.io.dec();
        throw new Error('IndexedDB session is closed');
      }
    }
    
    try {
      // IndexedDB transactions are automatically committed,
      // so no additional flush is needed
      return Promise.resolve();
    } finally {
      this.io.dec();
    }
  }

  /**
   * Suspend database operations
   * @returns {Promise<void>} Promise that resolves when database operations are suspended
   */
  async suspend() {
    if (this._suspending === null) this._suspending = this._suspend();
    return this._suspending;
  }

  /**
   * Internal suspend implementation
   * @private
   * @returns {Promise<void>} Promise that resolves when suspended
   */
  async _suspend() {
    if (this.opened === false) await this.ready();
    
    this.io.inc();
    
    if (this._resuming !== null) await this._resuming;
    
    this.io.dec();
    
    if (this._suspended === true) return;
    
    while (!this.io.isIdle()) await this.io.idle();
    
    this.io.inc();
    this.resumed = defer();
    
    try {
      this._suspended = true;
    } finally {
      this.io.dec();
      this._suspending = null;
    }
  }

  /**
   * Resume database operations
   * @returns {Promise<void>} Promise that resolves when database operations are resumed
   */
  resume() {
    if (this._resuming === null) this._resuming = this._resume();
    return this._resuming;
  }

  /**
   * Internal resume implementation
   * @private
   * @returns {Promise<void>} Promise that resolves when resumed
   */
  async _resume() {
    if (this.opened === false) await this.ready();
    
    this.io.inc();
    
    if (this._suspending !== null) await this._suspending;
    
    if (this._suspended === false) {
      this.io.dec();
      return;
    }
    
    try {
      this._suspended = false;
    } finally {
      this.io.dec();
      this._resuming = null;
    }
    
    const resumed = this.resumed;
    this.resumed = null;
    resumed.resolve(true);
  }
}

/**
 * Class for tracking open handles
 */
class Handles {
  constructor() {
    this._count = 0;
    this._idle = null;
    this._eventListeners = new Map();
  }

  inc() {
    this._count++;
  }

  dec() {
    if (--this._count === 0 && this._idle) {
      const idle = this._idle;
      this._idle = null;
      idle.resolve();
      this.emit('idle');
    }
  }

  isIdle() {
    return this._count === 0;
  }

  idle() {
    if (this.isIdle()) return Promise.resolve();
    if (!this._idle) this._idle = defer();
    return this._idle.promise;
  }

  /**
   * Add event listener
   * @param {string} event - Event name
   * @param {Function} listener - Event listener
   * @returns {Handles} This instance for chaining
   */
  on(event, listener) {
    if (!this._eventListeners.has(event)) {
      this._eventListeners.set(event, []);
    }
    this._eventListeners.get(event).push(listener);
    return this;
  }

  /**
   * Remove event listener
   * @param {string} event - Event name
   * @param {Function} listener - Event listener
   * @returns {Handles} This instance for chaining
   */
  off(event, listener) {
    if (!this._eventListeners.has(event)) return this;
    
    const listeners = this._eventListeners.get(event);
    const index = listeners.indexOf(listener);
    
    if (index !== -1) {
      listeners.splice(index, 1);
    }
    
    return this;
  }

  /**
   * Emit an event
   * @param {string} event - Event name
   * @param {...*} args - Event arguments
   */
  emit(event, ...args) {
    if (!this._eventListeners.has(event)) return;
    
    const listeners = this._eventListeners.get(event);
    for (const listener of listeners) {
      try {
        listener(...args);
      } catch (err) {
        console.error(`Error in event listener for ${event}:`, err);
      }
    }
  }
}

/**
 * Creates a deferred promise
 */
function defer() {
  const stack = {};
  Error.captureStackTrace(stack, defer);

  const o = {};
  o.stack = stack.stack;
  o.promise = new Promise((resolve, reject) => {
    o.resolve = resolve;
    o.reject = reject;
  });

  return o;
}

export default State;
