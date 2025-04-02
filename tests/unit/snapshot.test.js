import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { indexedDB } from 'fake-indexeddb';
import { Snapshot } from '../../lib/snapshot.js';
import { State } from '../../lib/state.js';
import { Batch } from '../../lib/batch.js';
import '../setup.js'; // Import the setup file

// Check that indexedDB is available
if (!indexedDB) {
  console.error('IndexedDB is not available for testing');
}

// Helper to wait a short time for IndexedDB operations
const wait = (ms = 50) => new Promise(resolve => setTimeout(resolve, ms));

// Helper to convert Buffer to string for test assertions
const toString = value => {
  if (Buffer.isBuffer(value)) {
    return value.toString();
  }
  return value;
};

describe('Snapshot Interface with IndexedDB', () => {
  let state;
  let db;
  let dbCounter = 0;

  beforeEach(async () => {
    // Create a fresh database for each test
    const dbName = `test_snapshot_${dbCounter++}`;
    
    // Create a state and set up the database
    state = new State(null, dbName);
    
    // Create a mock DB session object that references the state
    db = {
      _state: state,
      _ref: () => {},
      _unref: () => {},
      _columnFamily: 'default'
    };
    
    // Initialize the database
    await state.ready();
    
    // Wait a bit for IndexedDB setup
    await wait(100);
  });

  afterEach(async () => {
    // Clean up after each test
    if (state) {
      try {
        if (state._db) {
          state._db.close();
        }
      } catch (err) {
        console.error('Error closing database:', err);
      }
      state = null;
      await wait(100); // Give IndexedDB time to close properly
    }
  });

  describe('constructor', () => {
    it('should create a snapshot with deferred initialization', () => {
      // Set up state to defer snapshot initialization
      db._state.deferSnapshotInit = true;
      
      const snapshot = new Snapshot(db);
      
      expect(snapshot).toBeDefined();
      expect(snapshot._initialized).toBe(false);
      expect(snapshot._handle).toBe(null);
      expect(snapshot._refCount).toBe(0);
      expect(snapshot._snapshotId).toBeDefined();
    });
    
    it('should initialize immediately if deferSnapshotInit is false', async () => {
      // Set up state to initialize snapshot immediately
      db._state.deferSnapshotInit = false;
      
      const snapshot = new Snapshot(db);
      
      // Wait for async initialization to complete
      await wait(150);
      
      expect(snapshot).toBeDefined();
      expect(snapshot._initialized).toBe(true);
      expect(snapshot._handle).not.toBe(null);
      expect(snapshot._snapshotId).toBeDefined();
    });
  });
  
  describe('reference counting', () => {
    it('should handle reference counting correctly', async () => {
      const snapshot = new Snapshot(db);
      
      expect(snapshot._refCount).toBe(0);
      
      snapshot._ref();
      expect(snapshot._refCount).toBe(1);
      
      snapshot._ref();
      expect(snapshot._refCount).toBe(2);
      
      snapshot._unref();
      expect(snapshot._refCount).toBe(1);
      
      snapshot._unref();
      expect(snapshot._refCount).toBe(0);
    });
    
    it('should cleanup snapshot resources when reference count reaches zero', async () => {
      const snapshot = new Snapshot(db);
      
      // Initialize the snapshot
      await snapshot._init();
      await wait(100);
      
      // Ensure it's set up correctly
      expect(snapshot._handle).not.toBe(null);
      expect(snapshot._initialized).toBe(true);
      
      // Add a reference and then remove it
      snapshot._ref();
      snapshot._unref();
      
      // Wait for cleanup
      await wait(100);
      
      // Snapshot should be cleaned up
      expect(snapshot._handle).toBe(null);
    });
  });
  
  describe('snapshot database operations', () => {
    it('should preserve original values when changes are made after snapshot creation', async () => {
      // First, add some data to the database
      const writeBatch = await state.createWriteBatch(db);
      await writeBatch.put('key1', 'initial-value');
      await writeBatch.put('key2', 'another-value');
      await writeBatch.flush();
      
      // Create a snapshot
      const snapshot = new Snapshot(db);
      await snapshot._init();
      await wait(100);
      
      // Make changes to the database after snapshot is created
      const anotherBatch = await state.createWriteBatch(db);
      await anotherBatch.put('key1', 'modified-value');
      await anotherBatch.delete('key2');
      await anotherBatch.put('key3', 'new-value');
      await anotherBatch.flush();
      
      // Wait for operations to complete
      await wait(150);
      
      // Verify the database has the new values
      const readBatch = await state.createReadBatch(db);
      const currentValue1 = await readBatch.get('key1');
      const currentValue2 = await readBatch.get('key2');
      const currentValue3 = await readBatch.get('key3');
      
      expect(toString(currentValue1)).toBe('modified-value');
      expect(currentValue2).toBe(null);
      expect(toString(currentValue3)).toBe('new-value');
      
      // Verify snapshot preserves the original values
      const snapshotValue1 = await snapshot.getValue('key1');
      const snapshotValue2 = await snapshot.getValue('key2');
      const snapshotValue3 = await snapshot.getValue('key3');
      
      expect(snapshotValue1).toBe('initial-value');
      expect(snapshotValue2).toBe('another-value');
      expect(snapshotValue3).toBe(null); // Key3 didn't exist when snapshot was created
    });
    
    it('should handle multiple snapshots correctly', async () => {
      // First, add some data to the database
      const writeBatch = await state.createWriteBatch(db);
      await writeBatch.put('key1', 'initial-value');
      await writeBatch.flush();
      
      // Create first snapshot
      const snapshot1 = new Snapshot(db);
      await snapshot1._init();
      await wait(100);
      
      // Make first change
      const batch1 = await state.createWriteBatch(db);
      await batch1.put('key1', 'first-update');
      await batch1.put('key2', 'added-after-snapshot1');
      await batch1.flush();
      await wait(100);
      
      // Create second snapshot
      const snapshot2 = new Snapshot(db);
      await snapshot2._init();
      await wait(100);
      
      // Make second change
      const batch2 = await state.createWriteBatch(db);
      await batch2.put('key1', 'second-update');
      await batch2.put('key2', 'updated-after-snapshot2');
      await batch2.put('key3', 'added-after-snapshot2');
      await batch2.flush();
      await wait(100);
      
      // Verify current database state
      const readBatch = await state.createReadBatch(db);
      expect(toString(await readBatch.get('key1'))).toBe('second-update');
      expect(toString(await readBatch.get('key2'))).toBe('updated-after-snapshot2');
      expect(toString(await readBatch.get('key3'))).toBe('added-after-snapshot2');
      
      // Verify snapshot1 state
      expect(await snapshot1.getValue('key1')).toBe('initial-value');
      expect(await snapshot1.getValue('key2')).toBe(null);
      expect(await snapshot1.getValue('key3')).toBe(null);
      
      // Verify snapshot2 state
      expect(await snapshot2.getValue('key1')).toBe('first-update');
      expect(await snapshot2.getValue('key2')).toBe('added-after-snapshot1');
      expect(await snapshot2.getValue('key3')).toBe(null);
    });
    
    it('should support hasValue method correctly', async () => {
      // Add data to the database
      const writeBatch = await state.createWriteBatch(db);
      await writeBatch.put('existing-key', 'value');
      await writeBatch.flush();
      
      // Create snapshot
      const snapshot = new Snapshot(db);
      await snapshot._init();
      await wait(100);
      
      // Check hasValue for existing and non-existing keys
      expect(await snapshot.hasValue('existing-key')).toBe(true);
      expect(await snapshot.hasValue('non-existing-key')).toBe(false);
      
      // Now delete the key after snapshot creation
      const deleteBatch = await state.createWriteBatch(db);
      await deleteBatch.delete('existing-key');
      await deleteBatch.flush();
      await wait(100);
      
      // Key should still exist in snapshot but not in current DB
      expect(await snapshot.hasValue('existing-key')).toBe(true);
      
      const readBatch = await state.createReadBatch(db);
      expect(await readBatch.get('existing-key')).toBe(null);
    });
  });
  
  describe('snapshot integration', () => {
    it('should properly clean up resources when snapshots are destroyed', async () => {
      // Create and initialize a snapshot
      const snapshot = new Snapshot(db);
      await snapshot._init();
      await wait(100);
      
      // Verify it's in the state's snapshots
      expect(state._snapshots.has(snapshot)).toBe(true);
      
      // Close the snapshot
      await snapshot.close();
      await wait(100);
      
      // Verify it's removed from state
      expect(state._snapshots.has(snapshot)).toBe(false);
    });
    
    it('should preserve snapshot data for complex operations', async () => {
      // Add some initial data
      const writeBatch = await state.createWriteBatch(db);
      
      // Add 10 keys
      for (let i = 1; i <= 10; i++) {
        await writeBatch.put(`key${i}`, `value${i}`);
      }
      
      await writeBatch.flush();
      await wait(100);
      
      // Create snapshot
      const snapshot = new Snapshot(db);
      await snapshot._init();
      await wait(100);
      
      // Now update some values, delete some, and add new ones
      const updateBatch = await state.createWriteBatch(db);
      
      // Update even numbered keys
      for (let i = 2; i <= 10; i += 2) {
        await updateBatch.put(`key${i}`, `updated${i}`);
      }
      
      // Delete keys 7, 9
      await updateBatch.delete('key7');
      await updateBatch.delete('key9');
      
      // Add new keys 11-15
      for (let i = 11; i <= 15; i++) {
        await updateBatch.put(`key${i}`, `value${i}`);
      }
      
      await updateBatch.flush();
      await wait(100);
      
      // Verify current database state
      const readBatch = await state.createReadBatch(db);
      
      for (let i = 1; i <= 15; i++) {
        if (i === 7 || i === 9) {
          // These were deleted
          expect(await readBatch.get(`key${i}`)).toBe(null);
        } else if (i % 2 === 0 && i <= 10) {
          // These were updated
          expect(toString(await readBatch.get(`key${i}`))).toBe(`updated${i}`);
        } else {
          // These are either original or newly added
          expect(toString(await readBatch.get(`key${i}`))).toBe(`value${i}`);
        }
      }
      
      // Verify snapshot state preserves original values
      for (let i = 1; i <= 15; i++) {
        if (i <= 10) {
          // Original keys should have original values
          expect(toString(await snapshot.getValue(`key${i}`))).toBe(`value${i}`);
        } else {
          // New keys should not exist in snapshot
          expect(await snapshot.getValue(`key${i}`)).toBe(null);
        }
      }
    });
  });
  
  describe('RocksDB feature parity', () => {
    it('should implement the same core methods as RocksDB snapshots', () => {
      const snapshot = new Snapshot(db);
      
      expect(typeof snapshot._ref).toBe('function');
      expect(typeof snapshot._unref).toBe('function');
      expect(typeof snapshot._init).toBe('function');
      expect(typeof snapshot.getValue).toBe('function');
      expect(typeof snapshot.hasValue).toBe('function');
    });
  });
}); 