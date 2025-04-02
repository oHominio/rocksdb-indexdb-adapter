import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { indexedDB } from 'fake-indexeddb';
import { State } from '../../lib/state.js';
import '../setup.js'; // Import the setup file

// Check that indexedDB is available
if (!indexedDB) {
  console.error('IndexedDB is not available for testing');
}

// Helper to wait a short time for IndexedDB operations
const wait = (ms = 10) => new Promise(resolve => setTimeout(resolve, ms));

describe('State Interface with IndexedDB', () => {
  let state;
  let dbCounter = 0;

  beforeEach(async () => {
    // Create a fresh database for each test
    const dbName = `test_state_${dbCounter++}`;
    const mockDb = { close: () => {} };
    state = new State(mockDb, dbName);
    // Give IndexedDB a moment to complete setup
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
    it('should create a state instance with default options', () => {
      const mockDb = { close: () => {} };
      const path = 'test_db';
      const state = new State(mockDb, path);
      
      expect(state.db).toBe(mockDb);
      expect(state.path).toBe(path);
      expect(state.opened).toBe(false);
      expect(state._suspended).toBe(false);
      expect(state.MAX_BATCH_REUSE).toBe(64);
      expect(state.deferSnapshotInit).toBe(true);
      expect(state._columnFamilies.has('default')).toBe(true);
    });
    
    it('should initialize with column families from options', () => {
      const mockDb = { close: () => {} };
      const path = 'test_db';
      const options = {
        columnFamilies: ['cf1', 'cf2', { name: 'cf3' }]
      };
      
      const state = new State(mockDb, path, options);
      
      expect(state._columnFamilies.has('cf1')).toBe(true);
      expect(state._columnFamilies.has('cf2')).toBe(true);
      expect(state._columnFamilies.has('cf3')).toBe(true);
    });
  });
  
  describe('column family methods', () => {
    it('should get default column family when no name is provided', () => {
      const cf = state.getColumnFamily();
      expect(cf.name).toBe('default');
    });
    
    it('should get column family by name', () => {
      state._columnFamilies.set('test_cf', { name: 'test_cf' });
      const cf = state.getColumnFamily('test_cf');
      expect(cf.name).toBe('test_cf');
    });
    
    it('should create a new column family if it does not exist', () => {
      const cf = state.getColumnFamily('new_cf');
      expect(cf.name).toBe('new_cf');
      expect(state._columnFamilies.has('new_cf')).toBe(true);
    });
    
    it('should create a new column family even if database is flushed', () => {
      state._columnsFlushed = true;
      const cf = state.getColumnFamily('non_existent');
      expect(cf.name).toBe('non_existent');
      expect(state._columnFamilies.has('non_existent')).toBe(true);
    });
  });
  
  describe('batch management', () => {
    it('should create a read batch', async () => {
      const mockDb = { _ref: () => {} };
      const batch = await state.createReadBatch(mockDb);
      
      expect(batch.write).toBe(false);
      expect(batch.db).toBe(mockDb);
    });
    
    it('should create a write batch', async () => {
      const mockDb = { _ref: () => {} };
      const batch = await state.createWriteBatch(mockDb);
      
      expect(batch.write).toBe(true);
      expect(batch.db).toBe(mockDb);
    });
    
    it('should reuse batches', async () => {
      const mockDb = { _ref: () => {} };
      const batch1 = await state.createReadBatch(mockDb);
      
      // Store the batch for reuse
      state.freeBatch(batch1, false);
      
      // Get a new batch
      const batch2 = await state.createReadBatch(mockDb);
      
      // Should be the same object
      expect(batch2).toBe(batch1);
    });
  });
  
  describe('handles', () => {
    it('should track handles correctly', () => {
      expect(state.handles._count).toBe(0);
      
      state.handles.inc();
      expect(state.handles._count).toBe(1);
      
      state.handles.dec();
      expect(state.handles._count).toBe(0);
    });
    
    it('should resolve idle promise when count reaches zero', async () => {
      state.handles.inc();
      
      // Set up a promise that resolves when idle
      const idlePromise = state.handles.idle();
      
      // Decrement the counter
      state.handles.dec();
      
      // Promise should resolve
      await idlePromise;
      
      expect(state.handles.isIdle()).toBe(true);
    });
  });
  
  describe('event listeners', () => {
    it('should add and remove event listeners', () => {
      const listener = () => {};
      
      state.on('test', listener);
      expect(state._eventListeners.get('test')).toContain(listener);
      
      state.off('test', listener);
      expect(state._eventListeners.get('test')).not.toContain(listener);
    });
    
    it('should emit events to listeners', () => {
      let called = false;
      const listener = () => { called = true; };
      
      state.on('test', listener);
      state.emit('test');
      
      expect(called).toBe(true);
    });
  });
  
  describe('session management', () => {
    it('should add and remove sessions', () => {
      const session1 = { _snapshot: null };
      const session2 = { _snapshot: null };
      
      state.addSession(session1);
      expect(session1._index).toBe(0);
      expect(state.sessions.length).toBe(1);
      
      state.addSession(session2);
      expect(session2._index).toBe(1);
      expect(state.sessions.length).toBe(2);
      
      state.removeSession(session1);
      expect(session1._index).toBe(-1);
      expect(state.sessions.length).toBe(1);
      expect(state.sessions[0]).toBe(session2);
      expect(session2._index).toBe(0);
    });
    
    it('should handle sessions with snapshots', () => {
      const session = {
        _snapshot: {
          _ref: () => { session._snapshot.refCount++; },
          _unref: () => { session._snapshot.refCount--; },
          refCount: 0
        }
      };
      
      state.addSession(session);
      expect(session._snapshot.refCount).toBe(1);
      
      state.removeSession(session);
      expect(session._snapshot.refCount).toBe(0);
    });
  });
  
  describe('database operations', () => {
    it('should implement suspend and resume methods', async () => {
      // Just test that the methods exist and can be called
      await state.suspend();
      expect(state._suspended).toBe(true);
      
      await state.resume();
      expect(state._suspended).toBe(false);
    });
    
    it('should handle flush operation', async () => {
      const mockDb = { _columnFamily: { _handle: { name: 'default' } } };
      
      // Mock the ready method
      state.ready = async () => {
        state.opened = true;
      };
      
      // Should not throw
      await state.flush(mockDb);
    });
  });
  
  describe('feature parity with RocksDB', () => {
    it('should have the same core methods as RocksDB State', () => {
      // Check key methods that should match RocksDB's State implementation
      expect(typeof state.createReadBatch).toBe('function');
      expect(typeof state.createWriteBatch).toBe('function');
      expect(typeof state.freeBatch).toBe('function');
      expect(typeof state.addSession).toBe('function');
      expect(typeof state.removeSession).toBe('function');
      expect(typeof state.getColumnFamily).toBe('function');
      expect(typeof state.upsertColumnFamily).toBe('function');
      expect(typeof state.getColumnFamilyByName).toBe('function');
      expect(typeof state.ready).toBe('function');
      expect(typeof state._open).toBe('function');
      expect(typeof state._close).toBe('function');
      expect(typeof state.flush).toBe('function');
      expect(typeof state.suspend).toBe('function');
      expect(typeof state.resume).toBe('function');
    });
  });
}); 