import { describe, it, expect, beforeEach } from 'bun:test';
import { indexedDB } from 'fake-indexeddb';
import { ColumnFamily } from '../../lib/column-family.js';
import { BloomFilterPolicy } from '../../lib/filter-policy.js';
import '../setup.js'; // Import the setup file

// Check that indexedDB is available
if (!indexedDB) {
  console.error('IndexedDB is not available for testing');
}

describe('ColumnFamily Interface', () => {
  // Test constructor and options
  describe('constructor', () => {
    it('should create a column family with default options', () => {
      const name = 'test_cf';
      const cf = new ColumnFamily(name);
      
      expect(cf.name).toBe(name);
      expect(cf._handle).toEqual({ name });
      
      // Check default options match RocksDB behavior
      expect(cf.options.enableBlobFiles).toBe(false);
      expect(cf.options.minBlobSize).toBe(0);
      expect(cf.options.blobFileSize).toBe(0);
      expect(cf.options.enableBlobGarbageCollection).toBe(true);
      expect(cf.options.tableBlockSize).toBe(8192);
      expect(cf.options.tableCacheIndexAndFilterBlocks).toBe(true);
      expect(cf.options.tableFormatVersion).toBe(6);
      expect(cf.options.optimizeFiltersForMemory).toBe(false);
      expect(cf.options.blockCache).toBe(true);
    });
    
    it('should create a column family with custom options', () => {
      const name = 'test_cf_custom';
      const filterPolicy = new BloomFilterPolicy(12);
      const cf = new ColumnFamily(name, {
        enableBlobFiles: true,
        minBlobSize: 1024,
        tableBlockSize: 16384,
        filterPolicy
      });
      
      expect(cf.name).toBe(name);
      expect(cf._handle).toEqual({ name });
      
      // Check custom options were set
      expect(cf.options.enableBlobFiles).toBe(true);
      expect(cf.options.minBlobSize).toBe(1024);
      expect(cf.options.tableBlockSize).toBe(16384);
      expect(cf.options.filterPolicy).toBe(filterPolicy);
      
      // Check defaults for options not specified
      expect(cf.options.blobFileSize).toBe(0);
      expect(cf.options.enableBlobGarbageCollection).toBe(true);
    });
    
    it('should create a column family with IndexedDB-specific options', () => {
      const name = 'test_cf_idb';
      const cf = new ColumnFamily(name, {
        autoIncrement: true,
        keyPath: 'id'
      });
      
      expect(cf.name).toBe(name);
      expect(cf.options.autoIncrement).toBe(true);
      expect(cf.options.keyPath).toBe('id');
    });
  });
  
  // Test cloneSettings method
  describe('cloneSettings', () => {
    it('should clone settings to a new column family with a different name', () => {
      const originalName = 'original_cf';
      const newName = 'cloned_cf';
      const filterPolicy = new BloomFilterPolicy(16);
      
      const originalCf = new ColumnFamily(originalName, {
        enableBlobFiles: true,
        minBlobSize: 2048,
        filterPolicy
      });
      
      const clonedCf = originalCf.cloneSettings(newName);
      
      // Check new name is set but options are the same
      expect(clonedCf.name).toBe(newName);
      expect(clonedCf.options).toEqual(originalCf.options);
      
      // Ensure it's a new instance
      expect(clonedCf).not.toBe(originalCf);
    });
  });
  
  // Test name getter/setter
  describe('name property', () => {
    it('should get column family name', () => {
      const name = 'test_name_getter';
      const cf = new ColumnFamily(name);
      
      expect(cf.name).toBe(name);
    });
    
    it('should set column family name and update handle', () => {
      const originalName = 'original_name';
      const newName = 'new_name';
      
      const cf = new ColumnFamily(originalName);
      cf.name = newName;
      
      expect(cf.name).toBe(newName);
      expect(cf._handle.name).toBe(newName);
    });
  });
  
  // Test exists method (IndexedDB-specific but important for compatibility)
  describe('exists', () => {
    it('should return false if database is not provided', () => {
      const cf = new ColumnFamily('test_exists');
      expect(cf.exists(null)).toBe(false);
    });
    
    it('should check if object store exists in database', () => {
      const cf = new ColumnFamily('test_exists');
      
      // Mock database with object store names
      const mockDb = {
        objectStoreNames: ['test_exists', 'other_store']
      };
      
      expect(cf.exists(mockDb)).toBe(true);
      
      // Test with non-existent store
      const cf2 = new ColumnFamily('nonexistent_store');
      expect(cf2.exists(mockDb)).toBe(false);
    });
  });
  
  // Test ensureExists method
  describe('ensureExists', () => {
    it('should return false if database is not provided', () => {
      const cf = new ColumnFamily('test_ensure');
      expect(cf.ensureExists(null)).toBe(false);
    });
    
    it('should return true if store exists', () => {
      const cf = new ColumnFamily('test_ensure');
      
      // Mock database with object store names
      const mockDb = {
        objectStoreNames: ['test_ensure', 'other_store']
      };
      
      expect(cf.ensureExists(mockDb)).toBe(true);
    });
    
    it('should return false if store does not exist', () => {
      const cf = new ColumnFamily('nonexistent_store');
      
      // Mock database with object store names
      const mockDb = {
        objectStoreNames: ['test_ensure', 'other_store']
      };
      
      expect(cf.ensureExists(mockDb)).toBe(false);
    });
  });
  
  // Test createObjectStore method
  describe('createObjectStore', () => {
    it('should throw error if database is not provided', () => {
      const cf = new ColumnFamily('test_create');
      expect(() => cf.createObjectStore(null)).toThrow('Database connection not provided');
    });
    
    it('should create object store with correct options', () => {
      const name = 'test_create_store';
      const cf = new ColumnFamily(name, {
        keyPath: 'id',
        autoIncrement: true
      });
      
      // Mock database with implementation to test
      let createdStore = null;
      const mockDb = {
        objectStoreNames: [],
        exists: () => false,
        createObjectStore: (storeName, options) => {
          createdStore = { name: storeName, options };
          return createdStore;
        },
        deleteObjectStore: () => {}
      };
      
      // Add store existence check
      mockDb.objectStoreNames.includes = (name) => false;
      
      const store = cf.createObjectStore(mockDb);
      
      expect(store).toBe(createdStore);
      expect(createdStore.name).toBe(name);
      expect(createdStore.options.keyPath).toBe('id');
      expect(createdStore.options.autoIncrement).toBe(true);
    });
  });
  
  // Test getTransaction and getObjectStore methods
  describe('transaction methods', () => {
    it('should create a transaction for the column family', () => {
      const name = 'test_transaction';
      const cf = new ColumnFamily(name);
      
      // Mock database with transaction implementation
      let createdTx = null;
      const mockDb = {
        objectStoreNames: [name],
        transaction: (storeNames, mode) => {
          createdTx = { stores: storeNames, mode };
          return createdTx;
        }
      };
      
      // Add store existence check
      mockDb.objectStoreNames.includes = (name) => true;
      
      const tx = cf.getTransaction(mockDb, 'readwrite');
      
      expect(tx).toBe(createdTx);
      expect(tx.stores).toBe(name);
      expect(tx.mode).toBe('readwrite');
    });
    
    it('should get object store from transaction', () => {
      const name = 'test_object_store';
      const cf = new ColumnFamily(name);
      
      // Mock transaction with object store implementation
      let mockStore = { name };
      const mockTx = {
        objectStore: (storeName) => {
          expect(storeName).toBe(name);
          return mockStore;
        }
      };
      
      const store = cf.getObjectStore(mockTx);
      
      expect(store).toBe(mockStore);
    });
    
    it('should get object store from database', () => {
      const name = 'test_object_store_db';
      const cf = new ColumnFamily(name);
      
      // Mock store and transaction
      let mockStore = { name };
      let mockTx = {
        objectStore: (storeName) => {
          expect(storeName).toBe(name);
          return mockStore;
        }
      };
      
      // Mock database
      const mockDb = {
        objectStoreNames: [name],
        transaction: (storeNames, mode) => {
          expect(storeNames).toBe(name);
          expect(mode).toBe('readonly');
          return mockTx;
        }
      };
      
      // Add store existence check
      mockDb.objectStoreNames.includes = (name) => true;
      
      const store = cf.getObjectStore(mockDb);
      
      expect(store).toBe(mockStore);
    });
  });
  
  // Test withTransaction method
  describe('withTransaction', () => {
    it('should execute callback with transaction and object store', async () => {
      const name = 'test_with_tx';
      const cf = new ColumnFamily(name);
      
      // Mock store, transaction and database
      let mockStore = { name };
      let mockTx = {
        objectStore: (storeName) => {
          expect(storeName).toBe(name);
          return mockStore;
        },
        oncomplete: null,
        onerror: null,
        onabort: null
      };
      
      const mockDb = {
        objectStoreNames: [name],
        transaction: (storeNames, mode) => {
          expect(storeNames).toBe(name);
          expect(mode).toBe('readwrite');
          return mockTx;
        }
      };
      
      // Add store existence check
      mockDb.objectStoreNames.includes = (name) => true;
      
      // Create a promise to test the transaction promise
      const txPromise = cf.withTransaction(mockDb, (store, tx) => {
        expect(store).toBe(mockStore);
        expect(tx).toBe(mockTx);
        return 'test-result';
      }, 'readwrite');
      
      // Simulate transaction completion
      mockTx.oncomplete();
      
      const result = await txPromise;
      expect(result).toBe('test-result');
    });
    
    it('should reject when transaction fails', async () => {
      const name = 'test_with_tx_error';
      const cf = new ColumnFamily(name);
      
      // Mock store, transaction and database
      let mockStore = { name };
      let mockTx = {
        objectStore: (storeName) => mockStore,
        oncomplete: null,
        onerror: null,
        onabort: null
      };
      
      const mockDb = {
        objectStoreNames: [name],
        transaction: () => mockTx
      };
      
      // Add store existence check
      mockDb.objectStoreNames.includes = (name) => true;
      
      // Create a promise to test the transaction promise
      const txPromise = cf.withTransaction(mockDb, () => {});
      
      // Simulate transaction error
      mockTx.onerror({ target: { error: new Error('Transaction error') } });
      
      await expect(txPromise).rejects.toThrow('Transaction error');
    });
  });
}); 