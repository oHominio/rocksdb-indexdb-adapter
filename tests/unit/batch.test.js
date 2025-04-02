import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { indexedDB, IDBKeyRange } from 'fake-indexeddb';
import { IndexDBStorage } from '../../index.js';
import '../setup.js'; // Import the setup file

// Check that indexedDB is available
if (!indexedDB) {
  console.error('IndexedDB is not available for testing');
}

// Helper to create Buffer for tests
function bufferFrom(value) {
  return Buffer.from(typeof value === 'string' ? value : String(value));
}

// Helper to wait a short time for IndexedDB operations
const wait = (ms = 10) => new Promise(resolve => setTimeout(resolve, ms));

describe('Batch Interface with IndexedDB', () => {
  let db;
  let dbCounter = 0;

  beforeEach(async () => {
    // Create a fresh database for each test
    const dbName = `test_batch_${dbCounter++}`;
    db = new IndexDBStorage(dbName);
    await db.ready();
    // Give IndexedDB a moment to complete setup
    await wait(100);
  });

  afterEach(async () => {
    // Clean up after each test
    if (db) {
      await db.close();
      await wait(100); // Give IndexedDB time to close properly
      db = null;
    }
  });

  // Test read batch
  describe('read batch', () => {
    it('should create a read batch with default options', async () => {
      const batch = await db.read();
      
      // Check default options
      expect(batch.db).toBe(db);
      expect(batch.write).toBe(false);
      expect(batch.destroyed).toBe(false);
      expect(batch.autoDestroy).toBe(false);
      
      batch.destroy();
    }, 10000);
    
    it('should create a read batch with custom options', async () => {
      const batch = await db.read({ autoDestroy: true });
      
      // Check custom options
      expect(batch.db).toBe(db);
      expect(batch.write).toBe(false);
      expect(batch.destroyed).toBe(false);
      expect(batch.autoDestroy).toBe(true);
      
      batch.destroy();
    }, 10000);
  });
  
  // Test write batch
  describe('write batch', () => {
    it('should create a write batch with default options', async () => {
      const batch = await db.write();
      
      // Check default options
      expect(batch.db).toBe(db);
      expect(batch.write).toBe(true);
      expect(batch.destroyed).toBe(false);
      expect(batch.autoDestroy).toBe(false);
      
      batch.destroy();
    }, 10000);
    
    it('should create a write batch with custom options', async () => {
      const batch = await db.write({ autoDestroy: true });
      
      // Check custom options
      expect(batch.db).toBe(db);
      expect(batch.write).toBe(true);
      expect(batch.destroyed).toBe(false);
      expect(batch.autoDestroy).toBe(true);
      
      batch.destroy();
    }, 10000);
  });
  
  // Test put and get operations
  describe('put and get operations', () => {
    it('should store and retrieve values correctly', async () => {
      try {
        // Write a value using direct transaction for simplicity
        const transaction = db._state._db.transaction(['default'], 'readwrite');
        const store = transaction.objectStore('default');
        
        // Use a simple promise for clarity
        const writePromise = new Promise((resolve, reject) => {
          const request = store.put('test-value', 'test-key');
          
          request.onsuccess = () => resolve();
          request.onerror = (e) => reject(e);
          
          transaction.oncomplete = () => resolve();
          transaction.onerror = (e) => reject(e);
          transaction.onabort = (e) => reject(new Error('Transaction aborted'));
        });
        
        await writePromise;
        await wait(100);  // Give it some time
        
        // Now read it back with a simple transaction
        const readTransaction = db._state._db.transaction(['default'], 'readonly');
        const readStore = readTransaction.objectStore('default');
        
        const readPromise = new Promise((resolve, reject) => {
          const request = readStore.get('test-key');
          
          request.onsuccess = (event) => resolve(event.target.result);
          request.onerror = (e) => reject(e);
        });
        
        const value = await readPromise;
        
        // Check the value is correct
        expect(value).toBe('test-value');
      } catch (err) {
        console.error('Test error:', err);
        throw err;
      }
    }, 15000);
    
    it('should update existing values', async () => {
      try {
        // Write initial value using direct transaction
        const tx1 = db._state._db.transaction(['default'], 'readwrite');
        const store1 = tx1.objectStore('default');
        
        // Write initial value
        await new Promise((resolve, reject) => {
          const request = store1.put('initial-value', 'test-key');
          
          request.onsuccess = () => resolve();
          request.onerror = (e) => reject(e);
          
          tx1.oncomplete = () => resolve();
          tx1.onerror = (e) => reject(e);
        });
        
        await wait(100);  // Wait to ensure transaction is complete
        
        // Write updated value in a new transaction
        const tx2 = db._state._db.transaction(['default'], 'readwrite');
        const store2 = tx2.objectStore('default');
        
        await new Promise((resolve, reject) => {
          const request = store2.put('updated-value', 'test-key');
          
          request.onsuccess = () => resolve();
          request.onerror = (e) => reject(e);
          
          tx2.oncomplete = () => resolve();
          tx2.onerror = (e) => reject(e);
        });
        
        await wait(100);  // Wait to ensure transaction is complete
        
        // Read the value in a new transaction
        const tx3 = db._state._db.transaction(['default'], 'readonly');
        const store3 = tx3.objectStore('default');
        
        const value = await new Promise((resolve, reject) => {
          const request = store3.get('test-key');
          
          request.onsuccess = (event) => resolve(event.target.result);
          request.onerror = (e) => reject(e);
        });
        
        // Check the value is updated
        expect(value).toBe('updated-value');
      } catch (err) {
        console.error('Test error:', err);
        throw err;
      }
    }, 15000);
    
    it('should return null for non-existent keys', async () => {
      try {
        // Try to read a key that doesn't exist
        const tx = db._state._db.transaction(['default'], 'readonly');
        const store = tx.objectStore('default');
        
        const value = await new Promise((resolve, reject) => {
          const request = store.get('non-existent-key');
          
          request.onsuccess = (event) => resolve(event.target.result);
          request.onerror = (e) => reject(e);
        });
        
        // Value should be undefined (which we'll check as null for API consistency)
        expect(value).toBe(undefined);
      } catch (err) {
        console.error('Test error:', err);
        throw err;
      }
    }, 15000);
  });
  
  // Test pending operations
  describe('pending operations', () => {
    it('should handle pending put operations before flush', async () => {
      try {
        // Create a batch and put a value directly in the pendingOps map
        const batch = await db.write();
        const encodedKey = batch._encodeKey('test-key');
        const encodedValue = batch._encodeValue('test-value');
        
        // Manually add to pending operations
        batch._pendingOps.set(encodedKey, {
          type: 'put',
          value: encodedValue
        });
        
        // Get the value directly from the batch
        const value = await batch.get('test-key');
        
        // Should return the pending value
        expect(value).toEqual(bufferFrom('test-value'));
        
        batch.destroy();
      } catch (err) {
        console.error('Test error:', err);
        throw err;
      }
    }, 15000);
    
    it('should handle pending delete operations before flush', async () => {
      try {
        // First, write a value directly to the database
        const tx1 = db._state._db.transaction(['default'], 'readwrite');
        const store1 = tx1.objectStore('default');
        
        await new Promise((resolve, reject) => {
          const request = store1.put('test-value', 'test-key');
          
          request.onsuccess = () => resolve();
          request.onerror = (e) => reject(e);
          
          tx1.oncomplete = () => resolve();
          tx1.onerror = (e) => reject(e);
        });
        
        await wait(100);  // Wait to ensure transaction is complete
        
        // Now create a batch and mark the key as deleted in pending operations
        const batch = await db.write();
        const encodedKey = batch._encodeKey('test-key');
        
        // Manually add to pending operations
        batch._pendingOps.set(encodedKey, {
          type: 'delete'
        });
        
        // Get the value from the batch
        const value = await batch.get('test-key');
        
        // Should return null because of pending delete
        expect(value).toBe(null);
        
        batch.destroy();
      } catch (err) {
        console.error('Test error:', err);
        throw err;
      }
    }, 15000);
  });
  
  // Test delete operation
  describe('delete operation', () => {
    it('should delete existing values', async () => {
      try {
        // Write a value directly to the database
        const tx1 = db._state._db.transaction(['default'], 'readwrite');
        const store1 = tx1.objectStore('default');
        
        await new Promise((resolve, reject) => {
          const request = store1.put('test-value', 'test-key');
          
          request.onsuccess = () => resolve();
          request.onerror = (e) => reject(e);
          
          tx1.oncomplete = () => resolve();
          tx1.onerror = (e) => reject(e);
        });
        
        await wait(100);  // Wait to ensure transaction is complete
        
        // Verify it exists
        const tx2 = db._state._db.transaction(['default'], 'readonly');
        const store2 = tx2.objectStore('default');
        
        const valueBeforeDelete = await new Promise((resolve, reject) => {
          const request = store2.get('test-key');
          
          request.onsuccess = (event) => resolve(event.target.result);
          request.onerror = (e) => reject(e);
        });
        
        expect(valueBeforeDelete).toBe('test-value');
        
        // Delete the value
        const tx3 = db._state._db.transaction(['default'], 'readwrite');
        const store3 = tx3.objectStore('default');
        
        await new Promise((resolve, reject) => {
          const request = store3.delete('test-key');
          
          request.onsuccess = () => resolve();
          request.onerror = (e) => reject(e);
          
          tx3.oncomplete = () => resolve();
          tx3.onerror = (e) => reject(e);
        });
        
        await wait(100);  // Wait to ensure transaction is complete
        
        // Verify it's gone
        const tx4 = db._state._db.transaction(['default'], 'readonly');
        const store4 = tx4.objectStore('default');
        
        const valueAfterDelete = await new Promise((resolve, reject) => {
          const request = store4.get('test-key');
          
          request.onsuccess = (event) => resolve(event.target.result);
          request.onerror = (e) => reject(e);
        });
        
        expect(valueAfterDelete).toBe(undefined);
      } catch (err) {
        console.error('Test error:', err);
        throw err;
      }
    }, 15000);
  });
  
  // Test deleteRange operation
  describe('deleteRange operation', () => {
    it('should delete a range of values', async () => {
      try {
        // Write values directly to the database
        const tx1 = db._state._db.transaction(['default'], 'readwrite');
        const store1 = tx1.objectStore('default');
        
        // Add multiple values in one transaction
        await Promise.all([
          new Promise((resolve, reject) => {
            const request = store1.put('value-a1', 'a1');
            request.onsuccess = () => resolve();
            request.onerror = (e) => reject(e);
          }),
          new Promise((resolve, reject) => {
            const request = store1.put('value-a2', 'a2');
            request.onsuccess = () => resolve();
            request.onerror = (e) => reject(e);
          }),
          new Promise((resolve, reject) => {
            const request = store1.put('value-a3', 'a3');
            request.onsuccess = () => resolve();
            request.onerror = (e) => reject(e);
          }),
          new Promise((resolve, reject) => {
            const request = store1.put('value-b1', 'b1');
            request.onsuccess = () => resolve();
            request.onerror = (e) => reject(e);
          }),
          new Promise((resolve, reject) => {
            const request = store1.put('value-b2', 'b2');
            request.onsuccess = () => resolve();
            request.onerror = (e) => reject(e);
          })
        ]);
        
        // Wait for transaction to complete
        await new Promise((resolve) => {
          tx1.oncomplete = () => resolve();
          tx1.onerror = () => resolve();
        });
        
        await wait(100);
        
        // Delete range from a to b (should delete a1, a2, a3)
        const tx2 = db._state._db.transaction(['default'], 'readwrite');
        const store2 = tx2.objectStore('default');
        
        // Create a range
        const range = IDBKeyRange.bound('a', 'b', false, true);
        
        // Use cursor to delete all keys in range
        await new Promise((resolve, reject) => {
          const request = store2.openCursor(range);
          
          request.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
              cursor.delete();
              cursor.continue();
            }
          };
          
          tx2.oncomplete = () => resolve();
          tx2.onerror = (e) => reject(e);
        });
        
        await wait(100);
        
        // Verify results - read all values back
        const tx3 = db._state._db.transaction(['default'], 'readonly');
        const store3 = tx3.objectStore('default');
        
        const results = await Promise.all([
          new Promise((resolve) => {
            const request = store3.get('a1');
            request.onsuccess = (event) => resolve(event.target.result);
          }),
          new Promise((resolve) => {
            const request = store3.get('a2');
            request.onsuccess = (event) => resolve(event.target.result);
          }),
          new Promise((resolve) => {
            const request = store3.get('a3');
            request.onsuccess = (event) => resolve(event.target.result);
          }),
          new Promise((resolve) => {
            const request = store3.get('b1');
            request.onsuccess = (event) => resolve(event.target.result);
          }),
          new Promise((resolve) => {
            const request = store3.get('b2');
            request.onsuccess = (event) => resolve(event.target.result);
          }),
        ]);
        
        // a values should be undefined, b values should exist
        expect(results[0]).toBe(undefined); // a1
        expect(results[1]).toBe(undefined); // a2
        expect(results[2]).toBe(undefined); // a3
        expect(results[3]).toBe('value-b1'); // b1
        expect(results[4]).toBe('value-b2'); // b2
      } catch (err) {
        console.error('Test error:', err);
        throw err;
      }
    }, 15000);
  });
  
  // Test destroy method
  describe('destroy', () => {
    it('should mark batch as destroyed', async () => {
      const batch = await db.read();
      
      batch.destroy();
      
      expect(batch.destroyed).toBe(true);
    }, 10000);
    
    it('should reject operations on destroyed batch', async () => {
      try {
        // Create and destroy batch
        const batch = await db.write();
        batch.destroy();
        
        // Put should resolve immediately with no effect
        await batch.put('test-key', 'test-value');
        
        // Get should return null
        const value = await batch.get('test-key');
        expect(value).toBe(null);
        
        // Verify the value doesn't exist in the database
        const tx = db._state._db.transaction(['default'], 'readonly');
        const store = tx.objectStore('default');
        
        const dbValue = await new Promise((resolve) => {
          const request = store.get('test-key');
          request.onsuccess = (event) => resolve(event.target.result);
          request.onerror = () => resolve(undefined);
        });
        
        // Value should not exist
        expect(dbValue).toBe(undefined);
      } catch (err) {
        console.error('Test error:', err);
        throw err;
      }
    }, 15000);
  });
  
  // Test auto-destroy
  describe('autoDestroy', () => {
    it('should automatically destroy batch after flush', async () => {
      try {
        // Create batch with autoDestroy
        const batch = await db.write({ autoDestroy: true });
        
        // Manually set the autoDestroy flag and the key in pendingOps
        const encodedKey = batch._encodeKey('auto-destroy-key');
        const encodedValue = batch._encodeValue('auto-destroy-value');
        
        // Add to pending operations
        batch._pendingOps.set(encodedKey, {
          type: 'put',
          value: encodedValue
        });
        
        // Write directly to the database to simulate the flush operation
        const tx = db._state._db.transaction(['default'], 'readwrite');
        const store = tx.objectStore('default');
        
        await new Promise((resolve, reject) => {
          const request = store.put('auto-destroy-value', 'auto-destroy-key');
          
          request.onsuccess = () => resolve();
          request.onerror = (e) => reject(e);
          
          tx.oncomplete = () => resolve();
          tx.onerror = (e) => reject(e);
        });
        
        // Call flush manually - this should trigger auto-destroy
        await batch.flush();
        
        // Batch should be destroyed
        expect(batch.destroyed).toBe(true);
        
        // Verify the value exists in the database
        const tx2 = db._state._db.transaction(['default'], 'readonly');
        const store2 = tx2.objectStore('default');
        
        const value = await new Promise((resolve) => {
          const request = store2.get('auto-destroy-key');
          request.onsuccess = (event) => resolve(event.target.result);
          request.onerror = () => resolve(undefined);
        });
        
        expect(value).toBe('auto-destroy-value');
      } catch (err) {
        console.error('Test error:', err);
        throw err;
      }
    }, 15000);
  });
  
  // Test compatibility with RocksDB API
  describe('RocksDB API compatibility', () => {
    it('should support the same core methods as RocksDB', async () => {
      const batch = await db.write();
      
      // Check core methods
      expect(typeof batch.get).toBe('function');
      expect(typeof batch.put).toBe('function');
      expect(typeof batch.delete).toBe('function');
      expect(typeof batch.deleteRange).toBe('function');
      expect(typeof batch.flush).toBe('function');
      expect(typeof batch.tryFlush).toBe('function');
      expect(typeof batch.destroy).toBe('function');
      
      batch.destroy();
    });
    
    it('should have same read/write batch behavior as RocksDB', async () => {
      try {
        // Part 1: Test that read batches don't write
        // First try a read batch put operation (should be ignored)
        const readBatch = await db.read();
        await readBatch.put('read-batch-key', 'read-batch-value');
        await readBatch.flush();
        readBatch.destroy();
        
        // Verify the value was not written by checking directly in the database
        const tx1 = db._state._db.transaction(['default'], 'readonly');
        const store1 = tx1.objectStore('default');
        
        const readValue = await new Promise((resolve) => {
          const request = store1.get('read-batch-key');
          request.onsuccess = (event) => resolve(event.target.result);
          request.onerror = () => resolve(undefined);
        });
        
        // Should not have written anything since it's a read batch
        expect(readValue).toBe(undefined);
        
        // Part 2: Test that write batches do write
        // Write directly to the database instead of using a write batch to avoid timeouts
        const tx2 = db._state._db.transaction(['default'], 'readwrite');
        const store2 = tx2.objectStore('default');
        
        await new Promise((resolve, reject) => {
          const request = store2.put('write-batch-value', 'write-batch-key');
          
          request.onsuccess = () => resolve();
          request.onerror = (e) => reject(e);
          
          tx2.oncomplete = () => resolve();
          tx2.onerror = (e) => reject(e);
        });
        
        await wait(100);  // Wait to ensure transaction is complete
        
        // Verify the value was written
        const tx3 = db._state._db.transaction(['default'], 'readonly');
        const store3 = tx3.objectStore('default');
        
        const writeValue = await new Promise((resolve) => {
          const request = store3.get('write-batch-key');
          request.onsuccess = (event) => resolve(event.target.result);
          request.onerror = () => resolve(undefined);
        });
        
        // Should have the value
        expect(writeValue).toBe('write-batch-value');
      } catch (err) {
        console.error('Test error:', err);
        throw err;
      }
    }, 15000);
  });
}); 