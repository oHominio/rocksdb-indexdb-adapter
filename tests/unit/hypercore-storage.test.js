import { describe, it, expect } from 'bun:test';
import IndexDBStorage from '../../index.js';

// Tests for hypercore-storage integration with rocksdb-indexdb-adapter
describe('Hypercore Storage with RocksDB IndexedDB Adapter', () => {
  
  it('should support STORAGE_TYPE and version constants in IndexDBStorage', () => {
    // Check that the adapter exposes the STORAGE_TYPE constant
    expect(IndexDBStorage.STORAGE_TYPE).toBe('indexeddb');
    
    // Verify any version constants match hypercore-storage expectations
    // The VERSION constant is used by hypercore-storage to check compatibility
    expect(typeof IndexDBStorage.VERSION).toBe('number');
  });
}); 