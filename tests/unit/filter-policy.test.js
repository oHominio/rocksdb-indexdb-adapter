import { describe, it, expect } from 'bun:test';
import { indexedDB } from 'fake-indexeddb';
import { BloomFilterPolicy, RibbonFilterPolicy, createFilterPolicy } from '../../lib/filter-policy.js';
import '../setup.js'; // Import the setup file

// Check that indexedDB is available
if (!indexedDB) {
  console.error('IndexedDB is not available for testing');
}

describe('FilterPolicy Interface', () => {
  // Test BloomFilterPolicy
  describe('BloomFilterPolicy', () => {
    it('should create a bloom filter policy with correct properties', () => {
      const bitsPerKey = 16;
      const policy = new BloomFilterPolicy(bitsPerKey);
      
      // Check that it matches the RocksDB interface
      expect(policy.type).toBe(1);
      expect(policy.bitsPerKey).toBe(bitsPerKey);
    });
    
    it('should use default bitsPerKey when not provided', () => {
      const policy = new BloomFilterPolicy();
      expect(policy.bitsPerKey).toBe(10); // Default is 10
      expect(policy.type).toBe(1);
    });
    
    it('should implement stub methods for createFilter', () => {
      const policy = new BloomFilterPolicy(10);
      const keys = ['key1', 'key2', 'key3'];
      
      const filter = policy.createFilter(keys);
      expect(filter).toBeInstanceOf(Uint8Array);
      // Should create a filter with appropriate size based on bitsPerKey and number of keys
      expect(filter.length).toBe(Math.ceil(keys.length * 10 / 8));
    });
    
    it('should implement stub methods for keyMayMatch', () => {
      const policy = new BloomFilterPolicy(10);
      const filter = new Uint8Array(5);
      
      // In our IndexedDB implementation, this always returns true
      const result = policy.keyMayMatch('testKey', filter);
      expect(result).toBe(true);
    });
  });
  
  // Test RibbonFilterPolicy
  describe('RibbonFilterPolicy', () => {
    it('should create a ribbon filter policy with correct properties', () => {
      const bloomEquivalentBitsPerKey = 8;
      const bloomBeforeLevel = 2;
      const policy = new RibbonFilterPolicy(bloomEquivalentBitsPerKey, bloomBeforeLevel);
      
      // Check that it matches the RocksDB interface
      expect(policy.type).toBe(2);
      expect(policy.bloomEquivalentBitsPerKey).toBe(bloomEquivalentBitsPerKey);
      expect(policy.bloomBeforeLevel).toBe(bloomBeforeLevel);
    });
    
    it('should use default values when not provided', () => {
      const policy = new RibbonFilterPolicy(8);
      expect(policy.bloomEquivalentBitsPerKey).toBe(8);
      expect(policy.bloomBeforeLevel).toBe(0); // Default is 0
      expect(policy.type).toBe(2);
      
      const defaultPolicy = new RibbonFilterPolicy();
      expect(defaultPolicy.bloomEquivalentBitsPerKey).toBe(10); // Default is 10
    });
    
    it('should implement stub methods for createFilter', () => {
      const policy = new RibbonFilterPolicy(8);
      const keys = ['key1', 'key2', 'key3', 'key4'];
      
      const filter = policy.createFilter(keys);
      expect(filter).toBeInstanceOf(Uint8Array);
      // Should create a filter with appropriate size based on bloomEquivalentBitsPerKey and number of keys
      expect(filter.length).toBe(Math.ceil(keys.length * 8 / 8));
    });
    
    it('should implement stub methods for keyMayMatch', () => {
      const policy = new RibbonFilterPolicy(8);
      const filter = new Uint8Array(5);
      
      // In our IndexedDB implementation, this always returns true
      const result = policy.keyMayMatch('testKey', filter);
      expect(result).toBe(true);
    });
  });
  
  // Test createFilterPolicy factory function
  describe('createFilterPolicy', () => {
    it('should create a bloom filter policy with specified bits per key', () => {
      const bitsPerKey = 12;
      const policy = createFilterPolicy(bitsPerKey);
      
      expect(policy).toBeInstanceOf(BloomFilterPolicy);
      expect(policy.type).toBe(1);
      expect(policy.bitsPerKey).toBe(bitsPerKey);
    });
    
    it('should create a bloom filter policy with default bits per key', () => {
      const policy = createFilterPolicy();
      
      expect(policy).toBeInstanceOf(BloomFilterPolicy);
      expect(policy.type).toBe(1);
      expect(policy.bitsPerKey).toBe(10); // Default is 10
    });
  });
}); 