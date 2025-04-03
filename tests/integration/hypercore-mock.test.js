import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { IndexDBStorage } from '../../index.js'
import { indexedDB, IDBKeyRange } from 'fake-indexeddb'

// Set up fake IndexedDB in the global scope
global.indexedDB = indexedDB
global.IDBKeyRange = IDBKeyRange

// Mock for basic storage behavior similar to how Hypercore would use it
class MockHypercore {
  constructor(storage) {
    this.storage = storage
    this.length = 0
    this.key = Buffer.from('mock-hypercore-key')
    this._ready = false
  }

  async ready() {
    if (this._ready) return
    // Make sure the storage is open
    if (this.storage.opened !== true) {
      await this.storage.open()
    }
    this._ready = true
    return this
  }

  async append(data) {
    if (!this._ready) await this.ready()
    
    if (Array.isArray(data)) {
      // Handle array of blocks
      for (const block of data) {
        await this._appendSingle(block)
      }
    } else {
      // Handle single block
      await this._appendSingle(data)
    }
    
    return this.length
  }

  async _appendSingle(data) {
    const index = this.length++
    const key = `block/${index}`
    await this.storage.put(key, data)
    return index
  }

  async get(index) {
    if (!this._ready) await this.ready()
    
    if (index >= this.length) {
      throw new Error(`Index out of bounds: ${index}, length: ${this.length}`)
    }
    
    const key = `block/${index}`
    return this.storage.get(key)
  }

  async close() {
    // Nothing special to do for the mock
    return true
  }
}

// Mock for Corestore that manages multiple MockHypercores
class MockCorestore {
  constructor(storage) {
    this.storage = storage
    this.cores = new Map()
    this._ready = false
  }

  async ready() {
    if (this._ready) return
    
    // Make sure the storage is open
    if (this.storage.opened !== true) {
      await this.storage.open()
    }
    
    this._ready = true
    return this
  }

  get(options = {}) {
    const name = options.name || 'default'
    
    // Return existing core if we have it
    if (this.cores.has(name)) {
      return this.cores.get(name)
    }
    
    // Create a namespaced storage for this core
    const namespacedStorage = new NamespacedStorage(this.storage, name)
    
    // Create a new core with this storage
    const core = new MockHypercore(namespacedStorage)
    this.cores.set(name, core)
    
    return core
  }

  async close() {
    // Close all cores
    for (const core of this.cores.values()) {
      await core.close()
    }
    
    // Clear the map
    this.cores.clear()
    
    return true
  }
}

// Simple namespace wrapper for storage to isolate different cores
class NamespacedStorage {
  constructor(storage, namespace) {
    this.storage = storage
    this.namespace = namespace
    this.opened = storage.opened
  }

  async open() {
    if (!this.storage.opened) {
      await this.storage.open()
    }
    this.opened = true
  }

  async get(key) {
    const namespacedKey = `${this.namespace}/${key}`
    return this.storage.get(namespacedKey)
  }

  async put(key, value) {
    const namespacedKey = `${this.namespace}/${key}`
    return this.storage.put(namespacedKey, value)
  }

  async delete(key) {
    const namespacedKey = `${this.namespace}/${key}`
    return this.storage.delete(namespacedKey)
  }

  async close() {
    // We don't close the underlying storage, just mark this as closed
    this.opened = false
  }
}

describe('RocksDB IndexedDB adapter with MockCorestore', () => {
  it('should work with MockCorestore and MockHypercore', async () => {
    // Create our RocksDB IndexedDB adapter
    const dbPath = `test_mock_hypercore_${Date.now()}`
    const db = new IndexDBStorage(dbPath)
    await db.open()
    
    // Create a MockCorestore with our adapter directly
    const store = new MockCorestore(db)
    await store.ready()
    
    // Create two cores to test with
    const core1 = store.get({ name: 'core-1' })
    const core2 = store.get({ name: 'core-2' })
    
    // Wait for cores to be ready
    await Promise.all([core1.ready(), core2.ready()])
    
    // Test appending and reading data from core1
    const testData1 = Buffer.from('Hello from core1')
    await core1.append(testData1)
    
    // Verify the data was stored correctly
    const result1 = await core1.get(0)
    expect(result1).toEqual(testData1)
    
    // Test appending and reading data from core2
    const testData2 = Buffer.from('Hello from core2')
    await core2.append(testData2)
    
    // Verify the data was stored correctly
    const result2 = await core2.get(0)
    expect(result2).toEqual(testData2)
    
    // Test that core length is correct
    expect(core1.length).toBe(1)
    expect(core2.length).toBe(1)
    
    // Test updating with multiple entries
    await core1.append([
      Buffer.from('Entry 1'),
      Buffer.from('Entry 2'),
      Buffer.from('Entry 3')
    ])
    
    expect(core1.length).toBe(4)
    
    // Test reading multiple entries
    const entry1 = await core1.get(1)
    const entry2 = await core1.get(2)
    const entry3 = await core1.get(3)
    
    expect(entry1).toEqual(Buffer.from('Entry 1'))
    expect(entry2).toEqual(Buffer.from('Entry 2'))
    expect(entry3).toEqual(Buffer.from('Entry 3'))
    
    // Clean up
    await store.close()
    await db.close()
  });
}); 