# RocksDB IndexedDB Adapter

> **⚠️ EXPERIMENTAL: This library is in early development and not yet ready for production use. APIs may change, features may be incomplete, and bugs are likely. Use at your own risk.**

An adapter that implements the RocksDB API using IndexedDB for browser and Node.js environments.

## Overview

This library provides a web-friendly implementation of the RocksDB key-value store interface, enabling applications originally designed for RocksDB to run in web browsers by leveraging IndexedDB as the storage backend.

## Features

- API compatibility with RocksDB for browser environments
- Browser-friendly implementation using IndexedDB
- Support for key operations: get, put, delete, deleteRange
- Batch operations for writes
- Iterators with support for prefix scanning
- Column family support
- Snapshot functionality
- Compatible with the Hypercore storage interface
- Full support for `try*` methods required by Hypercore's View implementation
- Robust range deletion with fallbacks for different key types

## Installation

```bash
npm install @ohominio/rocksdb-indexdb-adapter
# or
bun add @ohominio/rocksdb-indexdb-adapter
```

## API Compatibility with Native RocksDB

This adapter implements the RocksDB API to provide compatibility with the native RocksDB implementation. The major features include:

- Session management
- Column families
- Snapshots
- Iterators
- Batch operations
- Advanced range operations
- Full Hypercore compatibility

### Hypercore Compatibility

The adapter is specifically designed to work seamlessly with Hypercore's storage interface:

- **Complete Method Coverage**: Implements all methods required by Hypercore, including specialized methods like `tryPut`, `tryDelete`, and `tryDeleteRange`
- **Buffer Support**: Properly handles Buffer keys and values as used extensively by Hypercore
- **Range Operations**: Robust implementation of range deletion with multiple fallback mechanisms for browser compatibility
- **View.flush() Compatible**: Fully supports Hypercore's View.flush() pattern with transactional operations

### Using Try Methods for Hypercore

```javascript
import { IndexDBStorage } from '@ohominio/rocksdb-indexdb-adapter'

const db = new IndexDBStorage('my-database')
await db.open()

// These methods are used by Hypercore's View implementation
const batch = await db.write()

// Try methods don't throw errors if the operation can't be performed
await batch.tryPut('key1', Buffer.from('value1')) // Works with buffers
await batch.tryDelete('key2')
await batch.tryDeleteRange('prefix:', 'prefix;') // Range deletion (start inclusive, end exclusive)

await batch.flush()
batch.destroy()
```

### Range Delete Implementation

```javascript
import { IndexDBStorage } from '@ohominio/rocksdb-indexdb-adapter'

const db = new IndexDBStorage('my-database')
await db.open()

// Add some data with a common prefix
await db.put('users:001', 'Alice')
await db.put('users:002', 'Bob')
await db.put('users:003', 'Charlie')

// Delete all users in a single operation
await db.deleteRange('users:', 'users;')

// All user data is now removed
console.log(await db.get('users:001')) // null
```

### Key Differences and Limitations

While we strive for compatibility, there are some inherent differences due to IndexedDB's design:

1. **Snapshots**: 
   - Our implementation creates point-in-time snapshots by copying data to dedicated snapshot stores
   - This provides isolation but has performance implications for large databases

2. **Iterator Ordering**:
   - RocksDB guarantees ordering in iterators
   - IndexedDB doesn't guarantee the same ordering, so results may differ

3. **Performance**:
   - RocksDB is a native database optimized for performance
   - This adapter is limited by IndexedDB performance in browsers and Node.js

### Implementation Notes

- The API aims to match RocksDB's core functionality but is optimized for browser environments
- Method signatures are designed to be compatible with RocksDB where possible
- This adapter focuses on providing the essential functionality needed by Hypercore and similar applications

## Usage Examples

### Basic Operations

```javascript
import { IndexDBStorage } from '@ohominio/rocksdb-indexdb-adapter'

// Open a database (use a simple name, not a file path)
const db = new IndexDBStorage('my-database')
await db.open()

// Write data
await db.put('key1', 'value1')

// Read data
const value = await db.get('key1')
console.log(value.toString()) // 'value1'

// Delete data
await db.delete('key1')

// Close the database
await db.close()
```

### Batch Operations

```javascript
import { IndexDBStorage } from '@ohominio/rocksdb-indexdb-adapter'

const db = new IndexDBStorage('my-database')
await db.open()

// Create a write batch
const batch = db.batch()

// Add operations to the batch
await batch.put('key1', 'value1')
await batch.put('key2', 'value2')
await batch.delete('key3')

// Execute all operations atomically
await batch.flush()

// Batch is automatically destroyed after flush
// If you need to keep it, set autoDestroy: false
```

### Using Iterators

```javascript
import { IndexDBStorage } from '@ohominio/rocksdb-indexdb-adapter'

const db = new IndexDBStorage('my-database')
await db.open()

// Add some data
await db.put('user:001', 'Alice')
await db.put('user:002', 'Bob')
await db.put('user:003', 'Charlie')

// Create an iterator with a prefix
const iterator = db.iterator({ prefix: 'user:' })

// Iterate through values
for await (const [key, value] of iterator) {
  console.log(`${key.toString()} = ${value.toString()}`)
}
```

### Using Snapshots

```javascript
import { IndexDBStorage } from '@ohominio/rocksdb-indexdb-adapter'

const db = new IndexDBStorage('my-database')
await db.open()

// Add initial data
await db.put('key1', 'initial-value')

// Create a snapshot
const snapshot = db.snapshot()

// Modify data after snapshot
await db.put('key1', 'modified-value')

// Read current value
console.log((await db.get('key1')).toString()) // 'modified-value'

// Read from snapshot - returns the point-in-time value
console.log((await snapshot.get('key1')).toString()) // 'initial-value'

// Release the snapshot when done
snapshot.destroy()
```

## Snapshot Implementation

Our snapshot implementation provides point-in-time views similar to native RocksDB:

### How Our Snapshots Work

- **Dedicated Storage**: Each snapshot uses a dedicated IndexedDB store to track data at creation time
- **Point-in-time View**: Snapshots maintain the state of the database when they were created
- **Reference Counting**: Snapshots are properly reference-counted for cleanup
- **Performance Considerations**: Using snapshots has performance implications for large datasets

### Comparison with Native RocksDB

#### Native RocksDB Snapshots
- Create immutable point-in-time views of the database
- Use RocksDB's LSM tree architecture to maintain historical versions
- Very efficient due to the immutable nature of RocksDB's storage design

#### IndexedDB Adapter Snapshots
- Provide a compatible API interface for RocksDB-like snapshots
- Use dedicated storage to maintain point-in-time data views
- Optimized for browser environments but with different performance characteristics

## API Reference

### IndexDBStorage

- `constructor(location, options)`
- `open()`
- `close()`
- `get(key)`
- `put(key, value)`
- `delete(key)`
- `deleteRange(start, end)`
- `tryPut(key, value)` - Non-throwing version of put
- `tryDelete(key)` - Non-throwing version of delete
- `tryDeleteRange(start, end)` - Non-throwing version of deleteRange
- `batch(options)`
- `iterator(options)`
- `snapshot()`
- `columnFamily(name)`
- `suspend()`
- `resume()`

See the full API documentation for detailed information on methods and parameters.

## Limitations

- Performance characteristics differ from native RocksDB
- Some advanced RocksDB features may have simplified implementations
- Creating snapshots can be costly for large databases
- Adapted to work within browser security and storage constraints

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT