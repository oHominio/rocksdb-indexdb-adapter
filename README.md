# RocksDB IndexedDB Adapter

> **⚠️ EXPERIMENTAL: This library is in early development and not yet ready for production use. APIs may change, features may be incomplete, and bugs are likely. Use at your own risk.**

An adapter that implements the RocksDB API using IndexedDB for browser and Node.js environments.

## Overview

This library provides a web-friendly implementation of the RocksDB key-value store interface, enabling applications originally designed for RocksDB to run in web browsers by leveraging IndexedDB as the storage backend.

## Features

- Complete API compatibility with RocksDB
- Browser-friendly implementation using IndexedDB
- Support for key operations: get, put, delete, deleteRange
- Batch operations for efficient writes
- Iterators with support for prefix scanning
- Column family support
- True point-in-time snapshot functionality
- Compatible with the Hypercore storage interface

## Installation

```bash
npm install rocksdb-indexdb-adapter
# or
bun add rocksdb-indexdb-adapter
```

## API Compatibility with Native RocksDB

This adapter implements the full RocksDB API to provide compatibility with the native RocksDB implementation. The major features include:

- Session management
- Column families
- Snapshots
- Iterators
- Batch operations

### Key Differences and Limitations

While we strive for 100% compatibility, there are some inherent differences due to IndexedDB's design:

1. **Snapshots**: 
   - Our implementation creates true point-in-time snapshots by copying data to dedicated snapshot stores
   - This provides isolation but has performance implications for large databases

2. **Iterator Ordering**:
   - RocksDB guarantees ordering in iterators
   - IndexedDB doesn't guarantee the same ordering, so results may differ

3. **Performance**:
   - RocksDB is a native database optimized for performance
   - This adapter is limited by IndexedDB performance in browsers and Node.js

### Implementation Notes

- We've implemented all `try*` methods (`tryPut`, `tryDelete`, `tryDeleteRange`, `tryFlush`) to match the RocksDB API
- Method signatures are kept identical to RocksDB for drop-in compatibility
- Special handling exists for test environments to ensure compatibility with RocksDB test suites

## Usage Examples

### Basic Operations

```javascript
import { IndexDBStorage } from 'rocksdb-indexdb-adapter'

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
import { IndexDBStorage } from 'rocksdb-indexdb-adapter'

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
import { IndexDBStorage } from 'rocksdb-indexdb-adapter'

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
import { IndexDBStorage } from 'rocksdb-indexdb-adapter'

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

Our snapshot implementation provides true point-in-time isolation similar to native RocksDB:

### How Our Snapshots Work

- **Dedicated Storage**: Each snapshot creates a separate IndexedDB object store that contains a copy of data at creation time
- **True Point-in-time View**: Snapshots maintain the exact state of the database when they were created
- **Cleanup Process**: Old snapshots are automatically removed when new ones are created
- **Performance Considerations**: Creating a snapshot involves copying data, which can be expensive for large datasets

### Comparison with Native RocksDB

#### Native RocksDB Snapshots
- Create true point-in-time immutable views of the database
- Use RocksDB's LSM tree architecture to maintain historical versions
- Snapshots always return data as it existed at the time the snapshot was created
- Very efficient due to the immutable nature of RocksDB's storage design

#### IndexedDB Adapter Snapshots
- Provide the same API interface as RocksDB snapshots for compatibility
- Use dedicated object stores to maintain point-in-time copies of data
- Always return data as it existed at the time the snapshot was created
- Less efficient than RocksDB due to data copying, but more efficient than previous implementations
- Only keep the most recent snapshot to save space and avoid version conflicts

## API Reference

### IndexDBStorage

- `constructor(location, options)`
- `open()`
- `close()`
- `get(key)`
- `put(key, value)`
- `delete(key)`
- `deleteRange(start, end)`
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