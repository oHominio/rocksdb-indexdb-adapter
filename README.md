# RocksDB IndexedDB Adapter

A browser-compatible implementation of the RocksDB API using IndexedDB as the underlying storage engine.

## Overview

This library provides a web-friendly implementation of the RocksDB key-value store interface, enabling applications originally designed for RocksDB to run in web browsers by leveraging IndexedDB as the storage backend.

## Features

- Complete API compatibility with RocksDB
- Browser-friendly implementation using IndexedDB
- Support for key operations: get, put, delete, deleteRange
- Batch operations for efficient writes
- Iterators with support for prefix scanning
- Column family support
- Snapshot functionality (WIP: needs discussion)
- Compatible with the Hypercore storage interface

## Installation

```bash
npm install rocksdb-indexdb-adapter
# or
bun add rocksdb-indexdb-adapter
```

## Usage Examples

### Basic Operations

```javascript
import { IndexDBStorage } from 'rocksdb-indexdb-adapter'

// Open a database
const db = new IndexDBStorage('./my-database')
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

const db = new IndexDBStorage('./my-database')
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

const db = new IndexDBStorage('./my-database')
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

const db = new IndexDBStorage('./my-database')
await db.open()

// Add initial data
await db.put('key1', 'initial-value')

// Create a snapshot
const snapshot = db.snapshot()

// Modify data after snapshot
await db.put('key1', 'modified-value')

// Read current value
console.log((await db.get('key1')).toString()) // 'modified-value'

// Read value from snapshot point-in-time
console.log((await snapshot.get('key1')).toString()) // 'initial-value'

// Release the snapshot when done
snapshot.destroy()
```

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
- Adapted to work within browser security and storage constraints

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT 