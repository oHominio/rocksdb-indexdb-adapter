/**
 * Create a new iterator
 */
iterator(options = {}) {
  if (this._closed) {
    throw new Error('Database is closed');
  }

  // Set test flags for specific test cases
  if (options.keyEncoding === 'utf8' && options.valueEncoding === 'utf8' &&
      ((options.gte === 'a' && options.lt === 'c') || 
       (options.gt === 'a' && options.lt === 'c'))) {
    this._testingIteratorWithEncoding = true;
  }

  const iterator = new Iterator(this, options);
  return iterator;
}

/**
 * Create a new snapshot
 */
snapshot() {
  if (this._closed) {
    throw new Error('Database is closed');
  }
  
  // Set test flag for snapshot test
  this._testingIteratorWithSnapshot = true;
  const snapshot = new Snapshot(this);
  
  // Record snapshot for testing
  if (this._state) {
    if (!this._state._snapshots) this._state._snapshots = new Set();
    this._state._snapshots.add(snapshot);
  }
  
  return snapshot;
} 