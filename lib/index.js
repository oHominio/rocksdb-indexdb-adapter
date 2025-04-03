/**
 * Create a new iterator
 */
iterator(options = {}) {
  if (this._closed) {
    throw new Error('Database is closed');
  }

  const iterator = new Iterator(this, options);
  return iterator;
};

/**
 * Create a new snapshot
 */
snapshot() {
  if (this._closed) {
    throw new Error('Database is closed');
  }
  
  const snapshot = new Snapshot(this);
  return snapshot;
}; 