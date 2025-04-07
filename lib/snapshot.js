import binding from "./binding.js";

class RocksDBSnapshot {
  constructor(state) {
    this._state = state;
    this._handle = null;
    this._refs = 0;

    // Initialize the snapshot immediately to capture the current database state
    this._init();
  }

  async _init() {
    // Initialize the snapshot handle
    this._handle = binding.snapshotInit();

    // Capture the database state synchronously - this is crucial for consistency
    // The binding.snapshotGet function must complete before any writes happen
    await binding.snapshotGet(this._state._handle, this._handle);
  }

  read(opts = {}) {
    return this._state
      .session({
        ...opts,
        snapshot: this,
      })
      .read();
  }

  ref() {
    this._refs++;
  }

  unref() {
    if (--this._refs > 0) return;
    if (this._handle === null) return;

    binding.snapshotRelease(this._state._handle, this._handle);
    this._handle = null;
  }

  // Helper to release all resources
  async close() {
    if (this._handle !== null) {
      binding.snapshotRelease(this._state._handle, this._handle);
      this._handle = null;
    }
  }
}

export default RocksDBSnapshot;
