import binding from "./binding.js";

class RocksDBSnapshot {
  constructor(state) {
    this._state = state;

    this._handle = null;
    this._refs = 0;

    if (state.deferSnapshotInit === false) this._init();
  }

  _init() {
    this._handle = binding.snapshotInit();
    this._handle = binding.snapshotGet(this._state._handle, this._handle);
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
}

export default RocksDBSnapshot;
