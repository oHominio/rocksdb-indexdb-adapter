export class BloomFilterPolicy {
  get type() {
    return 1;
  }

  constructor(bitsPerKey) {
    this.bitsPerKey = bitsPerKey;
  }
}

export class RibbonFilterPolicy {
  get type() {
    return 2;
  }

  constructor(bloomEquivalentBitsPerKey, bloomBeforeLevel = 0) {
    this.bloomEquivalentBitsPerKey = bloomEquivalentBitsPerKey;
    this.bloomBeforeLevel = bloomBeforeLevel;
  }
}
