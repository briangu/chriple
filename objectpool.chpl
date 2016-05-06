module ObjectPool {

  type EntryPoolIndex = uint(32);

  class ObjectPool {
    type T;
    var bankSize: uint = 2 ** 24;  // default is 256 banks of 2**24 entries => 24 + 8

    record PoolEntry {
      var item: T;
      var next: atomic EntryPoolIndex;
    }

    class PoolBank {
      var entries: [0..#bankSize] PoolEntry;
    }

    // use an array of PoolBank so that we don't have to allocate all entry slots up front
    var banks: [0..255] PoolBank;
    var bankIndex = -1; // current pool bank
    var totalEntryCount = -1; // total number of entries in the pool

    inline proc assembleEntryPoolIndex(bankIndex: uint(32), entryPos: uint(32)): EntryPoolIndex {
      return ((bankIndex: EntryPoolIndex) << 24) | (0x00FFFFFF & (entryPos: EntryPoolIndex));
    }

    inline proc splitEntryPoolIndex(poolIndex: EntryPoolIndex): (uint(32), uint(32)) {
      var bankIdx = ((poolIndex & 0xFF000000) & poolIndex) >> 24;
      var entryPos = (poolIndex & 0x00FFFFFF);
      return (bankIdx, entryPos);
    }

    proc add(item: T, next: EntryPoolIndex): EntryPoolIndex {
      totalEntryCount += 1;

      var bankEntryPos = totalEntryCount: uint % bankSize;
      if (bankEntryPos == 0) {
        bankIndex += 1;
        banks[bankIndex] = new PoolBank();
      }

      banks[bankIndex].entries[bankEntryPos].item = item;
      banks[bankIndex].entries[bankEntryPos].next.write(next);

      return assembleEntryPoolIndex(bankIndex: uint(32), bankEntryPos: uint(32));
    }

    inline proc getEntryByIndex(poolIndex: EntryPoolIndex): PoolEntry {
      var (bankIndex, entryPos) = splitEntryPoolIndex(poolIndex);
      return banks[bankIndex].entries[entryPos];
    }

    inline proc getItemByIndex(poolIndex: EntryPoolIndex): T {
      var (bankIndex, entryPos) = splitEntryPoolIndex(poolIndex);
      return banks[bankIndex].entries[entryPos].item;
    }

    inline proc getNextByIndex(poolIndex: EntryPoolIndex): EntryPoolIndex {
      var (bankIndex, entryPos) = splitEntryPoolIndex(poolIndex);
      return banks[bankIndex].entries[entryPos].next;
    }

    inline proc isFull(): bool {
      return totalEntryCount >= (bankIndex * bankSize);
    }
  }
}
