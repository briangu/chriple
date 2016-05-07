module Segment {

  use Common, GenHashKey32, ObjectPool, Operand, PrivateDist, Sort, Query;

  // Globally reusable Null / empty singleton operand
  var NullOperand: [PrivateSpace] Operand;

  class Segment {
    inline proc isSegmentFull(): bool {
      halt("not implemented");
      return true;
    }

    proc addTriple(triple: Triple): bool {
      halt("not implemented");
      return false;
    }

    iter query(query: Query): QueryResult {
      halt("not implemented");
      yield new QueryResult();
    }

    proc operandForTriple(triple: Triple): Operand {
      halt("not implemented");
      return NullOperand[here.id];
    }

    proc optimize() {
      // NOP
    }
  }

  class NaiveMemorySegment : Segment {

    var totalTripleCount: int;

    const predicateHashTableCount: uint(32) = 1024 * 32;

    class PredicateEntry {
      var predicate: PredicateId;

      var count: int;
      var soEntries: [0..#1024] EntityPair;
      var osEntries: [0..#1024] EntityPair;

      proc add(triple: Triple) {
        var soEntry = triple.toSOPair();
        var (found,idx) = soEntries.find(soEntry);
        if (!found) {
          soEntries.push_back(soEntry);
          osEntries.push_back(triple.toOSPair());
        }
      }

      proc optimize() {
        QuickSort(soEntries);
        QuickSort(osEntries);
      }
    }

    // Master table from triple -> tripleEntry -> Document posting list
    // This is a lock-free table and uses atomic PredicateEntryPoolIndex values to point to allocatiosn in the predicateEntryPool
    var predicateHashTable: [0..#predicateHashTableCount] PredicateEntry;

    inline proc predicateHashTableIndexForTriple(triple: Triple): int {
      return genHashKey32(triple.predicate) % predicateHashTableCount;
    }

    proc getOrAddPredicateEntry(triple: Triple): PredicateEntry {
      var entryIndex = predicateHashTableIndexForTriple(triple);;

      var entry = predicateHashTable[entryIndex];
      while (predicateHashTable[entryIndex] != nil) {
        if (entry.predicate == triple.predicate) {
          return entry;
        }
        entryIndex = (entryIndex + 1) % predicateHashTableCount;
        entry = predicateHashTable[entryIndex];
      }

      entry = new PredicateEntry(triple.predicate);;
      predicateHashTable[entryIndex] = entry;
      entry.count += 1;

      return entry;
    }

    proc get(triple: Triple): PredicateEntry {
      // iterate through the entries starting at this table position
      var entryIndex = predicateHashTableIndexForTriple(triple);
      var entry = predicateHashTable[entryIndex];
      while (entry != nil) {
        if (entry.predicate == triple.predicate) {
          return entry;
        }
        entryIndex = (entryIndex + 1) % predicateHashTableCount;
        entry = predicateHashTable[entryIndex];
      }
      return nil;
    }

    proc addTriple(triple: Triple): bool {
      if (isSegmentFull()) then return false;

      var predicateEntry = getOrAddPredicateEntry(triple);
      if (predicateEntry) {
        predicateEntry.add(triple);
        totalTripleCount += 1;
        return true;
      }

      return false;
    }

    iter query(query: Query): QueryResult {
      halt("not implemented");
      yield new QueryResult();
    }

    proc operandForTriple(triple: Triple): Operand {
      halt("not implemented");
      return NullOperand[here.id];
    }

    proc optimize() {
      forall i in predicateHashTable.domain {
        var entry = predicateHashTable[i];
        if (entry) then entry.optimize();
      }
    }
  }
}
