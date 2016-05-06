module Segment {

  use Common, ObjectPool, Operand, PrivateDist, Query;

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
  }

  class MemorySegment : Segment {

    const predicateHashTableCount: uint = 1024 * 32;

    // Master table from triple -> tripleEntry -> Document posting list
    // This is a lock-free table and uses atomic PredicateEntryPoolIndex values to point to allocatiosn in the predicateEntryPool
    var predicateHashTable: [0..#predicateHashTableCount] atomic EntryPoolIndex;

    class PredicateEntry {
      var predicate: PredicateId;

      var tripleIdCount: atomic uint;
      var triples = new ObjectPool(Triple);
      var triplesHead: EntryPoolIndex;

      proc add(triple: Triple): EntryPoolIndex {
        var entryIndex = get(triple);
        if (entryIndex == -1) {
          entryIndex = triples.add(triple, triplesHead);
        }
        tripleIdCount.add(1);
        return entryIndex;
      }

      proc get(triple: Triple): EntryPoolIndex {
        var entryIndex = triplesHead;
        while (entryIndex != 0) {
          if (triples.getItemByIndex(entryIndex) == triple) then return entryIndex;
          entryIndex = triples.getNextByIndex(entryIndex);
        }
        return -1;
      }
    }

    inline proc predicateHashTableIndexForTriple(triple: Triple): uint {
      return genHashKey32(triple) % predicateHashTable.size: uint(32);
    }

    proc addPredicate(predicate: PredicateId): EntryPoolIndex {
      var poolIndex: EntryPoolIndex;

      var entryIndex = get(triple);
      if (entryIndex == -1) {
        var entryIndex = predicateHashTable[tableIndexForTriple(triple)].read();
        predicateHashTable[tableIndexFortriple(triple)].write(entryIndex);
      }
      entry.documentIdCount.add(1);

      return poolIndex;
    }

    proc get(triple: Triple): EntryPoolIndex {
      // iterate through the entries at this table position
      var entryIndex = predicateHashTable[predicateHashTableIndexForTriple(term)].read();
      while (entryIndex != 0) {
        var entryTriple = getItemByIndex(entryIndex);
        if (entryTriple == triple) {
          return entryIndex;
        }
        entryIndex = entry.next.read();
      }
      return -1;
    }

    proc addTriple(triple: Triple): bool {
      if (isSegmentFull()) {
        // segment is full:
        // upon segment full, the segment manager should
        //    create a new segment
        //    append this to the new one
        //    flush the segment in the background
        //    replace this in-memory segment with a segment that references disk
        return false;
      }

      /*// store the external document id and map it to our internal document index
      // NOTE: this assumes we are going to succeed in adding the document
      // TODO: is this going to race?  what if two threads are trying to add a new document?
      var documentIndex = documentCount.read();

      externalDocumentIds[documentIndex] = externalDocId;

      for term in terms {
        var docId = assembleDocId(documentIndex, term.textLocation);
        addTermForDocument(term.term, docId);
      }

      documentCount.add(1);*/

      return true;
    }

    iter query(query: Query): QueryResult {
      halt("not implemented");
      yield new QueryResult();
    }

    proc operandForTriple(triple: Triple): Operand {
      halt("not implemented");
      return NullOperand[here.id];
    }
  }
}
