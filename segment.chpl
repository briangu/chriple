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

    config const predicateHashTableSize: uint = 1024 * 32;

    // Master table from triple -> tripleEntry -> Document posting list
    // This is a lock-free table and uses atomic PredicateEntryPoolIndex values to point to allocatiosn in the predicateEntryPool
    var predicateHashTable: [0..predicateHashTableSize-1] atomic PredicateEntryPoolIndex;

    class PredicateEntry {
      var triple: Triple;

      // pointer to the last document id in the doc id pool
      var lastTripleIdIndex: atomic TripleIdPoolIndex;

      // next term in the bucket chain
      var next: atomic PredicateEntryPoolIndex;

      proc PredicateEntry(triple: Triple, poolIndex: PredicateEntryPoolIndex) {
        this.triple = triple;
        this.next.write(poolIndex);
      }

      // total number of triples this predicate appears in
      var tripleIdCount: atomic uint;
    }

    proc add(item: Item): ObjectEntryPoolIndex {
      var poolIndex: ObjectEntryPoolIndex;

      var entry = get(item);
      if (entry == nil) {
        /*// no triple in this table position, allocate a new triple in the triple pool
        entry = new ObjectEntry(triple, predicateHashTable[tableIndexFortriple(triple)].read());
        poolIndex = allocateNewDocIdInDocumentIdPool(docId);
        entry.lastTripleIdIndex.write(poolIndex);
        var entryIndex = setObjectEntryAtNextObjectEntryPoolIndex(entry);
        predicateHashTable[tableIndexFortriple(triple)].write(entryIndex);*/
      } else {
        poolIndex = setDocIdAtNextDocumentIdPoolIndex(entry.lastTripleIdIndex.read(), docId);
        entry.lastTripleIdIndex.write(poolIndex);
      }
      entry.documentIdCount.add(1);

      return poolIndex;
    }

    proc getObjectEntryForTriple(triple: Triple): ObjectEntry {
      // iterate through the entries at this table position
      var entryIndex = predicateHashTable[predicateHashTableIndexForTriple(term)].read();
      while (entryIndex != 0) {
        var entry = getPoolEntry(entryIndex);
        if (entry.term == term) {
          return entry;
        }
        entryIndex = entry.next.read();
      }
      return nil;
    }

    inline proc predicateHashTableIndexForTriple(triple: Triple): uint {
      return genHashKey32(triple) % predicateHashTable.size: uint(32);
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
