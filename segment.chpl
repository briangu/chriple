module Segment {

  use Common, GenHashKey32, Logging, ObjectPool, Operand, PrivateDist, Sort, Time, Query;

  // Globally reusable Null / empty singleton operand
  var NullOperand: [PrivateSpace] Operand;
  var Partitions: [PrivateSpace] Segment;

  proc initPartitions() {
    var t: Timer;
    t.start();

    forall p in Partitions do p = new NaiveMemorySegment();
    forall n in NullOperand do n = new Operand();

    t.stop();
    timing("initialized partitions in ",t.elapsed(TimeUnits.microseconds), " microseconds");
  }

  inline proc partitionIdForTriple(triple: Triple): int {
    return partitionIdForPredicate(triple.predicate);
  }

  inline proc partitionIdForPredicate(predicate: PredicateId): int {
    return genHashKey32(predicate) % numLocales;
  }

  class Segment {
    inline proc isSegmentFull(count: int = 1): bool {
      halt("not implemented");
      return true;
    }

    proc addTriple(triple: Triple): bool {
      halt("not implemented");
      return false;
    }

    proc addTriples(triples: [?D] Triple): bool {
      halt("not implemented");
      return false;
    }

    iter query(query: Query): QueryResult {
      halt("not implemented");
      yield new QueryResult();
    }

    proc operandForScanPredicate(subjectIds: [?S] EntityId, predicateIds: [?P] PredicateId, objectIds: [?O] EntityId): Operand {
      halt("not implemented");
      return NullOperand[here.id];
    }

    proc optimize() {}

    iter dump(): Triple { halt(); yield new Triple(0,0,0); }
  }

  class PredicateEntry {
    var predicate: PredicateId;
    var initialEntryCount = 128*1024;
    var entriesArrayIncrementCount = 100;

    var count: int;
    var soEntries: [0..#initialEntryCount] EntityPair;
    var osEntries: [0..#initialEntryCount] EntityPair;

    inline proc add(triple: Triple) {
      var soEntry = triple.toSOPair();
      /*var (found,idx) = soEntries.find(soEntry);*/
      const found = false;
      if (!found) {
        /*info("adding ", triple, " count = ", count, " ", soEntries.size);*/
        if (count >= soEntries.size) {
          info("increasing size of soEntries to ", count+entriesArrayIncrementCount);
          // TODO: optimize inserts
          soEntries.insert(count+entriesArrayIncrementCount, 0);
          osEntries.insert(count+entriesArrayIncrementCount, 0);
        }
        soEntries[count] = soEntry;
        osEntries[count] = triple.toOSPair();
        count += 1;
      } else {
        /*writeln("found ", triple);*/
      }
    }

    proc optimize() {
      QuickSort(soEntries[0..#count]);
      QuickSort(osEntries[0..#count]);
      // TODO: remove duplicates
    }

    iter dump(): Triple {
      for i in 0..#count do yield toTriple(soEntries[i], predicate);
    }
  }

  class PredicateEntryOperandSO: Operand {
    var entry: PredicateEntry;
    var subjectIdCount: int;
    var subjectIds: [0..#subjectIdCount] EntityId;
    var objectIdCount: int;
    var objectIds: [0..#objectIdCount] EntityId;
    var entryPos = 0;
    var found: bool;

    proc init() {
      findNextEntry();
    }

    proc findNextEntry() {
      found = false;
      while (!found && (entryPos < entry.count)) {
        // TODO: if objectIdCount > subjectIdCount then bias to osEntries
        // TODO: replace this horribly inefficient code.
        var sEntry = (entry.soEntries[entryPos] >> 32):EntityId;
        for s in subjectIds {
          if (sEntry == s) {
            var oEntry = entry.soEntries[entryPos]: EntityId;
            for o in objectIds {
              if (oEntry == o) {
                found = true;
                break;
              }
            }
            if (found) then break;
          }
        }
        if (!found) then entryPos += 1;
      }
    }

    proc hasValue(): bool {
      return found;
    }

    proc getValue(): OperandValue {
      assert(hasValue());
      return toTriple(entry.soEntries[entryPos], entry.predicate);
    }

    proc advance() {
      assert(hasValue());
      entryPos += 1;
      findNextEntry();
    }
  }

  class PredicateEntryOperandStarSAndO: Operand {
    var entry: PredicateEntry;
    var objectIdCount: int;
    var objectIds: [0..#objectIdCount] EntityId;
    var entryPos = 0;
    var found: bool;

    proc init() {
      findNextEntry();
    }

    proc findNextEntry() {
      found = false;
      while (!found && (entryPos < entry.count)) {
        var oEntry = (entry.osEntries[entryPos] >> 32):EntityId;
        for o in objectIds {
          if (oEntry == o) {
            found = true;
            break;
          }
        }
        if (!found) then entryPos += 1;
      }
    }

    proc hasValue(): bool {
      return found;
    }

    proc getValue(): OperandValue {
      assert(hasValue());
      return toTripleFromOSEntry(entry.osEntries[entryPos], entry.predicate);
    }

    proc advance() {
      assert(hasValue());
      entryPos += 1;
      findNextEntry();
    }
  }

  class PredicateEntryOperandSAndStarO: Operand {
    var entry: PredicateEntry;
    var subjectIdCount: int;
    var subjectIds: [0..#subjectIdCount] EntityId;
    var entryPos = 0;
    var found: bool;

    proc init() {
      findNextEntry();
    }

    proc findNextEntry() {
      found = false;
      while (!found && (entryPos < entry.count)) {
        // TODO: replace this horribly inefficient code.
        var sEntry = (entry.soEntries[entryPos] >> 32):EntityId;
        for s in subjectIds {
          if (sEntry == s) {
            found = true;
            break;
          }
        }
        if (!found) then entryPos += 1;
      }
    }

    proc hasValue(): bool {
      return found;
    }

    proc getValue(): OperandValue {
      assert(hasValue());
      return toTriple(entry.soEntries[entryPos], entry.predicate);
    }

    proc advance() {
      assert(hasValue());
      entryPos += 1;
      findNextEntry();
    }
  }

  class PredicateEntryOperandStarSAndStarO: Operand {
    var entry: PredicateEntry;
    var entryPos = 0;

    proc hasValue(): bool {
      return (entryPos < entry.count);
    }

    proc getValue(): OperandValue {
      assert(hasValue());
      return toTriple(entry.soEntries[entryPos], entry.predicate);
    }

    proc advance() {
      assert(hasValue());
      entryPos += 1;
    }
  }

  class PredicateEntryOperand: Operand {
    var entry: PredicateEntry;
    var subjectIdCount: int;
    var subjectIds: [0..#subjectIdCount] EntityId;
    var objectIdCount: int;
    var objectIds: [0..#objectIdCount] EntityId;

    var operand: Operand;

    proc init() {
      /*info("PredicateEntryOperand::init");*/
      if (operand != nil) then return;

      if (subjectIdCount > 0) {
        if (objectIdCount > 0) {
          operand = new PredicateEntryOperandSO(entry, subjectIdCount, subjectIds, objectIdCount, objectIds);
        } else {
          operand = new PredicateEntryOperandSAndStarO(entry, subjectIdCount, subjectIds);
        }
      } else {
        if (objectIdCount > 0) {
          operand = new PredicateEntryOperandStarSAndO(entry, objectIdCount, objectIds);
        } else {
          operand = new PredicateEntryOperandStarSAndStarO(entry);
        }
      }
      operand.init();
    }

    proc cleanup() {
      /*info("PredicateEntryOperand::cleanup");*/
      if (operand == nil) then return;

      operand.cleanup();
      delete operand;
      operand = nil;
    }

    proc hasValue(): bool {
      /*info("PredicateEntryOperand::hasValue");*/
      assert(operand != nil);
      /*if (operand == nil) then return false;*/
      return operand.hasValue();
    }

    proc getValue(): OperandValue {
      /*info("PredicateEntryOperand::getValue");*/
      assert(operand != nil);
      return operand.getValue();
    }

    proc advance() {
      /*info("PredicateEntryOperand::advance");*/
      assert(operand != nil);
      operand.advance();
    }
  }

  class MultiPredicateEntryOperand: Operand {
    var entryCount: int;
    var entries: [0..#entryCount] PredicateEntry;
    var subjectIdCount: int;
    var subjectIds: [0..#subjectIdCount] EntityId;
    var objectIdCount: int;
    var objectIds: [0..#objectIdCount] EntityId;

    var operands: [0..#entryCount] Operand;
    var entryPos = 0;

    proc init() {
      if operands[0] != nil then return;

      for idx in 0..#entryCount {
        operands[idx] = new PredicateEntryOperand(entries[idx], subjectIdCount, subjectIds, objectIdCount, objectIds);
        operands[idx].init();
      }
    }

    proc cleanup() {
      if operands[0] == nil then return;

      for idx in operands.domain {
        operands[idx].cleanup();
        delete operands[idx];
        operands[idx] = nil;
      }
    }

    proc hasValue(): bool {
      assert(operands[0] != nil);
      if entryPos >= entryCount then return false;

      var found = operands[entryPos].hasValue();
      if (!found) {
        entryPos += 1;
        found = hasValue();
      }
      return found;
    }

    proc getValue(): OperandValue {
      assert(operands[0] != nil);
      assert(entryPos < entryCount);

      if entryPos >= entryCount then halt("MultiPredicateEntryOperand::getValue operand == nil");
      return operands[entryPos].getValue();
    }

    proc advance() {
      assert(operands[0] != nil);
      assert(entryPos < entryCount);

      operands[entryPos].advance();
    }
  }

  class NaiveMemorySegment : Segment {

    var totalTripleCount: int;
    var totalPredicateCount: int;

    const predicateHashTableCount: uint(32) = 1024 * 32;
    var predicateHashTable: [0..#predicateHashTableCount] PredicateEntry;

    inline proc predicateHashTableIndexForTriple(triple: Triple): int {
      return predicateHashTableIndexForPredicateId(triple.predicate);
    }
    inline proc predicateHashTableIndexForPredicateId(predicateId: PredicateId): int {
      return genHashKey32(predicateId) % predicateHashTableCount;
    }

    proc getOrAddPredicateEntry(triple: Triple): PredicateEntry {
      var entry: PredicateEntry;

      local {
        var entryIndex = predicateHashTableIndexForTriple(triple);;
        entry = predicateHashTable[entryIndex];
        while (predicateHashTable[entryIndex] != nil) {
          if (entry.predicate == triple.predicate) {
            return entry;
          }
          entryIndex = (entryIndex + 1) % predicateHashTableCount;
          entry = predicateHashTable[entryIndex];
        }

        entry = new PredicateEntry(triple.predicate);
        predicateHashTable[entryIndex] = entry;
        totalPredicateCount += 1;
      }

      return entry;
    }

    proc getEntryForPredicateId(predicateId: PredicateId): PredicateEntry {
      // iterate through the entries starting at this table position
      var entryIndex = predicateHashTableIndexForPredicateId(predicateId);
      var entry = predicateHashTable[entryIndex];
      while (entry != nil) {
        if (entry.predicate == predicateId) {
          return entry;
        }
        entryIndex = (entryIndex + 1) % predicateHashTableCount;
        entry = predicateHashTable[entryIndex];
      }
      return entry;
    }

    inline proc isSegmentFull(count: int = 1): bool {
      return false;
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

    proc addTriples(triples: [?D] Triple): bool {
      if (isSegmentFull(triples.size)) then return false;

      const startCount = totalTripleCount;

      for triple in triples {
        var predicateEntry = getOrAddPredicateEntry(triple);
        if (predicateEntry == nil) then break;
        predicateEntry.add(triple);
        totalTripleCount += 1;
      }

      return (totalTripleCount - startCount) == triples.size;
    }

    iter query(query: Query): QueryResult {
      var op = chasmInterpret(this, query.instructionBuffer);
      /*info("NaiveMemorySegment::query");*/
      if (op != nil) {
        for triple in op.evaluate() {
          yield new QueryResult(triple);
        }
      }
    }

    proc populateAllPredicateEntries(ref allPredicateEntries: [0..#totalPredicateCount] PredicateEntry) {
      return allPredicateEntries;
    }

    proc operandForScanPredicate(subjectIds: [?S] EntityId, predicateIds: [?P] PredicateId, objectIds: [?O] EntityId): Operand {
      var entryOperand: Operand;

      if (predicateIds.size == 0) {
        /*info("operandForScanPredicate: predicateIds.size == 0");*/
        var predicateEntries: [0..#totalPredicateCount] PredicateEntry;
        var idx: int;
        for entry in predicateHashTable {
          if entry != nil {
            predicateEntries[idx] = entry;
            idx += 1;
          }
        }
        if (idx > 0) {
          entryOperand = new MultiPredicateEntryOperand(idx, predicateEntries, subjectIds.size, subjectIds, objectIds.size, objectIds);
        }
      } else if (predicateIds.size == 1) {
        /*info("operandForScanPredicate: predicateIds.size == 1");*/
        var entry = getEntryForPredicateId(predicateIds[0]);
        if (entry != nil) {
          entryOperand = new PredicateEntryOperand(entry, subjectIds.size, subjectIds, objectIds.size, objectIds);
        }
      } else {
        /*info("operandForScanPredicate: predicateIds.size == ", predicateIds.size);*/
        var predicateEntries: [0..#predicateIds.size] PredicateEntry;
        var idx: int;
        for i in predicateIds {
          var entry = predicateHashTable[i];
          if entry != nil {
            predicateEntries[idx] = entry;
            idx += 1;
          }
        }
        if (idx > 0) {
          entryOperand = new MultiPredicateEntryOperand(idx, predicateEntries, subjectIds.size, subjectIds, objectIds.size, objectIds);
        }
      }

      return if entryOperand != nil then entryOperand else NullOperand[here.id];
    }

    proc optimize() {
      forall i in predicateHashTable.domain {
        var entry = predicateHashTable[i];
        if (entry) then entry.optimize();
      }
    }

    iter dump(): Triple {
      for i in predicateHashTable.domain {
        var entry = predicateHashTable[i];
        if (entry) {
          for triple in entry.dump() do yield triple;
        }
      }
    }
  }
}
