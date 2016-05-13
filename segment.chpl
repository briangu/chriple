module Segment {

  use Common, GenHashKey32, Logging, ObjectPool, Operand, PrivateDist, Sort, Query;

  // Globally reusable Null / empty singleton operand
  var NullOperand: [PrivateSpace] Operand;

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

  class NaiveMemorySegment : Segment {

    var totalTripleCount: int;
    var totalPredicateCount: int;

    const predicateHashTableCount: uint(32) = 1024 * 32;
    var predicateHashTable: [0..#predicateHashTableCount] PredicateEntry;

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
          /*writeln("adding ", triple, " count = ", count, " on locale ", here.id);*/
          if (count >= soEntries.size) {
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

    class PredicateEntryOperand: Operand {
      var entry: PredicateEntry;
      var subjectIdCount: int;
      var subjectIds: [0..#subjectIdCount] EntityId;
      var objectIdCount: int;
      var objectIds: [0..#objectIdCount] EntityId;
      var entryPos = 0;
      var needAdvance = true;
      var lastFound = false;

      // TODO: this code is too naive, even for a naive impl.
      //        e.g. we should leverage the fact that the entry arrays are sorted
      //             don't keep searching for subjects which are already found...
      inline proc hasValue(): bool {
        if (!needAdvance) {
          /*info("PredicateEntryOperand hasValue() ", entry.predicate, " ", lastFound);*/
          return lastFound;
        }

        /*info("PredicateEntryOperand hasValue() ", entry.predicate);*/
        // TODO: enable for S, O, SO, and OS scenarios
        // TODO: this method should be idempotent
        var found = false;
        while (!found && (entryPos < entry.count)) {
          if (subjectIdCount > 0) {
            if (objectIdCount > 0) {
              // TODO: if objectIdCount > subjectIdCount then bias to osEntries
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
              if (found) then break;
              entryPos += 1;
            } else {
              var sEntry = (entry.soEntries[entryPos] >> 32):EntityId;
              for s in subjectIds {
                if (sEntry == s) {
                  found = true;
                  break;
                }
              }
              if (found) then break;
              entryPos += 1;
            }
          } else {
            if (objectIdCount > 0) {
              var oEntry = (entry.osEntries[entryPos] >> 32):EntityId;
              for o in objectIds {
                if (oEntry == o) {
                  found = true;
                  break;
                }
              }
              if (found) then break;
              entryPos += 1;
            } else {
              // both subjectIds and objectIds are not specified so just scan through all soEntries
              found = true;
            }
          }
        }
        lastFound = found;
        needAdvance = false;
        /*info("PredicateEntryOperand hasValue() out ", entry.predicate, " ", lastFound);*/
        return found;
      }

      inline proc getValue(): OperandValue {
        /*info("PredicateEntryOperand getValue() ", entry.predicate);*/
        if (!hasValue()) then halt("iterated past end of triples ", entry.predicate);
        if (subjectIdCount > 0) {
          return toTriple(entry.soEntries[entryPos], entry.predicate);
        } else {
          if (objectIdCount > 0) {
            return toTripleFromOSEntry(entry.osEntries[entryPos], entry.predicate);
          } else {
            return toTriple(entry.soEntries[entryPos], entry.predicate);
          }
        }
      }

      inline proc advance() {
        /*info("PredicateEntryOperand advance() ", entry.predicate);*/
        if (!hasValue()) then halt("iterated past end of triples", entry.predicate);
        entryPos += 1;
        needAdvance = true;
      }
    }

    class MultiPredicateEntryOperand: Operand {
      var predicateIdCount: int;
      var predicateIds: [0..#predicateIdCount] PredicateId;
      var subjectIdCount: int;
      var subjectIds: [0..#subjectIdCount] EntityId;
      var objectIdCount: int;
      var objectIds: [0..#objectIdCount] EntityId;

      var entryPos = 0;
      var operand: PredicateEntryOperand;
      var predicateIdPos: int;

      inline proc hasValue(): bool {
        if (operand != nil) {
          /*writeln("MultiPredicateEntryOperand hasValue() ", operand.entry.predicate);*/
        } else {
          /*writeln("MultiPredicateEntryOperand hasValue() ", predicateIds);*/
        }
        if operand == nil {
          while (operand == nil && predicateIdPos < predicateIdCount) {
            var entry = getEntryForPredicateId(predicateIds[predicateIdPos]);
            if (entry != nil) {
              operand = new PredicateEntryOperand(entry, subjectIdCount, subjectIds, objectIdCount, objectIds);
            }
            predicateIdPos += 1;
          }
          if operand == nil then return false;
        }
        /*writeln("MultiPredicateEntryOperand hasValue() new operand ", operand.entry.predicate);*/
        var found = operand.hasValue();
        /*if !found {
          delete operand;
          operand = nil;
        }*/
        return found;
      }

      inline proc getValue(): OperandValue {
        /*writeln("MultiPredicateEntryOperand getValue() ", operand.entry.predicate);*/
        if operand == nil then halt("MultiPredicateEntryOperand::getValue operand == nil");
        return operand.getValue();
      }

      inline proc advance() {
        /*writeln("MultiPredicateEntryOperand advance() ", operand.entry.predicate);*/
        if operand == nil then halt("MultiPredicateEntryOperand::advance operand == nil");
        operand.advance();
        if (!operand.hasValue()) {
          delete operand;
          operand = nil;
        }
      }
    }

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
      if (op != nil) {
        for triple in op.evaluate() {
          yield new QueryResult(triple);
        }
      }
    }

    proc operandForScanPredicate(subjectIds: [?S] EntityId, predicateIds: [?P] PredicateId, objectIds: [?O] EntityId): Operand {
      if predicateIds.size == 0 {
        var allPredicateIds: [0..#totalPredicateCount] PredicateId;
        var idx: int;
        for i in predicateHashTable.domain {
          var entry = predicateHashTable[i];
          if entry != nil {
            allPredicateIds[idx] = entry.predicate;
            idx += 1;
          }
        }
        if (idx > 0) {
          return new MultiPredicateEntryOperand(idx, allPredicateIds, subjectIds.size, subjectIds, objectIds.size, objectIds);
        } else {
          return NullOperand[here.id];
        }
      } else if (predicateIds.size == 1) {
        var entry = getEntryForPredicateId(predicateIds[0]);
        if (entry != nil) {
          return new PredicateEntryOperand(entry, subjectIds.size, subjectIds, objectIds.size, objectIds);
        } else {
          return NullOperand[here.id];
        }
      } else {
        return new MultiPredicateEntryOperand(predicateIds.size, predicateIds, subjectIds.size, subjectIds, objectIds.size, objectIds);
      }
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
