/*

  Triple Naive is a naive implementation of a triple store using predicate based hash partitions
  and in-memory SO and OS indexes on each partition.  The strategy is naive in the sense that it
  does not make an effort to be super efficient with storage.

*/
use Chasm, Common, GenHashKey32, Logging, Operand, Partition, PrivateDist, Query, Segment, Time, VisualDebug;

config const subjectCount = 16;
config const predicateCount = 8;
config const objectCount = 16;
const totalTripleCount = subjectCount * predicateCount * objectCount;
const sRange = 0..#subjectCount;
const pRange = 0..#predicateCount;
const oRange = 0..#objectCount;
var soCount = subjectCount * objectCount;

config const verify_print = false;

proc initPartitions() {
  var t: Timer;
  t.start();

  forall p in Partitions do p = new PartitionManager(new NaiveMemorySegment());
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

proc addTriple(triple: Triple) {
  var partitionId = partitionIdForTriple(triple);
  on Locales[partitionId] do Partitions[here.id].addTriple(triple);
}

proc addPredicateTriples(predicate: PredicateId, triples: [?D] Triple) {
  var partitionId = partitionIdForPredicate(predicate);
  on Locales[partitionId] do Partitions[here.id].addTriples(triples);
}

proc addSyntheticData() {
  /*startVdebug("add_triple");*/
  for p in 0..#predicateCount {
    writeln("adding predicate: ", p);
    var partitionId = partitionIdForPredicate(p:PredicateId);
    on Locales[partitionId] {
      var triples: [0..#soCount] Triple;
      for s in 0..#subjectCount {
        for o in 0..#objectCount {
          triples[s*objectCount + o] = new Triple(s:EntityId, p:PredicateId, o:EntityId);
        }
      }
      addPredicateTriples(p: PredicateId, triples);
    }
  }
  /*stopVdebug();*/
}

iter localQuery(query: Query) {
  var lq: Query;
  if (query.locale.id != here.id) {
    var lq: Query = new Query(query);
  } else {
    lq = query;
  }
  local {
    for res in Partitions[here.id].query(lq) {
      yield res;
    }
  }
}

// serial iterator
iter query(query: Query) {

  var totalCounts = 0;
  var outerResults: [0..(Locales.size * query.partitionLimit)-1] QueryResult;

  for loc in Locales {
    on loc {
      // copy query into locale
      var lq: Query = new Query(query);

      var innerResults: [0..lq.partitionLimit-1] QueryResult;
      var innerCount = 0;

      local {
        for res in localQuery(lq) {
          innerResults[innerCount] = res;
          innerCount += 1;
          if (innerCount > innerResults.domain.high) {
            break;
          }
        }
      }

      if (innerCount > 0) {
        outerResults[totalCounts..totalCounts+innerCount-1] = innerResults[0..innerCount-1];
        totalCounts += innerCount;
      }
    }
  }

  for i in 0..totalCounts-1 {
    yield outerResults[i];
  }
}

proc printTriples(q: Query) {
  for result in query(q) {
    writeln(result.triple);
  }
}

proc verifyTriples(sRange, pRange, oRange, q: Query) {
  var tuples: [sRange, pRange, oRange] bool;
  for s in sRange {
    for p in pRange {
      for o in oRange {
        tuples[s,p,o] = true;
      }
    }
  }

  for result in query(q) {
    var t = result.triple;
    if (verify_print) then writeln(t);

    if (t.subject < sRange.low && t.subject > sRange.high) then halt("t.subject < sRange.low && t.subject > sRange.high");
    if (t.predicate < pRange.low && t.predicate > pRange.high) then halt("t.predicate < pRange.low && t.predicate > pRange.high");
    if (t.object < oRange.low && t.object > oRange.high) then halt("t.object < oRange.low && t.object > oRange.high");

    if (!tuples[t.subject, t.predicate, t.object]) then halt("tuple not found: ", t);

    // mark the tuple as touched
    tuples[t.subject, t.predicate, t.object] = false;
  }

  var failed = false;
  for s in sRange {
    for p in pRange {
      for o in oRange {
        if (tuples[s,p,o]) {
          writeln(" (", s, " ", p, " ", o, ") was not verified.");
          failed = true;
        }
      }
    }
  }
  if (failed) then halt("found tuples which were not verified.");
}

proc verifyOperand(sRange, pRange, oRange, op: Operand) {
  var tuples: [sRange, pRange, oRange] bool;
  for s in sRange {
    for p in pRange {
      for o in oRange {
        tuples[s,p,o] = true;
      }
    }
  }

  for t in op.evaluate() {
    if (t.subject < sRange.low && t.subject > sRange.high) then halt("t.subject < sRange.low && t.subject > sRange.high");
    if (t.predicate < pRange.low && t.predicate > pRange.high) then halt("t.predicate < pRange.low && t.predicate > pRange.high");
    if (t.object < oRange.low && t.object > oRange.high) then halt("t.object < oRange.low && t.object > oRange.high");

    if (!tuples[t.subject, t.predicate, t.object]) then halt("tuple not found: ", t);

    // mark the tuple as touched
    tuples[t.subject, t.predicate, t.object] = false;
  }

  var failed = false;
  for s in sRange {
    for p in pRange {
      for o in oRange {
        if (tuples[s,p,o]) {
          writeln(" (", s, " ", p, " ", o, ") was not verified.");
          failed = true;
        }
      }
    }
  }
  if (failed) then halt("found tuples which were not verified.");
}

proc querySyntheticData() {
  var q = new Query(new InstructionBuffer(2048));
  {
    q.instructionBuffer.clear();
    var w = new InstructionWriter(q.instructionBuffer);
    // select * where s = 1 and p = 2 and o = 3
    //    --> on p, find all triples with s = 1 and o = 3 (which is at most 1)
    w.writeScanPredicate();
    // list of subjects to scan
    w.writeCount(1);
    w.writeSubjectId(1);
    // list of predicates to scan
    w.writeCount(1);
    w.writePredicateId(2);
    // list of objects to scan
    w.writeCount(1);
    w.writeObjectId(3);

    w.writeHalt();

    writeln("triples of (1,2,3)");
    verifyTriples(1..1, 2..2, 3..3, q);
  }

  {
    q.instructionBuffer.clear();
    var w = new InstructionWriter(q.instructionBuffer);
    w.writeScanPredicate();
    w.writeCount(1);
    w.writeSubjectId(1);
    w.writeCount(1);
    w.writePredicateId(2);
    w.writeCount(2);
    w.writeObjectId(3);
    w.writeObjectId(4);
    w.writeHalt();

    writeln("triples of (1,2,[3,4])");
    verifyTriples(1..1, 2..2, 3..4, q);
  }
  {
    q.instructionBuffer.clear();
    var w = new InstructionWriter(q.instructionBuffer);
    w.writeScanPredicate();
    w.writeCount(1);
    w.writeSubjectId(1);
    w.writeCount(0);
    w.writeCount(2);
    w.writeObjectId(3);
    w.writeObjectId(4);
    w.writeHalt();

    writeln("scan all triples of the form (1,*,[3,4])");
    verifyTriples(1..1, 0..#predicateCount, 3..4, q);
  }
  {
    q.instructionBuffer.clear();
    var w = new InstructionWriter(q.instructionBuffer);
    w.writeScanPredicate();
    w.writeCount(0);
    w.writeCount(1);
    w.writePredicateId(2);
    w.writeCount(2);
    w.writeObjectId(3);
    w.writeObjectId(4);
    w.writeHalt();

    writeln("scan all triples of the form (*,2,[3,4])");
    verifyTriples(0..#subjectCount, 2..2, 3..4, q);
  }
  {
    q.instructionBuffer.clear();
    var w = new InstructionWriter(q.instructionBuffer);
    w.writeScanPredicate();
    w.writeCount(1);
    w.writeSubjectId(1);
    w.writeCount(1);
    w.writePredicateId(2);
    w.writeCount(0);
    w.writeHalt();

    writeln("scan all triples of the form (1,2,*])");
    verifyTriples(1..1, 2..2, 0..#objectCount, q);
  }
  {
    q.instructionBuffer.clear();
    var w = new InstructionWriter(q.instructionBuffer);
    w.writeScanPredicate();
    w.writeCount(0);
    w.writeCount(0);
    w.writeCount(0);
    w.writeHalt();

    writeln("scan all triples");
    verifyTriples(0..#subjectCount, 0..#predicateCount, 0..#objectCount, q);
  }
  {
    q.instructionBuffer.clear();
    var w = new InstructionWriter(q.instructionBuffer);
    // A = (*,2,3)
    w.writeScanPredicate();
    w.writeCount(4);
    w.writeSubjectId(1);
    w.writeSubjectId(2);
    w.writeSubjectId(3);
    w.writeSubjectId(4);
    w.writeCount(1);
    w.writePredicateId(2);
    w.writeCount(1);
    w.writeObjectId(3);

    // B = ([2,3],3,4)
    w.writeScanPredicate();
    w.writeCount(2);
    w.writeSubjectId(2);
    w.writeSubjectId(3);
    w.writeCount(1);
    w.writePredicateId(2);
    w.writeCount(1);
    w.writeSubjectId(4);

    // A.object AND B.object
    w.writeAnd();
    w.writeSPOMode(OperandSPOModeSubject);

    w.writeHalt();

    writeln("scan all triples of the form (2..3, 2..3, 3..4)");
    printTriples(q);
    /*verifyTriples(2..3, 2..3, 3..4, q);*/
  }
}

proc testOperand() {
  var t = new Triple(1,2,3);

  var tarr: [0..#totalTripleCount] Triple;
  var idx = 0;
  for p in pRange {
    for s in sRange {
      for o in oRange {
        tarr[idx].subject = s: EntityId;
        tarr[idx].predicate = p: PredicateId;
        tarr[idx].object = o: EntityId;
        /*writeln(idx, " ", tarr[idx]);*/
        idx += 1;
      }
    }
  }

  /*writeln("soCount ", soCount);*/
  var entry = new PredicateEntry(1, soCount);
  for t in 0..#soCount do entry.add(tarr[t]);
  /*entry.optimize();*/
  /*writeln("soEntries");*/
  /*for x in entry.soEntries do writef("%016xu\n", x);*/
  /*writeln("osEntries");
  for x in entry.osEntries do writef("%016xu\n", x);*/

  var subjectIdCount: int = 2;
  var subjectIds: [0..#subjectIdCount] EntityId;
  subjectIds[0] = 1;
  subjectIds[1] = 2;
  var objectIdCount: int = 3;
  var objectIds: [0..#objectIdCount] EntityId;
  objectIds[0] = 3;
  objectIds[1] = 4;
  objectIds[2] = 5;

  var peo = new PredicateEntryOperand(entry, subjectIdCount, subjectIds, objectIdCount, objectIds);
  verifyOperand(1..2, 1..1, 3..5, peo);
}

proc testPredicateEntry() {
  var t = new Triple(1,2,3);

  var tarr: [0..#totalTripleCount] Triple;
  var idx = 0;
  for p in pRange {
    for s in sRange {
      for o in oRange {
        tarr[idx].subject = s: EntityId;
        tarr[idx].predicate = p: PredicateId;
        tarr[idx].object = o: EntityId;
        idx += 1;
      }
    }
  }

  var entry = new PredicateEntry(1, soCount);
  for t in 0..#soCount do entry.add(tarr[t]);
  entry.optimize();
  /*for x in entry.soEntries do writef("%016xu\n", x);*/
  /*for x in entry.osEntries do writef("%016xu\n", x);*/
  var sidx = 0;
  var oidx = 0;
  for x in entry.soEntries {
    var expected = (sidx << 32 | oidx);
    /*writef("%016xu %016xu\n", x, expected);*/
    assert(x == expected);
    oidx += 1;
    if (oidx % objectCount == 0) {
      sidx += 1;
      oidx = 0;
    }
  }
  sidx = 0;
  oidx = 0;
  for x in entry.osEntries {
    var expected = (oidx << 32 | sidx);
    /*writef("%016xu %016xu\n", x, expected);*/
    assert(x == expected);
    sidx += 1;
    if (sidx % subjectCount == 0) {
      sidx = 0;
      oidx += 1;
    }
  }
}

proc dump() {
  for loc in Locales do on loc do for triple in Partitions[here.id].dump() do writeln(triple);
}

proc main() {
  writeln("testTriple:");
  testTriple();

  writeln("testOperand:");
  testOperand();

  writeln("testPredicateEntry");
  testPredicateEntry();

  writeln("test queries");
  initPartitions();
  addSyntheticData();
  querySyntheticData();
  /*dump();*/
}
