/*

  Triple Naive is a naive implementation of a triple store using predicate based hash partitions
  and in-memory SO and OS indexes on each partition.  The strategy is naive in the sense that it
  does not make an effort to be super efficient with storage.

*/
use Chasm, Common, GenHashKey32, Logging, Operand, PrivateDist, Query, Segment, Time, VisualDebug;

config const subjectCount = 16;
config const predicateCount = 8;
config const objectCount = 16;
const totalTripleCount = subjectCount * predicateCount * objectCount;
const sRange = 0..#subjectCount;
const pRange = 0..#predicateCount;
const oRange = 0..#objectCount;
const soCount = subjectCount * objectCount;

config const verify_print = false;

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

/*iter localQuery(query: Query) {
  var lq = if (query.instructionBuffer.locale.id != here.id) then new Query(query) else query;
  local {
    for res in Partitions[here.id].query(lq) do yield res;
  }
}*/

// Since iterators cannot be stepped manually (e.g. with next()),
// in order to apply cross partition operands, we need to spool partition
// results into a local memory segment.  This will possibly result in a
// shortcoming of results if the local operand filters out many of the spooled
// results, so to compensate for that the query should have a larger partition limit.
proc populateLocalSegment(partitionQueries: [0..#numLocales] Query): Segment {
  var segment = new NaiveMemorySegment();

  for loc in Locales {
    if (partitionQueries[loc.id].instructionBuffer == nil) then continue;

    on loc {
      // copy query into locale
      var otherIB = partitionQueries[here.id].instructionBuffer;
      var instructionBuffer = new InstructionBuffer(otherIB.count);
      instructionBuffer.buffer = otherIB.buffer;
      var lq = new Query(instructionBuffer, partitionQueries[here.id].partitionLimit);

      var innerResults: [0..#lq.partitionLimit] Triple;
      var innerCount = 0;

      local {
        for res in Partitions[here.id].query(lq) {
          innerResults[innerCount] = res.triple;
          innerCount += 1;
          if (innerCount >= lq.partitionLimit) then break;
        }
      }

      if (innerCount > 0) {
        on segment {
          segment.addTriples(innerResults[0..#innerCount]);
        }
      }
    }
  }

  return segment;
}

// serial iterator
iter query(query: Query) {
  var partitionQueries: [0..#numLocales] Query;
  for p in partitionQueries do p = query;
  for r in queryWithPartitionQueries(partitionQueries, query) do yield r;
}

iter queryWithPartitionQueries(partitionQueries: [0..#numLocales] Query, topQuery: Query) {
  var segment = populateLocalSegment(partitionQueries);
  for t in segment.query(topQuery) do yield t;
  delete segment;
}

proc printTriples(q: Query) {
  for result in query(q) {
    writeln(result.triple);
  }
}

proc printPartitionQueryTriples(partitionQueries: [0..#numLocales] Query, topQuery: Query) {
  for result in queryWithPartitionQueries(partitionQueries, topQuery) {
    writeln(result.triple);
  }
}

proc createTripleVerificationArray(sRange, pRange, oRange, defaultValue = true) {
  var triples: [sRange, pRange, oRange] bool;
  for s in sRange {
    for p in pRange {
      for o in oRange {
        triples[s,p,o] = defaultValue;
      }
    }
  }
  return triples;
}

proc verifyPopulatedTriples(triples) {
  var failed = false;
  for (s,p,o) in triples.domain {
    if (triples[s,p,o]) {
      writeln(" (", s, " ", p, " ", o, ") was not verified.");
      failed = true;
    }
  }
  if (failed) then halt("found triples which were not verified.");
}

proc verifyTriples(sRange, pRange, oRange, q: Query) {
  verifyTriplesWithArray(createTripleVerificationArray(sRange, pRange, oRange), q);
}

proc verifyTriplesWithArray(triples, q: Query) {

  const sRange = triples.domain.dim(1);
  const pRange = triples.domain.dim(2);
  const oRange = triples.domain.dim(3);

  for result in query(q) {
    var t = result.triple;
    if (verify_print) then writeln(t);

    if (t.subject < sRange.low && t.subject > sRange.high) then halt("t.subject < sRange.low && t.subject > sRange.high");
    if (t.predicate < pRange.low && t.predicate > pRange.high) then halt("t.predicate < pRange.low && t.predicate > pRange.high");
    if (t.object < oRange.low && t.object > oRange.high) then halt("t.object < oRange.low && t.object > oRange.high");

    if (!triples[t.subject, t.predicate, t.object]) then halt("tuple not found: ", t);

    // mark the tuple as touched
    triples[t.subject, t.predicate, t.object] = false;
  }

  verifyPopulatedTriples(triples);
}

proc verifyPartitionQueryTriplesWithArray(triples, partitionQueries: [0..#numLocales] Query, topQuery: Query) {

  const sRange = triples.domain.dim(1);
  const pRange = triples.domain.dim(2);
  const oRange = triples.domain.dim(3);

  for result in queryWithPartitionQueries(partitionQueries, topQuery) {
    var t = result.triple;
    if (verify_print) then writeln(t);

    if (t.subject < sRange.low && t.subject > sRange.high) then halt("t.subject < sRange.low && t.subject > sRange.high");
    if (t.predicate < pRange.low && t.predicate > pRange.high) then halt("t.predicate < pRange.low && t.predicate > pRange.high");
    if (t.object < oRange.low && t.object > oRange.high) then halt("t.object < oRange.low && t.object > oRange.high");

    if (!triples[t.subject, t.predicate, t.object]) then halt("tuple not found: ", t);

    // mark the tuple as touched
    triples[t.subject, t.predicate, t.object] = false;
  }

  verifyPopulatedTriples(triples);
}

proc verifyOperand(sRange, pRange, oRange, op: Operand) {
  var triples: [sRange, pRange, oRange] bool;
  for s in sRange {
    for p in pRange {
      for o in oRange {
        triples[s,p,o] = true;
      }
    }
  }

  for t in op.evaluate() {
    if (t.subject < sRange.low && t.subject > sRange.high) then halt("t.subject < sRange.low && t.subject > sRange.high");
    if (t.predicate < pRange.low && t.predicate > pRange.high) then halt("t.predicate < pRange.low && t.predicate > pRange.high");
    if (t.object < oRange.low && t.object > oRange.high) then halt("t.object < oRange.low && t.object > oRange.high");

    if (!triples[t.subject, t.predicate, t.object]) then halt("tuple not found: ", t);

    // mark the tuple as touched
    triples[t.subject, t.predicate, t.object] = false;
  }

  var failed = false;
  for s in sRange {
    for p in pRange {
      for o in oRange {
        if (triples[s,p,o]) {
          writeln(" (", s, " ", p, " ", o, ") was not verified.");
          failed = true;
        }
      }
    }
  }
  if (failed) then halt("found triples which were not verified.");
}

proc testSimpleQueries() {
  var q = new Query(new InstructionBuffer());
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
    q.getWriter().writeScanPredicate(1, 1, 1, 2, 1, 3);
    writeln("triples of (1,2,3)");
    verifyTriples(1..1, 2..2, 3..3, q);
  }

  {
    q.instructionBuffer.clear();
    q.getWriter().writeScanPredicate(1, 1, 1, 2, 2, 3, 4);
    writeln("triples of (1,2,[3,4])");
    verifyTriples(1..1, 2..2, 3..4, q);
  }
  {
    q.instructionBuffer.clear();
    q.getWriter().writeScanPredicate(1, 1, 0, 2, 3, 4);
    writeln("scan all triples of the form (1,*,[3,4])");
    verifyTriples(1..1, 0..#predicateCount, 3..4, q);
  }
  {
    q.instructionBuffer.clear();
    q.getWriter().writeScanPredicate(0, 1, 2, 2, 3, 4);
    writeln("scan all triples of the form (*,2,[3,4])");
    verifyTriples(0..#subjectCount, 2..2, 3..4, q);
  }
  {
    q.instructionBuffer.clear();
    q.getWriter().writeScanPredicate(1, 1, 1, 2, 0);
    writeln("scan all triples of the form (1,2,*])");
    verifyTriples(1..1, 2..2, 0..#objectCount, q);
  }
  {
    q.instructionBuffer.clear();
    q.getWriter().writeScanPredicate(0, 0, 0);
    writeln("scan all triples");
    verifyTriples(0..#subjectCount, 0..#predicateCount, 0..#objectCount, q);
  }
  {
    q.instructionBuffer.clear();
    var w = q.getWriter();
    // A = (*,2,3)
    /*
    (subject = 1, predicate = 2, object = 3)
    (subject = 2, predicate = 2, object = 3)
    (subject = 3, predicate = 2, object = 3)
    (subject = 4, predicate = 2, object = 3)
    */
    w.writeScanPredicate(4, 1, 2, 3, 4, 1, 2, 1, 3);

    // B = ([2,3],3,4)
    /*
    (subject = 2, predicate = 2, object = 4)
    (subject = 3, predicate = 2, object = 4)
    */
    w.writeScanPredicate(2, 2, 3, 1, 2, 1, 4);

    // A.object OR B.object
    /*
    (subject = 2, predicate = 2, object = 4)
    (subject = 3, predicate = 2, object = 4)
    (subject = 1, predicate = 2, object = 3)
    (subject = 2, predicate = 2, object = 3)
    (subject = 3, predicate = 2, object = 3)
    (subject = 4, predicate = 2, object = 3)
    */
    w.writeOr();
    w.writeSPOMode(OperandSPOModeSubject);

    writeln("union triple subjects of the form (1..4, 2..2, 3..4)");
    var triples = createTripleVerificationArray(1..4, 2..2, 3..4);
    triples[1,2,4] = false;
    triples[4,2,4] = false;
    verifyTriplesWithArray(triples, q);
  }
  {
    q.instructionBuffer.clear();
    var w = new InstructionWriter(q.instructionBuffer);
    // A = (*,2,3)
    /*
    (subject = 1, predicate = 2, object = 3)
    (subject = 2, predicate = 2, object = 3)
    (subject = 3, predicate = 2, object = 3)
    (subject = 4, predicate = 2, object = 3)
    */
    w.writeScanPredicate(4, 1, 2, 3, 4, 1, 2, 1, 3);

    // B = ([2,3],3,4)
    /*
    (subject = 2, predicate = 2, object = 4)
    (subject = 3, predicate = 2, object = 4)
    */
    w.writeScanPredicate(2, 2, 3, 1, 2, 1, 4);

    // A.object AND B.object
    /*
    (subject = 2, predicate = 2, object = 4)
    (subject = 2, predicate = 2, object = 3)
    (subject = 3, predicate = 2, object = 4)
    (subject = 3, predicate = 2, object = 3)
    */
    w.writeAnd();
    w.writeSPOMode(OperandSPOModeSubject);

    writeln("intersect triple subjects of the form (2..3, 2..2, 3..4)");
    verifyTriples(2..3, 2..2, 3..4, q);
  }
  {
    q.instructionBuffer.clear();
    var w = new InstructionWriter(q.instructionBuffer);
    // A = (1,2,1..4)
    /*
    (subject = 1, predicate = 2, object = 1)
    (subject = 1, predicate = 2, object = 2)
    (subject = 1, predicate = 2, object = 3)
    (subject = 1, predicate = 2, object = 4)
    */
    w.writeScanPredicate(1, 1, 1, 2, 4, 1, 2, 3, 4);

    // B = (2,3,[2,3])
    /*
    (subject = 2, predicate = 2, object = 2)
    (subject = 2, predicate = 2, object = 3)
    */
    w.writeScanPredicate(1, 2, 1, 2, 2, 2, 3);

    // A.object AND B.object
    /*
    (subject = 2, predicate = 2, object = 2)
    (subject = 1, predicate = 2, object = 2)
    (subject = 2, predicate = 2, object = 3)
    (subject = 1, predicate = 2, object = 3)
    */
    w.writeAnd();
    w.writeSPOMode(OperandSPOModeObject);

    writeln("intersect triple objects of the form (1..2, 2..2, 2..3)");
    verifyTriples(1..2, 2..2, 2..3, q);
  }
}

proc testComplexQueries() {
  {
    var partitionQuries: [0..#numLocales] Query;
    partitionQuries[0] = new Query(new InstructionBuffer());
    partitionQuries[0].getWriter().writeScanPredicate(1, 1, 1, 2, 1, 3);
    partitionQuries[1] = new Query(new InstructionBuffer());
    partitionQuries[1].getWriter().writeScanPredicate(1, 1, 1, 3, 1, 4);

    var topQuery = new Query(new InstructionBuffer());
    var w = topQuery.getWriter();
    w.writeScanPredicate(0,1,2,0);
    w.writeScanPredicate(0,1,3,0);
    w.writeAndWithMode(OperandSPOModeSubject);

    writeln("intersect triple objects of the form [1,2,3] Subject AND [1,3,4]");
    var triples = createTripleVerificationArray(1..1, 2..3, 3..4);
    triples[1,2,4] = false;
    triples[1,3,3] = false;
    verifyPartitionQueryTriplesWithArray(triples, partitionQuries, topQuery);
  }

  {
    var partitionQuries: [0..#numLocales] Query;
    partitionQuries[0] = new Query(new InstructionBuffer());
    partitionQuries[0].getWriter().writeScanPredicate(1, 1, 1, 2, 1, 3);
    partitionQuries[1] = new Query(new InstructionBuffer());
    partitionQuries[1].getWriter().writeScanPredicate(1, 2, 1, 3, 1, 4);

    var topQuery = new Query(new InstructionBuffer());
    var w = topQuery.getWriter();
    w.writeScanPredicate(0,1,2,0);
    w.writeScanPredicate(0,1,3,1,4);
    w.writeOrWithMode(OperandSPOModeSubject);

    writeln("intersect triple objects of the form [*,2,*] Subject OR [*,3,4]");
    var triples = createTripleVerificationArray(1..2, 2..3, 3..4);
    /*triples[1,2,3] = false;*/
    triples[1,2,4] = false;
    triples[2,2,3] = false;
    triples[2,2,4] = false;
    triples[1,3,3] = false;
    triples[1,3,4] = false;
    triples[2,3,3] = false;
    /*triples[2,3,4] = false;*/
    verifyPartitionQueryTriplesWithArray(triples, partitionQuries, topQuery);
  }

  {
    var partitionQuries: [0..#numLocales] Query;
    partitionQuries[0] = new Query(new InstructionBuffer());
    partitionQuries[0].getWriter().writeScanPredicate(1, 2, 1, 2, 1, 4);
    partitionQuries[1] = new Query(new InstructionBuffer());
    partitionQuries[1].getWriter().writeScanPredicate(1, 3, 1, 3, 2, 3, 4);

    var topQuery = new Query(new InstructionBuffer());
    var w = topQuery.getWriter();
    w.writeScanPredicate(0,1,2,0);
    w.writeScanPredicate(0,1,3,1,4);
    w.writeAndWithMode(OperandSPOModeObject);

    writeln("intersect triple objects of the form [*,2,*] Object AND [*,3,4]");
    var triples = createTripleVerificationArray(2..3, 2..3, 3..4);
    triples[2,2,3] = false;
    /*triples[2,2,4] = false;*/
    triples[2,3,3] = false;
    triples[2,3,4] = false;
    triples[3,2,3] = false;
    triples[3,2,4] = false;
    triples[3,3,3] = false;
    /*triples[3,3,4] = false;*/
    verifyPartitionQueryTriplesWithArray(triples, partitionQuries, topQuery);
  }

  {
    var partitionQuries: [0..#numLocales] Query;
    partitionQuries[0] = new Query(new InstructionBuffer());
    partitionQuries[0].getWriter().writeScanPredicate(1, 1, 1, 2, 1, 3);
    partitionQuries[1] = new Query(new InstructionBuffer());
    partitionQuries[1].getWriter().writeScanPredicate(1, 2, 1, 3, 2, 3, 4);

    var topQuery = new Query(new InstructionBuffer());
    var w = topQuery.getWriter();
    w.writeScanPredicate(0,1,2,0);
    w.writeScanPredicate(0,1,3,1,4);
    w.writeOrWithMode(OperandSPOModeObject);

    writeln("intersect triple objects of the form [1,2,*] Object OR [1,3,4]");
    var triples = createTripleVerificationArray(1..2, 2..3, 3..4);
    /*triples[1,2,3] = false;*/
    triples[1,2,4] = false;
    triples[2,2,3] = false;
    triples[2,2,4] = false;
    triples[1,3,3] = false;
    triples[1,3,4] = false;
    triples[2,3,3] = false;
    /*triples[2,3,4] = false;*/
    verifyPartitionQueryTriplesWithArray(triples, partitionQuries, topQuery);
  }
}

proc testPredicateEntryOperand() {
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

proc testChasm() {
  /*var expectedTriple = 10: Triple;

  var buffer = new InstructionBuffer(1024);

  var writer = new InstructionWriter(buffer);
  writer.write_push();
  writer.write_(expectedTerm);

  buffer.rewind();
  var reader = new InstructionReader(buffer);
  var op: ChasmOp;
  var term: Term;
  op = reader.read();
  if (op != CHASM_PUSH) then halt("opcode should have been CHASM_PUSH: ", " got ", op, " ", reader);
  term = reader.readTerm();
  if (term != expectedTerm) then halt("term should have been ", expectedTerm, " got ", term, " ", reader);

  delete buffer;*/
}

class FixedDataOperand : Operand {
  var count: uint;
  var data: [0..count-1] OperandValue;
  var offset: uint = 0;

  proc hasValue(): bool {
    return offset <= data.domain.high;
  }

  proc getValue(): OperandValue {
    if (!hasValue()) {
      halt("iterated too far");
    }
    return data[offset];
  }

  proc advance() {
    if (!hasValue()) {
      halt("iterated too far");
    }
    offset += 1;
  }
}

proc testOperands() {
  {
    writeln("start validating FixedDataOperand");
    var fixed = new FixedDataOperand(1);
    fixed.data[0] = new Triple(1,2,3);
    var count = 0;
    for result in fixed.evaluate() {
      if (result != fixed.data[0]) {
        halt("result not expected: ", result);
      }
      count += 1;
    }
    if (count != 1) {
      halt("count != 1 got ", count, fixed);
    }
    delete fixed;
    writeln("stop validating FixedDataOperand");
  }

  {
    writeln("start validating UnionOperand");
    var fixedA = new FixedDataOperand(1);
    fixedA.data[0] = new Triple(1,2,3);

    var fixedB = new FixedDataOperand(1);
    fixedB.data[0] = new Triple(2,3,4);

    var op = new UnionOperand(OperandSPOModeSubject, fixedA, fixedB);
    var count = 0;
    for result in op.evaluate() {
      if (result != fixedA.data[0] && result != fixedB.data[0]) {
        halt("result not expected: ", result);
      }
      count += 1;
    }
    if (count != 2) {
      halt("count != 2 got ", count, fixedA, fixedB);
    }
    delete op;
    writeln("stop validating UnionOperand");
  }

  {
    writeln("start validating IntersectionOperand");
    var fixedA = new FixedDataOperand(1);
    fixedA.data[0] = new Triple(1,2,3);

    var fixedB = new FixedDataOperand(2);
    fixedB.data[0] = new Triple(1,3,4);
    fixedB.data[1] = new Triple(4,5,6);

    var op = new IntersectionOperand(OperandSPOModeSubject, fixedA, fixedB);
    var count = 0;
    for result in op.evaluate() {
      if (result.subject != fixedA.data[0].subject && result.subject != fixedB.data[1].subject) {
        halt("result not expected: ", result.subject);
      }
      count += 1;
    }
    if (count != 2) {
      halt("count != 2 got ", count, fixedA, fixedB);
    }
    delete op;
    writeln("stop validating UnionOperand");
  }
}

proc dump() {
  for loc in Locales do on loc do for triple in Partitions[here.id].dump() do writeln(triple);
}

proc extractGraph(startSubject: EntityId) {
  var encounteredEntities: [0..#objectCount] bool;
  var entityStack: [0..#objectCount] int;
  var stackPos = 0;

  var q = new Query(new InstructionBuffer());
  entityStack[stackPos] = startSubject;
  stackPos += 1;

  while (stackPos > 0) {
    stackPos -= 1;
    var entityId = entityStack[stackPos];
    q.instructionBuffer.clear();
    q.getWriter().writeScanPredicate(1, entityId, 0, 0);
    for r in query(q) {
      var t = r.triple;
      if !encounteredEntities[t.object] {
        encounteredEntities[t.object] = true;
        entityStack[stackPos] = t.object;
        stackPos += 1;
      }
    }
  }

  for idx in 0..#objectCount {
    if (encounteredEntities[idx]) {
      q.instructionBuffer.clear();
      q.getWriter().writeScanPredicate(1, idx, 0, 0);
      for r in query(q) do writeln(r);
    }
  }
}

proc testGraphExtraction() {
  resetPartitions();
  addTriple(new Triple(1, 2, 3));
  addTriple(new Triple(1, 2, 4));
  addTriple(new Triple(3, 2, 1));
  addTriple(new Triple(4, 3, 1));
  addTriple(new Triple(5, 3, 2));
  addTriple(new Triple(2, 4, 5));

  writeln("extracting graph starting with 1");
  extractGraph(1);
  writeln("extracting graph starting with 5");
  extractGraph(5);
}

iter readGraphFile(fileName) {
  var f = open(fileName, iomode.r);
  var r = f.reader();
  var numLines: int;

  var subject: EntityId;
  var predicate: PredicateId;
  var object: EntityId;

  // only test the read of the subject and assume predicate and object are there
  while r.read(subject) {
    r.read(predicate);
    r.read(object);
    yield new Triple(subject, predicate, object);
  }

  r.close();
  f.close();
}

proc testReadGraphFile() {
  resetPartitions();
  var triples = createTripleVerificationArray(1..3, 1..3, 1..3);
  for t in readGraphFile("data/graph.txt") do triples[t.subject, t.predicate, t.object] = false;
  verifyPopulatedTriples(triples);
}

proc main() {
  startVdebug("network");

  writeln("testTriple:");
  testTriple();

  writeln("testChasm");
  testChasm();
  testOperands();

  writeln("testOperand:");
  PredicateEntryOperand();

  writeln("testPredicateEntry");
  testPredicateEntry();

  writeln("test queries");
  initPartitions();
  addSyntheticData();
  testSimpleQueries();
  testComplexQueries();

  writeln("test graph extraction");
  testGraphExtraction();

  writeln("test read graph file");
  testReadGraphFile();

  stopVdebug();
}
