
use Operand, Segment;

config const subjectCount = 16;
config const predicateCount = 8;
config const objectCount = 16;
const totalTripleCount = subjectCount * predicateCount * objectCount;
const sRange = 0..#subjectCount;
const pRange = 0..#predicateCount;
const oRange = 0..#objectCount;

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
    /*if (verify_print) then writeln(t);*/

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

var q = new Query(new InstructionBuffer(2048));
q.instructionBuffer.clear();
var w = new InstructionWriter(q.instructionBuffer);
w.writeScanPredicate();
w.writeCount(1);
w.writeSubjectId(1);
w.writeCount(1);
w.writePredicateId(2);
w.writeCount(1);
w.writeObjectId(3);
w.writeHalt();


var soCount = subjectCount * objectCount;
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
