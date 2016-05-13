
use Operand, Segment;

config const subjectCount = 16;
config const predicateCount = 8;
config const objectCount = 16;
const totalTripleCount = subjectCount * predicateCount * objectCount;
const sRange = 0..#subjectCount;
const pRange = 0..#predicateCount;
const oRange = 0..#objectCount;

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
for p in pRange {
  for s in sRange {
    for o in oRange {
      var idx = p * (predicateCount*objectCount) + s * objectCount + o;
      tarr[idx].subject = s: EntityId;
      tarr[idx].predicate = p: PredicateId;
      tarr[idx].object = o: EntityId;
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
var entry = new PredicateEntry(1, soCount);
for t in tarr[0..#soCount] do entry.add(t);
entry.optimize();
/*writeln("soEntries");
for x in entry.soEntries do writef("%016xu\n", x);
writeln("osEntries");
for x in entry.osEntries do writef("%016xu\n", x);*/
