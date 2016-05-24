/*

  Triple Naive is a naive implementation of a triple store using predicate based hash partitions
  and in-memory SO and OS indexes on each partition.  The strategy is naive in the sense that it
  does not make an effort to be super efficient with storage.

*/
use Chasm, Common, GenHashKey32, Logging, Operand, PrivateDist, Query, Segment, Time, Verify, VisualDebug;

config const subjectCount = 16;
config const predicateCount = 8;
config const objectCount = 16;
const totalTripleCount = subjectCount * predicateCount * objectCount;
const sRange = 0..#subjectCount;
const pRange = 0..#predicateCount;
const oRange = 0..#objectCount;
const soCount = subjectCount * objectCount;

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

proc main() {
}
