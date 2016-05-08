/*

  Triple Naive is a naive implementation of a triple store using predicate based hash partitions
  and in-memory SO and OS indexes on each partition.  The strategy is naive in the sense that it
  does not make an effort to be super efficient with storage.

*/
use Chasm, Common, GenHashKey32, Logging, Operand, Partition, PrivateDist, Segment, Time, VisualDebug;

config const subjectCount = 16;
config const predicateCount = 8;
config const objectCount = 16;

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
      var triples: [0..#(subjectCount * objectCount)] Triple;
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

proc dump() {
  for loc in Locales do on loc do for triple in Partitions[here.id].dump() do writeln(triple);
}

proc main() {
  writeln("starting tests");
  testTriple();
  writeln("ending tests");

  initPartitions();
  addSyntheticData();
  /*dump();*/
}
