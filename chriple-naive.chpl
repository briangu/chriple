/*

  Triple Naive is a naive implementation of a triple store using predicate based hash partitions
  and in-memory SO and OS indexes on each partition.  The strategy is naive in the sense that it
  does not make an effort to be super efficient with storage.

*/
use Chasm, Common, GenHashKey32, Logging, Operand, PrivateDist, Segment, Time;

proc initPartitions() {
  var t: Timer;
  t.start();

  for loc in Locales do on loc do local {
    Partitions[here.id] = new PartitionManager(new MemorySegment());
    NullOperand[here.id] = new Operand();
  }

  t.stop();
  timing("initialized partitions in ",t.elapsed(TimeUnits.microseconds), " microseconds");
}

proc partitionIdForTriple(triple: Triple): int {
  return genHashKey32(triple.predicate) % numLocales;
}

proc addTriple(triple: Triple) {
  var partitionId = partitionIdForTriple(triple);
  on Partitions[partitionId] do local {
    // TODO: is triple local now?
    Partitions[here.id].addTriple(triple);
  }
}

proc addSyntheticData() {

}

proc querySyntheticData() {

}

proc main() {
  initPartitions();
  addSyntheticData();
  querySyntheticData();
}
