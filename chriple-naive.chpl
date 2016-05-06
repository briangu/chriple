/*

  Triple Naive is a naive implementation of a triple store using predicate based hash partitions
  and in-memory SO and OS indexes on each partition.  The strategy is naive in the sense that it
  does not make an effort to be super efficient with storage.

*/
use Chasm, Common, GenHashKey32, Logging, Operand, PrivateDist, Segment, Time;

var Partitions: [PrivateSpace] PartitionManager;

class PartitionManager {
  var segment: Segment;

  proc addTriple(triple: Triple): bool {
    // TODO: handle multiple segments
    var success = segment.addTriple(triple);
    if (!success) {
      // TODO: handle segmentFull scenario
    }
    return success;
  }

  iter query(query: Query): QueryResult {
    // TODO: handle multiple segments
    for opValue in segment.query(query) {
      yield opValue;
    }
  }
}

proc initPartitions() {
  var t: Timer;
  t.start();

  for loc in Locales do on loc do local {
    // TODO: handle multiple segments
    Partitions[here.id] = new PartitionManager(new MemorySegment());
    NullOperand[here.id] = new Operand();
  }

  t.stop();
  timing("initialized partitions in ",t.elapsed(TimeUnits.microseconds), " microseconds");
}

proc addTriple(triple: Triple) {
  var partitionId = partidionIdForTriple(triple);
  on Partitions[partitionId] do local {
    Partitions[here.id].addTriple(triple);
  }
}

proc addTriples(triples: [?D] Triple) {
  // TODO: batching
  for triple in triples do addTriple(triple);
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
