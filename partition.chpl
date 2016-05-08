module Partition {
  use Common, GenHashKey32, Logging, Operand, PrivateDist, Segment, Time;

  var Partitions: [PrivateSpace] PartitionManager;

  class PartitionManager {
    var segment: Segment;

    proc addTriple(triple: Triple): bool {
      return segment.addTriple(triple);
    }

    proc addTriples(triples: [?D] Triple): bool {
      return segment.addTriples(triples);
    }

    iter query(query: Query): QueryResult {
      for opValue in segment.query(query) do yield opValue;
    }

    iter dump(): Triple {
      for triple in segment.dump() do yield triple;
    }
  }
}
