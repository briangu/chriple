module Query {

  use Chasm, Common;

  record QueryResult {
    var triple: Triple;
  }

  record Query {
    var instructionBuffer: InstructionBuffer;
    var partitionLimit: int = 2048;

    // For some reason, creating a copy construtor fails to work correctly on multiple locales
    /*proc Query(query: Query) {
      var otherIB = query.instructionBuffer;
      instructionBuffer = new InstructionBuffer(otherIB.count);
      instructionBuffer.buffer = otherIB.buffer;
      partitionLimit = query.partitionLimit;
    }*/

    proc getWriter(): InstructionWriter {
      return new InstructionWriter(instructionBuffer);
    }

    proc getReader(): InstructionReader {
      return new InstructionReader(instructionBuffer);
    }
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

    // TODO: parallelize scatter gather
    //        requires making addTriples thread safe
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

  iter pdump() {
    halt("serial iteration not supported");
    yield new Triple(0,0,0);
  }

  iter pdump(param tag: iterKind)
    where tag == iterKind.leader {

    coforall loc in Locales do on loc {
      var p = Partitions[here.id];
      local for t in p.dump() do yield t;
    }
  }

  iter pdump(param tag: iterKind, followThis)
    where tag == iterKind.follower && followThis.size == 1 {

    writeln("Follower received ", followThis, " as work chunk; shifting to ");

    for i in followThis(1) do yield i;
  }

  iter pdump(param tag: iterKind)
    where tag == iterKind.standalone {

    coforall loc in Locales do on loc {
      var p = Partitions[here.id];
      local for t in p.dump() do yield t;
    }
  }

  proc printTriples(q: Query) {
    for result in query(q) {
      writeln(result.triple);
    }
  }

  class Counter {
    var count: atomic uint;
  }

  proc countAllTriples() {
    var counter: Counter = new Counter();
    forall r in pdump() do counter.count.add(1);
    return counter.count.read();
  }

  proc countTriples(q: Query) {
    var count = 0;
    for result in query(q) do count += 1;
    return count;
  }

  proc printPartitionQueryTriples(partitionQueries: [0..#numLocales] Query, topQuery: Query) {
    for result in queryWithPartitionQueries(partitionQueries, topQuery) {
      writeln(result.triple);
    }
  }
}
