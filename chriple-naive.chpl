/*

  Triple Naive is a naive implementation of a triple store using predicate based hash partitions
  and in-memory SO and OS indexes on each partition.  The strategy is naive in the sense that it
  does not make an effort to be super efficient with storage.

*/
use Chasm, Common, GenHashKey32, Logging, Operand, Partition, PrivateDist, Query, Segment, Time, VisualDebug;

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

proc querySyntheticData() {
  var q = new Query(new InstructionBuffer(2048));
  var w = new InstructionWriter(q.instructionBuffer);
  // select * where s = 1 and p = 2 and o = 3
  //    --> on p, find all triples with s = 1 and o = 3 (which is at most 1)
  w.writeScanPredicate();
  // list of predicates to scan
  w.writeCount(1);
  w.writePredicateId(2);
  // list of subjects to scan
  w.writeCount(1);
  w.writeSubjectId(1);
  // list of objects to scan
  w.writeCount(1);
  w.writeObjectId(3);

  w.writeHalt();

  for result in query(q) {
    writeln(result);
  }
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
  querySyntheticData();
  /*dump();*/
}
