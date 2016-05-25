/*

  Triple Naive is a naive implementation of a triple store using predicate based hash partitions
  and in-memory SO and OS indexes on each partition.  The strategy is naive in the sense that it
  does not make an effort to be super efficient with storage.

*/
use Chasm, Common, GenHashKey32, Logging, Operand, PrivateDist, Random, Query, Segment, Time, Verify, VisualDebug;

config const subjectCount = 16;
config const predicateCount = 8;
config const objectCount = 16;
config const randomSeed = 17;
config const graphDensity = 20; // 20%

const totalTripleCount = subjectCount * predicateCount * objectCount;
const sRange = 0..#subjectCount;
const pRange = 0..#predicateCount;
const oRange = 0..#objectCount;
const soCount = subjectCount * objectCount;

var next_random: [PrivateSpace] uint(64);
forall n in next_random do n = randomSeed: uint(64);

proc nextRandom() {
  next_random[here.id] = next_random[here.id] * 25214903917:uint(64) + 11;
  return next_random[here.id];
}

proc addTriple(triple: Triple) {
  var partitionId = partitionIdForTriple(triple);
  on Locales[partitionId] do Partitions[here.id].addTriple(triple);
}

proc addPredicateTriples(predicate: PredicateId, triples: [?D] Triple) {
  var partitionId = partitionIdForPredicate(predicate);
  on Locales[partitionId] do Partitions[here.id].addTriples(triples);
}

proc createRandomGraph() {
  /*startVdebug("add_triple");*/
  for p in 0..#predicateCount {
    /*writeln("adding predicate: ", p);*/
    var partitionId = partitionIdForPredicate(p:PredicateId);
    on Locales[partitionId] {
      var count = 0;
      var triples: [0..#soCount] Triple;
      local {
        for s in 0..#subjectCount {
          for o in 0..#objectCount {
            if (nextRandom() % 100 <= graphDensity) {
              count += 1;
              triples[s*objectCount + o] = new Triple(s:EntityId, p:PredicateId, o:EntityId);
            }
          }
        }
      }
      /*writeln(count);*/
      addPredicateTriples(p: PredicateId, triples[0..#count]);
    }
  }
  /*stopVdebug();*/
}

proc main() {
  initPartitions();
  var timer: Timer;
  timer.start();
  createRandomGraph();
  timer.stop();
  writeln("create time: ", timer.elapsed(), " seconds");

  /*var q = new Query(new InstructionBuffer());
  q.partitionLimit = 1024*1024*1024;
  q.getWriter().writeScanPredicate(0,0,0);

  writeln(countTriples(q));*/
  timer.clear();
  timer.start();
  var count = countAllTriples();
  timer.stop();
  writeln("total triples added: ", count);
  writeln("query time: ", timer.elapsed(), " seconds");
}
