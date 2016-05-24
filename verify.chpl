module Verify {

  use Query;

  config const verify_print = false;

  proc createTripleVerificationArray(sRange, pRange, oRange, defaultValue = true) {
    var triples: [sRange, pRange, oRange] bool;
    for s in sRange {
      for p in pRange {
        for o in oRange {
          triples[s,p,o] = defaultValue;
        }
      }
    }
    return triples;
  }

  proc verifyPopulatedTriples(triples) {
    var failed = false;
    for (s,p,o) in triples.domain {
      if (triples[s,p,o]) {
        writeln(" (", s, " ", p, " ", o, ") was not verified.");
        failed = true;
      }
    }
    if (failed) then halt("found triples which were not verified.");
  }

  proc verifyTriples(sRange, pRange, oRange, q: Query) {
    verifyTriplesWithArray(createTripleVerificationArray(sRange, pRange, oRange), q);
  }

  proc verifyTriplesWithArray(triples, q: Query) {

    const sRange = triples.domain.dim(1);
    const pRange = triples.domain.dim(2);
    const oRange = triples.domain.dim(3);

    for result in query(q) {
      var t = result.triple;
      if (verify_print) then writeln(t);

      if (t.subject < sRange.low && t.subject > sRange.high) then halt("t.subject < sRange.low && t.subject > sRange.high");
      if (t.predicate < pRange.low && t.predicate > pRange.high) then halt("t.predicate < pRange.low && t.predicate > pRange.high");
      if (t.object < oRange.low && t.object > oRange.high) then halt("t.object < oRange.low && t.object > oRange.high");

      if (!triples[t.subject, t.predicate, t.object]) then halt("tuple not found: ", t);

      // mark the tuple as touched
      triples[t.subject, t.predicate, t.object] = false;
    }

    verifyPopulatedTriples(triples);
  }

  proc verifyPartitionQueryTriplesWithArray(triples, partitionQueries: [0..#numLocales] Query, topQuery: Query) {

    const sRange = triples.domain.dim(1);
    const pRange = triples.domain.dim(2);
    const oRange = triples.domain.dim(3);

    for result in queryWithPartitionQueries(partitionQueries, topQuery) {
      var t = result.triple;
      if (verify_print) then writeln(t);

      if (t.subject < sRange.low && t.subject > sRange.high) then halt("t.subject < sRange.low && t.subject > sRange.high");
      if (t.predicate < pRange.low && t.predicate > pRange.high) then halt("t.predicate < pRange.low && t.predicate > pRange.high");
      if (t.object < oRange.low && t.object > oRange.high) then halt("t.object < oRange.low && t.object > oRange.high");

      if (!triples[t.subject, t.predicate, t.object]) then halt("tuple not found: ", t);

      // mark the tuple as touched
      triples[t.subject, t.predicate, t.object] = false;
    }

    verifyPopulatedTriples(triples);
  }

  proc verifyOperand(sRange, pRange, oRange, op: Operand) {
    var triples: [sRange, pRange, oRange] bool;
    for s in sRange {
      for p in pRange {
        for o in oRange {
          triples[s,p,o] = true;
        }
      }
    }

    for t in op.evaluate() {
      if (t.subject < sRange.low && t.subject > sRange.high) then halt("t.subject < sRange.low && t.subject > sRange.high");
      if (t.predicate < pRange.low && t.predicate > pRange.high) then halt("t.predicate < pRange.low && t.predicate > pRange.high");
      if (t.object < oRange.low && t.object > oRange.high) then halt("t.object < oRange.low && t.object > oRange.high");

      if (!triples[t.subject, t.predicate, t.object]) then halt("tuple not found: ", t);

      // mark the tuple as touched
      triples[t.subject, t.predicate, t.object] = false;
    }

    var failed = false;
    for s in sRange {
      for p in pRange {
        for o in oRange {
          if (triples[s,p,o]) {
            writeln(" (", s, " ", p, " ", o, ") was not verified.");
            failed = true;
          }
        }
      }
    }
    if (failed) then halt("found triples which were not verified.");
  }
}
