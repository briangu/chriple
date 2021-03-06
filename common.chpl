module Common {

  type EntityId = uint(32);
  type EntityPair = uint(64);
  type PredicateId = EntityId;

  record Triple {
    var subject: EntityId;
    var predicate: PredicateId;
    var object: EntityId;

    proc toSOPair(): EntityPair {
      return (subject: EntityPair << 32) | object;
    }

    proc toOSPair(): EntityPair {
      return (object: EntityPair << 32) | subject;
    }
  }

  inline proc toTriple(soPair: EntityPair, predicate: PredicateId): Triple {
    return new Triple((soPair >> 32):EntityId, predicate, soPair:EntityId);
  }

  inline proc toTripleFromOSEntry(osPair: EntityPair, predicate: PredicateId): Triple {
    return new Triple(osPair: EntityId, predicate, (osPair >> 32):EntityId);
  }

  proc testTriple() {
    var triple = new Triple(1,2,3);
    var soPair = triple.toSOPair();
    var osPair = triple.toOSPair();
    if (triple.toSOPair() != (1: EntityPair << 32 | 3)) then halt("triple.toSOPair() != (1: EntityPair << 32 | 3)");
    if (triple.toOSPair() != (3: EntityPair << 32 | 1)) then halt("triple.toSOPair() != (1: EntityPair << 32 | 3)");
  }
}
