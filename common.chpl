module Common {

  type EntityId = uint(32);
  type EntityPair = uint(64);
  type PredicateId = uint(16);

  record Triple {
    var subject: EntityId;
    var predicate: PredicateId;
    var object: EntityId;

    proc toSOPair() {
      var pair: EntityPair = subject;
      return (pair << 32) | object;
    }

    proc toOSPair() {
      var pair: EntityPair = object;
      return (pair << 32) | subject;
    }
  }
}
