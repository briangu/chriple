module Common {

  type EntityId = uint(32);
  type EntityPair = uint(64);
  type PredicateId = uint(16);

  record Triple {
    var subject: EntityId;
    var predicate: PredicateId;
    var object: EntityId;
  }
}
