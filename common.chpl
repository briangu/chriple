module Common {

  type EntityId = uint(32);
  type PredicateId = uint(16);

  record Triple {
    var Subject: EntityId;
    var Predicate: PredicateId;
    var Object: EntityId;
  }
}
