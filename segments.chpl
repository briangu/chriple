module Segments {

  use Common, Operands, PrivateDist, Query;

  // Globally reusable Null / empty singleton operand
  var NullOperand: [PrivateSpace] Operand;

  class Segment {
    inline proc isSegmentFull(): bool {
      halt("not implemented");
      return true;
    }

    proc addTriple(triple: Triple): bool {
      halt("not implemented");
      return false;
    }

    iter query(query: Query): QueryResult {
      halt("not implemented");
      yield new QueryResult();
    }

    proc operandForTriple(triple: Triple): Operand {
      halt("not implemented");
      return NullOperand[here.id];
    }
  }

  class MemorySegment : Segment {
  }
}
