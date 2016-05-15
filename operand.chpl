module Operand {

  use Common, Logging, PrivateDist;

  /**
    Operand value is the value that represents the run-time, internal search results.
    After the query processing is complete, it is converted to a QueryResult which has
    the resolved external document id instead of the internal document index.

    An operand value is a partitioned uint(64) value that contains the following fields:

    | term id (32-bit) | DocId (32-bit) |

    which expands to

    | term id (32-bit) | text location (8-bit) | document index (24-bit) |

    Note that we keep the document index on the LSB side so that we can mask it out
    and use it for document equivalent comparision between Operand values.

  */
  type OperandValue = Triple;
  type OperandSPOMode = uint(8);

  /*const OperandSPOModeTriple = 0: OperandSPOMode;*/
  const OperandSPOModeSubject = 1: OperandSPOMode;
  const OperandSPOModePredicate = 2: OperandSPOMode;
  const OperandSPOModeObject = 3: OperandSPOMode;

  // Operand base class.  Also serves as Null / empty Operand
  // TODO: convert Operands to be proper Chapel iterators so we can iterate through the AST in parallel
  class Operand {
    proc init() {}
    proc cleanup() {}

    inline proc hasValue(): bool {
      return false;
    }

    proc getValue(): OperandValue {
      if (!hasValue()) {
        halt("iterated too far");
      }
      return new Triple(0,0,0);
    }

    proc advance() {
      if (!hasValue()) {
        halt("iterated too far");
      }
    }

    iter evaluate() {
      /*info("evaluate ");*/
      init();
      while (hasValue()) {
        yield getValue();
        advance();
      }
      cleanup();
    }
  }

  inline proc tripleComponentFromOperand(mode: OperandSPOMode, op: Operand): EntityId {
    if mode == OperandSPOModeSubject then return op.getValue().subject;
    if mode == OperandSPOModePredicate then return op.getValue().predicate;
    if mode == OperandSPOModeObject then return op.getValue().object;
    halt("unsupported mode ", mode);
  }

  class UnionOperand : Operand {
    var mode: OperandSPOMode;
    var opA: Operand;
    var opB: Operand;
    var curOp: Operand;

    proc init() {
      /*info("UnionOperand::init");*/
      if (curOp != nil) then return;

      opA.init();
      opB.init();
      curOp = nextOperand();
      /*info("UnionOperand:curOp ", curOp);*/
    }

    proc cleanup() {
      /*info("UnionOperand::cleanup");*/
      if (opA == nil) then return;

      opA.cleanup();
      opA = nil;
      opB.cleanup();
      opB = nil;
      curOp = nil;
    }

    proc nextOperand(): Operand {
      /*info("UnionOperand::nextOperand");*/
      var op: Operand = nil;

      if (opA.hasValue() && opB.hasValue()) {
        var docIndexA = tripleComponentFromOperand(mode, opA);
        var docIndexB = tripleComponentFromOperand(mode, opB);

        if (docIndexA > docIndexB) {
          op = opA;
        } else if (docIndexA == docIndexB) {
          op = opA;
        } else {
          op = opB;
        }
      } else if (opA.hasValue()) {
        op = opA;
      } else if (opB.hasValue()) {
        op = opB;
      }

      /*info("UnionOperand::nextOperand ", op == nil);*/

      return op;
    }

    inline proc hasValue(): bool {
      /*info("UnionOperand::hasValue ", curOp == nil);*/
      assert(opA != nil);
      return curOp != nil;
    }

    inline proc getValue(): OperandValue {
      assert(hasValue());

      return curOp.getValue();
    }

    proc advance() {
      assert(hasValue());

      curOp.advance();
      curOp = nextOperand();
    }
  }

  class IntersectionOperand : Operand {
    var mode: OperandSPOMode;
    var opA: Operand;
    var opB: Operand;
    var curOp: Operand;

    proc init() {
      /*info("IntersectionOperand::init");*/
      if (curOp != nil) then return;

      opA.init();
      opB.init();
      curOp = nextOperand();
    }

    proc cleanup() {
      /*info("IntersectionOperand::cleanup");*/
      if (opA == nil) then return;

      opA.cleanup();
      opA = nil;
      opB.cleanup();
      opB = nil;
      curOp = nil;
    }

    proc nextOperand(): Operand {
      /*info("IntersectionOperand::nextOperand");*/
      var op: Operand = nil;
      var firstAdvance = true;

      while(opA.hasValue() && opB.hasValue()) {
        var docIndexA = tripleComponentFromOperand(mode, opA);
        var docIndexB = tripleComponentFromOperand(mode, opB);

        /*info(docIndexA, " ", docIndexB, " ", if curOp != nil then tripleComponentFromOperand(mode, curOp) else 0);*/

        if (docIndexA > docIndexB) {
          /*info("docIndexA > docIndexB");*/
          opB.advance();
          firstAdvance = false;
        } else if (docIndexA == docIndexB) {
          /*info("docIndexA == docIndexB");*/
          if (curOp != nil) {
            /*info("(curOp != nil) ", curOp == opA);*/
            if (firstAdvance) then curOp.advance();
            op = if curOp == opA then opB else opA;
          } else {
            /*info("op = opA");*/
            op = opA;
          }
          break;
        } else { // A < B
          /*info("opB.advance");*/
          opB.advance();
          firstAdvance = false;
        }
      }

      return op;
    }

    inline proc hasValue(): bool {
      assert(opA != nil);

      return curOp != nil;
    }

    inline proc getValue(): OperandValue {
      assert(hasValue());

      /*info(curOp == opA, " ", curOp.getValue());*/
      return curOp.getValue();
    }

    inline proc advance() {
      /*info("IntersectionOperand::advance");*/
      assert(hasValue());

      curOp = nextOperand();
    }
  }
}
