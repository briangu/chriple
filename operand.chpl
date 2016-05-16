module Operand {

  use Common;

  type OperandValue = Triple;
  type OperandSPOMode = uint(8);

  const OperandSPOModeSubject = 1: OperandSPOMode;
  const OperandSPOModeObject = 2: OperandSPOMode;

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
    if mode == OperandSPOModeObject then return op.getValue().object;
    halt("unsupported mode ", mode);
  }

  class UnionOperand : Operand {
    var mode: OperandSPOMode;
    var opA: Operand;
    var opB: Operand;
    var curOp: Operand;

    proc init() {
      if (curOp != nil) then return;

      opA.init();
      opB.init();
      curOp = nextOperand();
    }

    proc cleanup() {
      if (opA == nil) then return;

      opA.cleanup();
      opA = nil;
      opB.cleanup();
      opB = nil;
      curOp = nil;
    }

    proc nextOperand(): Operand {
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

      return op;
    }

    inline proc hasValue(): bool {
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
      if (curOp != nil) then return;

      opA.init();
      opB.init();
      curOp = nextOperand();
    }

    proc cleanup() {
      if (opA == nil) then return;

      opA.cleanup();
      opA = nil;
      opB.cleanup();
      opB = nil;
      curOp = nil;
    }

    proc nextOperand(): Operand {
      var op: Operand = nil;
      var firstAdvance = true;

      while(opA.hasValue() && opB.hasValue()) {
        var docIndexA = tripleComponentFromOperand(mode, opA);
        var docIndexB = tripleComponentFromOperand(mode, opB);

        if (docIndexA > docIndexB) {
          opB.advance();
          firstAdvance = false;
        } else if (docIndexA == docIndexB) {
          if (curOp != nil) {
            if (firstAdvance) then curOp.advance();
            op = if curOp == opA then opB else opA;
          } else {
            op = opA;
          }
          break;
        } else { // A < B
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

      return curOp.getValue();
    }

    inline proc advance() {
      assert(hasValue());

      curOp = nextOperand();
    }
  }
}
