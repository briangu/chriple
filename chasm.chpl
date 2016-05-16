/**
  CHASM: (Chearch Assembly) is the Chearch query assembly language (that helps you cross the search query chasm.)


  Types of graph queries supported:

  Definitions:
    Predicate => P
    Subject => S
    Object => O
    Triple => T

  [select * where]

    S = X
    P = X
    O = X
    T = X
    S = X AND P = Y
    S = X AND O = Y
    P = X AND O = Y

      S P O
    S x 1 1
    P 1 x 1
    O 1 1 x

  Invalid queries:
    S = X AND S = Y [NOT ALLOWED as no triple will ever have two subjects]
    P = X AND P = Y [NOT ALLOWED as no triple will ever have two subjects]
    O = X AND O = Y [NOT ALLOWED as no triple will ever have two objects]

*/
module Chasm {

  use Common, Logging, Operand, Segment;

  type ChasmOp = uint(8); // CHASM opcode type

  const CHASM_HALT: ChasmOp = 0: ChasmOp; // HALT (0) is the default value in the instructions array
  const CHASM_SCAN_PREDICATE: ChasmOp = 1: ChasmOp;
  const CHASM_AND:  ChasmOp = 2: ChasmOp;
  const CHASM_OR:   ChasmOp = 3: ChasmOp;

  class InstructionBuffer {
    var count: uint;
    var buffer: [0..count-1] ChasmOp;
    var offset = 0: uint;

    inline proc atEnd(): bool {
      return (offset >= count);
    }

    inline proc rewind() {
      offset = 0;
    }

    inline proc clear() {
      buffer = 0;
      offset = 0;
    }

    inline proc advance() {
      offset += 1;
    }

    inline proc canAdvance(increment: uint): bool {
      return (offset + increment) <= count;
    }

    inline proc read(): ChasmOp {
      if (atEnd()) {
        error("extended past instructions array end.");
        return 0;
      }
      var op = buffer[offset];
      advance();
      return op;
    }

    inline proc write(op: ChasmOp): bool {
      if (atEnd()) {
        error("write is out of instruction space at offset ", offset, " for op code ", op);
        return false;
      }

      buffer[offset] = op;
      advance();

      return true;
    }
  }

  record InstructionReader {
    var instructions: InstructionBuffer;

    proc InstructionReader(instructions: InstructionBuffer) {
      this.instructions = instructions;
      this.instructions.rewind();
    }

    inline proc atEnd(): bool {
      return instructions.atEnd();
    }

    inline proc read(): ChasmOp {
      return instructions.read();
    }

    inline proc readCount(): uint(32) {
      return
        ((instructions.read(): uint(32)) << 24) |
        ((instructions.read(): uint(32)) << 16) |
        ((instructions.read(): uint(32)) << 8) |
        (instructions.read(): uint(32));
    }

    // read the next 4 bytes from high to low order and create a Term
    // if something goes wrong while readNext we just use 0s in those slots and fail later.
    inline proc readEntityId(): EntityId {
      return
        ((instructions.read(): EntityId) << 24) |
        ((instructions.read(): EntityId) << 16) |
        ((instructions.read(): EntityId) << 8) |
        (instructions.read(): EntityId);
    }

    inline proc readPredicateId(): PredicateId {
      return
        ((instructions.read(): PredicateId) << 24) |
        ((instructions.read(): PredicateId) << 16) |
        ((instructions.read(): PredicateId) << 8) |
        (instructions.read(): PredicateId);
    }

    inline proc readSPOMode(): OperandSPOMode {
      return instructions.read(): OperandSPOMode;
    }

    inline proc readPredicateIds() {
      var count = readCount();
      var P: domain(1) = {0..#count};
      var result: [P] PredicateId;
      for r in result do r = readPredicateId();
      return result;
    }

    inline proc readEntityIds() {
      var count = readCount();
      var P: domain(1) = {0..#count};
      var result: [P] PredicateId;
      for r in result do r = readEntityId();
      return result;
    }

    inline proc readSubjectIds() {
      return readEntityIds();
    }
    inline proc readObjectIds() {
      return readEntityIds();
    }
  }

  record InstructionWriter {
    var instructions: InstructionBuffer;

    proc writeCount(count: uint(32)): bool {
      if (!instructions.canAdvance(4)) {
        error("writeCount is out of instruction space for count: ", count, " at offset ", instructions.offset);
        return false;
      }

      instructions.write((count >> 24): ChasmOp);
      instructions.write(((count & 0x00FF0000) >> 16): ChasmOp);
      instructions.write(((count & 0x0000FF00) >> 8): ChasmOp);
      instructions.write(count: ChasmOp);

      return true;
    }

    proc writeSubjectId(subjectId: EntityId): bool {
      return writeEntityId(subjectId);
    }

    proc writeObjectId(objectId: EntityId): bool {
      return writeEntityId(objectId);
    }

    proc writeEntityId(entityId: EntityId): bool {
      if (!instructions.canAdvance(4)) {
        error("writeEntityId is out of instruction space for entityId: ", entityId, " at offset ", instructions.offset);
        return false;
      }

      instructions.write((entityId >> 24): ChasmOp);
      instructions.write(((entityId & 0x00FF0000) >> 16): ChasmOp);
      instructions.write(((entityId & 0x0000FF00) >> 8): ChasmOp);
      instructions.write(entityId: ChasmOp);

      return true;
    }

    proc writePredicateId(predicateId: PredicateId): bool {
      if (!instructions.canAdvance(4)) {
        error("writePredicateId is out of instruction space for predicateId: ", predicateId, " at offset ", instructions.offset);
        return false;
      }

      instructions.write((predicateId >> 24): ChasmOp);
      instructions.write(((predicateId & 0x00FF0000) >> 16): ChasmOp);
      instructions.write(((predicateId & 0x0000FF00) >> 8): ChasmOp);
      instructions.write(predicateId: ChasmOp);

      return true;
    }

    proc writeSPOMode(spoMode: OperandSPOMode): bool {
      if (!instructions.canAdvance(1)) {
        error("writeSPOMode is out of instruction space for SPO mode: ", spoMode, " at offset ", instructions.offset);
        return false;
      }

      instructions.write(spoMode: ChasmOp);

      return true;
    }

    proc writeScanPredicate(): bool {
      if (!instructions.canAdvance(1)) {
        error("writeScanPredicate is out of instruction space for CHASM_SCAN_PREDICATE at offset ", instructions.offset);
        return false;
      }

      instructions.write(CHASM_SCAN_PREDICATE: ChasmOp);

      return true;
    }

    proc writeAnd(): bool {
      if (!instructions.canAdvance(1)) {
        error("writeAnd is out of instruction space for CHASM_AND at offset ", instructions.offset);
        return false;
      }

      return instructions.write(CHASM_AND);
    }

    proc writeOr(): bool {
      if (!instructions.canAdvance(1)) {
        error("writeOr is out of instruction space for CHASM_OR at offset ", instructions.offset);
        return false;
      }

      return instructions.write(CHASM_OR);
    }

    proc writeHalt(): bool {
      if (!instructions.canAdvance(1)) {
        error("writeHalt is out of instruction space for CHASM_HALT at offset ", instructions.offset);
        return false;
      }

      return instructions.write(CHASM_HALT);
    }
  }

  proc operandForScanPredicate(subjectIds: [?S] EntityId, predicateIds: [?P] PredicateId, objectIds: [?O] EntityId): Operand {
    var entryOperand: Operand;

    if (predicateIds.size == 0) {
      /*info("operandForScanPredicate: predicateIds.size == 0");*/
      // Get predicate count for all partitions
      var predicateEntries: [0..#totalPredicateCount] PredicateEntry;
      var idx: int;
      for loc in Locales do on loc {
        var partitionPredicateEntries =  Partitions[here.id].segment.allPredicateEntries();
        for entry in partitionPredicateEntries {
          predicateEntries[idx] = entry;
          idx += 1;
        }
      }
      if (idx > 0) {
        entryOperand = new MultiPredicateEntryOperand(idx, predicateEntries, subjectIds.size, subjectIds, objectIds.size, objectIds);
      }
    } else if (predicateIds.size == 1) {
      /*info("operandForScanPredicate: predicateIds.size == 1");*/
      var partitionId = partitionIdForPredicate(predicateIds[0]);
      var entry = Partitions[partitionId].segment.operandForPredicate(predicateId);
      if (entry != nil) {
        entryOperand = new PredicateEntryOperand(entry, subjectIds.size, subjectIds, objectIds.size, objectIds);
      }
    } else {
      /*info("operandForScanPredicate: predicateIds.size == ", predicateIds.size);*/
      var predicateEntries: [0..#totalPredicateCount] PredicateEntry;
      var idx: int;
      for loc in Locales do on loc {
        for predicateId in predicateIds {
          var entry = Partitions[here.id].segment.operandForPredicate(predicateId);
          if (entry != nil) {
            predicateEntries[idx] = entry;
            idx += 1;
          }
        }
      }
      if (idx > 0) {
        entryOperand = new MultiPredicateEntryOperand(idx, predicateEntries, subjectIds.size, subjectIds, objectIds.size, objectIds);
      }
    }

    return if entryOperand != nil then entryOperand else NullOperand[here.id];
  }

  /**
    Intepret a query instruction sequence into a query AST object tree.
  */
  proc chasmInterpret(segment: Segment, instructionBuffer: InstructionBuffer): Operand {

    var reader = new InstructionReader(instructionBuffer);
    var stack: [0..1023] Operand;
    var stackPtr = stack.domain.high + 1;

    inline proc push(op: Operand) {
      if (stackPtr <= 0) {
        error("pushing out of stack space @ ", reader, " for buffer ", instructionBuffer);
        return;
      }
      stackPtr -= 1;
      stack[stackPtr] = op;
    }

    inline proc pop(): Operand {
      if (stackPtr > stack.domain.high) {
        error("popping out of stack space @ ", reader, " for buffer ", instructionBuffer);
        return new Operand();
      }
      var op = stack[stackPtr];
      stackPtr += 1;
      return op;
    }

    while (!reader.atEnd()) {
      var op = reader.read();
      select op {
        when CHASM_HALT           do break;
        when CHASM_SCAN_PREDICATE do push(operandForScanPredicate(reader.readSubjectIds(), reader.readPredicateIds(), reader.readObjectIds()));
        when CHASM_AND            do push(new IntersectionOperand(reader.readSPOMode(), pop(), pop()));
        when CHASM_OR             do push(new UnionOperand(reader.readSPOMode(), pop(), pop()));
      }
    }

    return pop();
  }
}
