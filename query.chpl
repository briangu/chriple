module Query {

  use Chasm, Common;

  record QueryResult {
    var triple: Triple;
  }

  record Query {
    var instructionBuffer: InstructionBuffer;
    var partitionLimit: int = 2048;

    proc Query(query: Query) {
      instructionBuffer = new InstructionBuffer(query.instructionBuffer.count);
      instructionBuffer.buffer = query.instructionBuffer.buffer;
      partitionLimit = query.partitionLimit;
    }
  }
}
