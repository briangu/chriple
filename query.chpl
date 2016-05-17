module Query {

  use Chasm, Common;

  record QueryResult {
    var triple: Triple;
  }

  record Query {
    var instructionBuffer: InstructionBuffer;
    var partitionLimit: int = 2048;

    // For some reason, creating a copy construtor fails to work correctly on multiple locales
    /*proc Query(query: Query) {
      var otherIB = query.instructionBuffer;
      instructionBuffer = new InstructionBuffer(otherIB.count);
      instructionBuffer.buffer = otherIB.buffer;
      partitionLimit = query.partitionLimit;
    }*/
  }
}
