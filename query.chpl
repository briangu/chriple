module Query {

  use Chasm, Common;

  record QueryResult {
    var triple: Triple;
  }

  record Query {
    var instructionBuffer: InstructionBuffer;
    var partitionLimit: int = 2048;

    proc Query(query: Query) {
      writeln(here.id, " ", query.locale.id, " ", query.instructionBuffer.locale.id); //, " ", query.instructionBuffer.buffer.locale.id);
      var otherIB = query.instructionBuffer;
      instructionBuffer = new InstructionBuffer(otherIB.count);
      for (x,y) in zip(instructionBuffer.buffer, otherIB.buffer) {
        x = y;
      }
      partitionLimit = query.partitionLimit;
    }
  }
}
