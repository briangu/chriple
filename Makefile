all: bin chriple

chriple:
	chpl --print-passes --fast -o bin/chriple chriple-naive.chpl

chriple_dbg:
	chpl --print-passes -g --savec=code -o bin/chriple chriple-naive.chpl

dbg:
	chpl --print-passes -g -o bin/chriple chriple-naive.chpl

bin:
	mkdir -p bin

clean:
	rm -f bin/*
