all: bin chriple

chriple:
	chpl --print-passes -o bin/chriple chriple-naive.chpl

bin:
	mkdir -p bin

clean:
	rm -f bin/*
