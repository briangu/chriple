all: bin chriple

chriple:
	chpl --print-passes --fast -o bin/chriple chriple-naive.chpl

bin:
	mkdir -p bin

clean:
	rm -f bin/*
