# Chriple

Chriple is a simple Chapel implementation of an RDF triple store.  

Setup
=====

Install Chapel.  All demo scripts expect a multi-node Chapel installation.

Visit http://chapel.cray.com/download.html and download

To setup Chapel to run locally, add this to your ~/.bash_profile

    cd $CHAPEL_HOME
    source ./util/setchplenv.sh

    export CHPL_COMM=gasnet
    export GASNET_SPAWNFN=L
    export CHPL_TARGET_ARCH=native

Build Chapel

    cd $CHAPEL_HOME
    make
    make check

Build and run Chriple demo

    make
    ./bin/chriple -nl 2
