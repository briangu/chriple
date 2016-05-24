#
# Transform freebase rdf triples to chriple format
#   requires previously mapped nouns and predicates
#
import sqlite3
import gzip
import sys

nounsDBConnection = sqlite3.connect("nouns.db")
nounsDBConnection.text_factory = str
nounsDB = nounsDBConnection.cursor()

predicatesDBConnection = sqlite3.connect("predicates.db")
predicatesDBConnection.text_factory = str
predicatesDB = predicatesDBConnection.cursor()

lastKey = ["","",""]
lastValue = [0,0,0]

def idForNoun(noun,pos):
    global lastKey
    global lastValue
    if noun == lastKey[pos]:
        return lastValue[pos]
    a = nounsDB.execute("SELECT id FROM nouns WHERE noun = ?", (noun, ))
    r = a.fetchall()
    lastKey[pos] = noun
    lastValue[pos] = r[0][0] if len(r) > 0 else 0
    return lastValue[pos]

def idForPredicate(predicate, pos):
    global lastKey
    global lastValue
    if predicate == lastKey[pos]:
        return lastValue[pos]
    a = predicatesDB.execute("SELECT id FROM predicates WHERE predicate = ?", (predicate, ))
    r = a.fetchall()
    lastKey[pos] = predicate
    lastValue[pos] = r[0][0] if len(r) > 0 else 0
    return lastValue[pos]

with gzip.open(sys.argv[1]+".out.gz",'w') as fout:
    with gzip.open(sys.argv[1], 'r') as fin:
        for line in fin:
            line = line.strip()
            (subject, predicate, obj, junk) = line.split('\t')
            subjectId = idForNoun(subject,0)
            predicateId = idForPredicate(predicate,1)
            objId = idForNoun(obj,2)
            fout.write("{} {} {}\n".format(subjectId, predicateId, objId))

nounsDBConnection.close()
predicatesDBConnection.close()
