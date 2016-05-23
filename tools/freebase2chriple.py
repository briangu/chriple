#
# Transform freebase rdf triples to chriple format
#   requires previously mapped nouns and predicates
#
import sqlite3
import gzip

nounsDB = sqlite3.open("nouns.db")
predicatesDB = sqlite3.open("predicates.db")

def idForNoun(noun):
    result = nounsDB.execute("SELECT id FROM nouns WHERE noun = ?", (noun,))

def idForPredicate(predicate):
    result = predicatesDB.execute("SELECT id FROM predicates WHERE predicate = ?", (predicate,))
    print(result)

with gzip.open("freebase-latest.gz", 'r') as fin:
    for lines in fin.read:
        (subject, predicate, obj) = lines.strip().split(' ')
        subjectId = idForNoun(subject)
        predicateId = idForPredicate(predicate)
        objId = idForNoun(obj, nounsDB)
        print(subjectId, " ", predicateId, " ", objId, "\n")

nounsDB.close()
predicatesDB.close()
