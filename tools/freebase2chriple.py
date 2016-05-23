#
# Transform freebase rdf triples to chriple format
#   requires previously mapped nouns and predicates
#
import sqlite3
import gzip

nounsDBConnection = sqlite3.connect("nouns.db")
nounsDBConnection.text_factory = str
nounsDB = nounsDBConnection.cursor()

predicatesDBConnection = sqlite3.connect("predicates.db")
predicatesDBConnection.text_factory = str
predicatesDB = predicatesDBConnection.cursor()

def idForNoun(noun):
    a = nounsDB.execute("SELECT id FROM nouns WHERE noun = ?", (noun, ))
    r = a.fetchall()
    return r[0][0] if len(r) > 0 else 0

def idForPredicate(predicate):
    a = predicatesDB.execute("SELECT id FROM predicates WHERE predicate = ?", (predicate, ))
    r = a.fetchall()
    return r[0][0] if len(r) > 0 else 0

with gzip.open("freebase-rdf-latest.gz", 'r') as fin:
    for line in fin:
        (subject, predicate, obj, junk) = line.strip().split('\t')
        subjectId = idForNoun(subject)
        predicateId = idForPredicate(predicate)
        objId = idForNoun(obj)
        print "{} {} {}".format(subjectId, predicateId, objId)

nounsDBConnection.close()
predicatesDBConnection.close()
