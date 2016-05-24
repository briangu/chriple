#
# Transform freebase rdf triples to chriple format
#   requires previously mapped nouns and predicates
#
import sqlite3
import gzip

nounsDBConnection = sqlite3.connect("nouns.db")
nounsDBConnection.text_factory = str
nounsDB = nounsDBConnection.cursor()

lastNounKey = ""
lastNounValue = 0

predicatesDBConnection = sqlite3.connect("predicates.db")
predicatesDBConnection.text_factory = str
predicatesDB = predicatesDBConnection.cursor()

lastPredicateKey = ""
lastPredicateValue = 0

def idForNoun(noun):
    global lastNounKey
    global lastNounValue
    if noun == lastNounKey:
        return lastNounValue
    a = nounsDB.execute("SELECT id FROM nouns WHERE noun = ?", (noun, ))
    r = a.fetchall()
    lastNounKey = noun
    lastNounValue = r[0][0] if len(r) > 0 else 0
    return lastNounValue

def idForPredicate(predicate):
    global lastPredicateKey
    global lastPredicateValue
    if predicate == lastPredicateKey:
        return lastPredicateValue
    a = predicatesDB.execute("SELECT id FROM predicates WHERE predicate = ?", (predicate, ))
    r = a.fetchall()
    lastPredicateKey = predicate
    lastPredicateValue = r[0][0] if len(r) > 0 else 0
    return lastPredicateValue

with gzip.open("freebase-rdf-latest.gz", 'r') as fin:
    for line in fin:
        line = line.strip()
        (subject, predicate, obj, junk) = line.split('\t')
        subjectId = idForNoun(subject)
        predicateId = idForPredicate(predicate)
        objId = idForNoun(obj)
        print "{} {} {}".format(subjectId, predicateId, objId)

nounsDBConnection.close()
predicatesDBConnection.close()
