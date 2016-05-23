import sqlite3
import gzip

# TODO: ensure unique (id,[noun | predicate])

nounsDBConnection = sqlite3.connect("nouns.db")
nounsDBConnection.text_factory = str
nounsDB = nounsDBConnection.cursor()
nounsDB.execute("CREATE TABLE nouns (id INTEGER PRIMARY KEY AUTOINCREMENT, noun text)")
nounsDB.execute("CREATE INDEX Idx1 ON nouns(noun)")

predicatesDBConnection = sqlite3.connect("predicates.db")
predicatesDBConnection.text_factory = str
predicatesDB = predicatesDBConnection.cursor()
predicatesDB.execute("CREATE TABLE predicates (id INTEGER PRIMARY KEY AUTOINCREMENT, predicate text)")
predicatesDB.execute("CREATE INDEX Idx1 ON predicates(predicate)")

def upsertNoun(noun):
    nounsDB.execute("INSERT into nouns (noun) VALUES (?)", (noun,))

def upsertPredicate(predicate):
    nounsDB.execute("INSERT into predicates (predicate) VALUES (?)", (predicate,))

with gzip.open("freebase-latest.gz", 'r') as fin:
    for lines in fin.read:
        (subject, predicate, obj) = lines.strip().split(' ')
        upsertNoun(subject)
        upsertPredicate(predicate)
        upsertNoun(obj)

nounsDBConnection.commit()
nounsDBConnection.close()

predicatesDBConnection.commit()
predicatesDBConnection.close()
