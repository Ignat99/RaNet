import rdflib

rdflib.plugin.register('sparql', rdflib.query.Processor,
                       'rdfextras.sparql.processor', 'Processor')
rdflib.plugin.register('sparql', rdflib.query.Result,
                       'rdfextras.sparql.query', 'SPARQLQueryResult')


from rdflib import Graph
g = Graph()
g.parse("http://bigasterisk.com/foaf.rdf")
g.parse("http://www.w3.org/People/Berners-Lee/card.rdf")

from rdflib import Namespace
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
g.parse("http://danbri.livejournal.com/data/foaf") 
[g.add((s, FOAF['name'], n)) for s,_,n in g.triples((None, FOAF['member_name'], None))]

for row in g.query('SELECT ?aname ?bname WHERE { ?a foaf:knows ?b . ?a foaf:name ?aname . ?b foaf:name ?bname . }', 
                   initNs=dict(foaf=Namespace("http://xmlns.com/foaf/0.1/"))):
    print "%s knows %s" % tuple(row)
