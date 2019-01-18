A very limited Python implementation of CQL (Corpus Query Language)
===================================================================

CQLtry implements a small subset of of the Corpus Query Language (see e.g. 
https://www.sketchengine.eu/documentation/corpus-querying/), a query langage for linguistic corpora.
It consists of three modules: 

- taggedString.py: a store (taggedStringStore), as a Python shelve, of taggedStrings, i.e. 
  tokenized sentences with lemmas and POS-tags. It also creates an ElasticSearch index on the shelve. 
- handleQuery.py: translates the CQL queries to queryUnit objects and 
  executes them on a taggedStringStore. Optionally, it uses the ElasticSearch index.
- cqlGUI.py: GUI for entering a CQL query, sending it to handleQuery and giving a simple KWIC-like 
  display of the query output.

There are other and better implementations of CQL around, but those that I could find were (for me)
unsuitable, because of platform, complexity, language or otherwise. I also needed an implementation 
that allowed me to execute CQL queries on my own objects, rather than on a corpus format beyond 
my control. That's why I decided to write my own small implementation. I use the software to 
investigate online book reviews for the aspects of books that are being discussed in the reviews
(see https://github.com/pboot/bookcrit).

CQLtry assumes a single-level structure (sentences). Functionality that CQLtry supports:

- testing on (sequences of) tokens, by word, lemma or tag
- testing on string value or regular expression
- testing on equality or inequality
- use of 'any token' ([]) 
- optional tokens (?)
- repetitions (+ or {})
- anchoring query to beginning of sentence

So many of the interesting aspects of CQL are missing. I may add a feature or two in the future, 
but don't expect any large improvements in CQL coverage. 


 
