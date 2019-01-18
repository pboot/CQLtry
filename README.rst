# cqltry

A very limited Python implementation of CQL (Corpus Query Language)
===================================================================

CQLtry implements a small subset of of the Corpus Query Language (see e.g. 
https://www.sketchengine.eu/documentation/corpus-querying/). It consists of three 
modules: 

- taggedString.py: a store (taggedStringStore), as a Python shelve, of taggedStrings, i.e. 
tokenized sentences with lemmas and POS-tags 

- handleQuery.py: translates the CQL queries to queryUnit objects and 
executes them on a taggedStringStore

- cqlGUI.py: GUI for entering a CQL query, sending it to handleQuery and giving a simple KWIC-like 
display of the query output


