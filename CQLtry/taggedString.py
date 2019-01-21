# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch, helpers
from collections import namedtuple
import shelve
import re
import shutil
import os
import time
import math
from random import randint

#meta = namedtuple('meta', ['collection', 'genre', 'nur', 'rating'])

class taggedString(list):
    """ taggedStrings are lists of taggedWords (see below). They also have ident and
        meta properties: ident is an identification, meta is used to carry some metadata
        (of unspecified content)
    """
    def __init__(self,ident,l,meta):
        self.id = ident
        self.meta = meta
        self += l
#    def __init__(self,ident,string,meta):
#        self.id = ident
#        self.string = string
#        self.meta = meta
#        for ss in string.split():
#            init = self.inittw(ss)
#            self.append(taggedWord(init[0],init[1], init[2], init[3]))

    def flatstring(self):
        """ Returns a string representation of the taggedString """
        return ' '.join(tw.word + '/' + tw.tag + '/' + tw.lemma for tw in self)
    
    def unusedgetcollocatelemmas(self,termlist,nwords):
        """ notused """
        o = []
        gen = (tw for tw in self if tw.word in termlist or tw.lemma in termlist)
        for tw in gen:
#            print(tw)
#            o.append([tw1.lemma for tw1 in self[max(0,tw.pos - nwords):tw.pos] if tw1.tag[0:4] != 'verb'] +
#                [tw1.lemma for tw1 in self[tw.pos+1:min(len(self),tw.pos + 1 + nwords)] if tw1.tag[0:4] != 'verb'])
            o.append([tw1.lemma for tw1 in self[max(0,tw.pos - nwords):tw.pos]] +
                [tw1.lemma for tw1 in self[tw.pos+1:min(len(self),tw.pos + 1 + nwords)]])
        return o
    
    def getIndexEntry(self):
        """ Returns dictionary representation of the taggedString, used for the elasticsearch indexing """
        out = self.meta._asdict()
        out['text'] = ' '.join(self.getwordsnopunc())
        out['lemmas'] = ' '.join(self.getlemmasnopunc())
#        out['tags'] = ' '.join(self.gettags())
        return out
    
    def unusedgetlemmasfromstring(self,termlist):
        """ notused """
        if set(self.getlemmas() + self.getwords()) & termlist:
            return [self.getlemmas()]
        else:
            return []
        
    def getlemmas(self):
        """ returns a list of the lemmas of the taggedString's taggedWords """
        return [tw.lemma for tw in self]
    
    def unusedgetlemmasnoverb(self):
        """ returns a list of the lemmas of the taggedString's taggedWords that are not verbs """
        return [tw.lemma for tw in self if tw.tag[0:4] != 'verb']
    
    def getlemmasnopunc(self):
        """ returns a list of the lemmas of the taggedString's taggedWords that are not puntuation """
        return [tw.lemma for tw in self if tw.tag[0:4] != 'punc']
    
    def gettags(self):
        """ returns a list of the tags of the taggedString's taggedWords"""
        return [tw.tag for tw in self]
    
    def getwords(self):
        """ returns a list of the words of the taggedString's taggedWords """
        return [tw.word for tw in self]

    def unusedgetwordsnoverb(self):
        """ returns a list of the words of the taggedString's taggedWords that are not verbs """
        return [tw.word for tw in self if tw.tag[0:4] != 'verb']

    def getwordsnopunc(self):
        """ returns a list of the words of the taggedString's taggedWords that are not punctuation """
        return [tw.word for tw in self if tw.tag[0:4] != 'punc']

    def repos(self):
        """ resets the pos fields of the taggedString's taggedWords to run from 0 to len(taggedString) """
        i = 0
        for i in range(len(self)):
            self[i] = self[i]._replace(pos = i)

class taggedStringStore():
    """ A taggedStringStore is a Python shelve that stores taggedStrings. Creating and 
        filling a taggedStringStore also creates an ElasticSearch index on the 
        taggedStrings. indexed are the lemmas, the words and the metadata.
    """
    
    def __init__(self, dirname):
        """ Create the taggedStringStore by giving the directory where the shelve will be placed. """
        self.dirname = dirname
#        self.shelvename = os.path.join(self.dirname,'shelve')
        self.shelvename = self.dirname + '/'+ 'shelve'

    def reset(self):
        """ remove the entire directory and recreate it """
        try:
            shutil.rmtree(self.dirname)
        except FileNotFoundError:
            pass
        time.sleep(1)
        os.mkdir(self.dirname)

    def tssOpen(self):
        """ Open the shelve and return the opened shelve"""
        self.shelve = shelve.open(self.shelvename)
        return self.shelve

    def tssOpenRead(self):
        """ Open the shelve for reading only and return the opened shelve"""
        self.sshelve = shelve.open(self.shelvename,flag='r')
        print(self.shelvename)
        return self.sshelve

    def tssAdd(self,ident,ts):
        """ add a taggedString to the store """
        self.shelve[ident] = ts

    def tssClose(self):
        """ Closes the shelve and creates the elasticSearch index """
        self.createIndex()
        self.shelve.close()

    def createIndex(self):
        """ Creates an elastic search index on the store, deletes an existing index first. 
            The name of the index is the last part (part after last slash) of the directory name """
        dirname = self.dirname.replace('\\','/')
        ixname = dirname.rstrip('/').rpartition('/')[2]
        es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        es.indices.delete(index=ixname, ignore=[400, 404])
        gen = (self.createIndexParms(self.shelve[r], ixname) for r in self.shelve.keys())
        helpers.bulk(es,gen)

    def createIndexParms(self,ts,ixname):
        """ creates the data to be indexed for an individual taggedString in the store """
        d = ts.getIndexEntry()
        d['_type'] = 'response'
        d['_index'] = ixname
        d['_id'] = ts.id
        return d

class taggedStringCreator():
    """ utility object, helps create taggedString out of id, string and metadata
        string will consist of space-separated groups of word/pos-tag/lemma."""
    def create(self,ident,string,meta):
        l = []
        i = 0
        for ss in string.split():
            init = self.inittw(ss)
            l.append(taggedWord(i, init[0],init[1], init[2]))
            i += 1
        return taggedString(ident, l, meta)
    
    def inittw (self,string):
        """ creates taggedWord out of word/pos-tag/lemma group """
        string = string.lower()
        c = string.count('/')
        if c % 2 == 0:
            half = int(c / 2)
            s = string.split('/')
            word = '/'.join(s[0:half])
            tag = s[half]
            lemma = '/'.join(s[half+1:])
        elif string.rpartition('/')[2] == '@card@':
            s = string[:-7].rpartition('/')
            word = s[0]
            tag = s[1]
            lemma = '@card@'
        else:            
            print(string)
            raise ValueError
        return word, tag, lemma.lower()

class taggedWord(namedtuple('tw', ['pos', 'word','tag','lemma'])):
    """
    taggedWords are named tuples. They contain the fields:
        pos: position in taggedString
        word: token
        tag: pos-tag of token
        lemma: lemma of token
    """
    __slots__ = ()
#    def match(self,qterm):
#        if not isinstance(qterm,qt):
#            raise ValueError('qterm is not a qt but a ' + str(type(qterm)))
#        if qterm.attribute not in ['word','lemma','tag']:
#            raise ValueError('qterm attribute must be word, lemma or tag but is ' + qterm.attribute)
#        if qterm.value is None:
#            return True
#        if qterm.attribute == "word":
#            testStr = self.word
#        elif qterm.attribute == "lemma":
#            testStr =  self.lemma
#        elif qterm.attribute == "tag":
#            testStr = self.tag
#        if testregex(qterm.value):
#            return bool(re.match(qterm.value,testStr))
#        else:
#            return qterm.value == testStr

