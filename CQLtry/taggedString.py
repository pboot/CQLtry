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

class taggedStringStore():
    def __init__(self, dirname):
        self.dirname = dirname
        self.shelvename = os.path.join(self.dirname,'shelve')
    def reset(self):
        try:
            shutil.rmtree(self.dirname)
        except FileNotFoundError:
            pass
        time.sleep(1)
        os.mkdir(self.dirname)
    def tssOpen(self):
        self.shelve = shelve.open(self.shelvename)
        return self.shelve
    def tssAdd(self,ident,ts):
        self.shelve[ident] = ts
    def tssClose(self):
        self.createIndex()
        self.shelve.close()
    def createIndex(self):
        dirname = self.dirname.replace('\\','/')
        ixname = dirname.rstrip('/').rpartition('/')[2]
        es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        es.indices.delete(index=ixname, ignore=[400, 404])
        gen = (self.createIndexParms(self.shelve[r], ixname) for r in self.shelve.keys())
        helpers.bulk(es,gen)
    def createIndexParms(self,ts,ixname):
        d = ts.getIndexEntry()
        d['_type'] = 'response'
        d['_index'] = ixname
        d['_id'] = ts.id
        return d

class taggedStringCreator():
    def create(self,ident,string,meta):
        l = []
        i = 0
        for ss in string.split():
            init = self.inittw(ss)
            l.append(taggedWord(i, init[0],init[1], init[2]))
            i += 1
        return taggedString(ident, l, meta)
    def inittw (self,string):
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

class taggedString(list):
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
        return ' '.join(tw.word + '/' + tw.tag + '/' + tw.lemma for tw in self)
    def getcollocatelemmas(self,termlist,nwords):
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
        out = self.meta._asdict()
        out['text'] = ' '.join(self.getwordsnopunc())
        out['lemmas'] = ' '.join(self.getlemmasnopunc())
#        out['tags'] = ' '.join(self.gettags())
        return out
    def getlemmasfromstring(self,termlist):
        if set(self.getlemmas() + self.getwords()) & termlist:
            return [self.getlemmas()]
        else:
            return []
    def getlemmas(self):
        return [tw.lemma for tw in self]
    def getlemmasnoverb(self):
        return [tw.lemma for tw in self if tw.tag[0:4] != 'verb']
    def getlemmasnopunc(self):
        return [tw.lemma for tw in self if tw.tag[0:4] != 'punc']
    def gettags(self):
        return [tw.tag for tw in self]
    def getwords(self):
        return [tw.word for tw in self]
    def getwordsnoverb(self):
        return [tw.word for tw in self if tw.tag[0:4] != 'verb']
    def getwordsnopunc(self):
        return [tw.word for tw in self if tw.tag[0:4] != 'punc']
    def repos(self):
        i = 0
        for i in range(len(self)):
            self[i] = self[i]._replace(pos = i)

class taggedWord(namedtuple('tw', ['pos', 'word','tag','lemma'])):
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

