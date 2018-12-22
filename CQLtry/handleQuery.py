# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-

#import taggedString as ts
#import handleQuery as hq
#import cqlutil as cu
from . import cqlutil as cu
from . import taggedString as ts


from elasticsearch import Elasticsearch, helpers
from collections import namedtuple
import shelve
import re
import shutil
import os
import time
import math
from random import randint

def testregex(string):
#    print(string)
    for c in '.*[]^{}\\|':
        if c in string: return True
    return False

qttemp = namedtuple('qt', ['value','attribute','nopt','oper'])
qttemp.__new__.__defaults__ = (None,"word",None, '=')

class qt(qttemp):
    def match(self,tw):
#        print(self, tw)
        if self.value is None:
            return True
        if self.attribute == "word":
            testStr = tw.word
        elif self.attribute == "lc":
            testStr = tw.word.lower()
        elif self.attribute == "lemma":
            testStr =  tw.lemma
        elif self.attribute == "tag":
            testStr = tw.tag
        if testregex(self.value):
            if self.oper == '=':
                return bool(re.fullmatch(self.value,testStr))
            else: 
                return not bool(re.fullmatch(self.value,testStr))
        else:
            if self.oper == '=':
                return self.value == testStr
            else: 
                return self.value != testStr

hitres = namedtuple('hitres', ['hitString','pos'])

class tssQuery():

    def __init__(self,ashelve,query,elastic=False,elindex=None):
        if not isinstance(ashelve,shelve.DbfilenameShelf):
            raise ValueError('Shelve is not a shelve but a ' + str(type(ashelve)))
        self.setQuery(query)
        self.shelve = ashelve
        self.elastic = elastic
        if elastic:
            self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
            self.ixname = elindex

    def setQuery(self,query):
        self.checkQuery(query)
        self.query = query
        self.qlen = len(query)

    def checkQuery(self,query):
        if not isinstance(query,list):
            raise ValueError('Query must be a list but is a ' + str(type(query)))
        for qterm in query:
            if not isinstance(qterm,qt):
                raise ValueError('Query must consist of qt-s but contains a ' + str(type(qterm)))
            if qterm.attribute not in ['word','lemma','tag','lc']:
                raise ValueError('qterm attribute must be word, lc, lemma or tag but is ' + qterm.attribute)

    def execute(self):
        if len(self.query) == 0:
            raise ValueError('Query is empty')
        res = []
        qh = tsQueryHelper()
        checkall = True
        if self.elastic:
            keys = []
            print(self.query)
            et = []
            lw = [qtm.value for qtm in self.query if not qtm.value is None 
                  and not testregex(qtm.value) and qtm.attribute in ['lc', 'word'] and (qtm.oper is None or qtm.oper == '=')]
            if lw:
                testw = {"match": {'text':{'query': ' '.join(lw), 'operator' : 'and'}}} 
                et.append(testw)
            lw = [qtm.value for qtm in self.query if not qtm.value is None 
                  and not testregex(qtm.value) and qtm.attribute == 'lemma' and (qtm.oper is None or qtm.oper == '=')]
            if lw:
                testw = {"match": {'lemmas':{'query': ' '.join(lw), 'operator' : 'and'}}} 
                et.append(testw)
            lw = [qtm.value for qtm in self.query if not qtm.value is None 
                  and testregex(qtm.value) and qtm.attribute in ['lc', 'word'] and  (qtm.oper is None or qtm.oper == '=')]
            for w in lw:
                testw = {"regexp": {'text': w}} 
                et.append(testw)
            lw = [qtm.value for qtm in self.query if not qtm.value is None 
                  and testregex(qtm.value) and qtm.attribute == 'lemma' and  (qtm.oper is None or qtm.oper == '=')]
            for w in lw:
                testw = {"regexp": {'lemmas': w}} 
                et.append(testw)
            if et:
                checkall = False
                et = {"query": {'constant_score' : { 'filter' : {"bool": 
                                           {"must": et}
                                          }}}, "size": 1 }
                print(et)
                s = self.es.search(self.ixname, body=et)
                print(s)
                print('Total hits in elastic', s['hits']['total'])
                factor = s['hits']['total'] / 200
                print('factor', factor)
                et['size'] = 200
                et['from'] = 0 if factor < 2 else randint(0, min(49,math.floor(factor - 1))) * 200
                print('from', et['from'])
                s = self.es.search(self.ixname, body=et)
                keys = [h['_id'] for h in s['hits']['hits']]
#                print(keys)
        if checkall:
            keys = self.shelve.keys()
            factor = 1
        for k in keys:
#            print(k)
            sent = self.shelve[k]
            for r in qh.tsqueryexec(self.query,sent):
                hitString = ts.taggedString(sent.id,'',sent.meta)
                hitString += r[1]
                res.append(hitres(hitString,r[0]))
        return(res, len(res) if factor <= 1 else math.floor(len(res) * factor))

class queryUnit(list):
    """ """
    def __init__(self,l=[],anchored=None):
        self.checkQUnit(l,anchored)
        self.setAnchor(anchored)
        self += l
    def setAnchor(self,anchored):
        self.anchored = anchored
    def checkQUnit(self,l,anchored):
        if anchored is not None and anchored not in ['start']:
            raise ValueError("anchored must be None or 'start' but is " + anchored)
        if not isinstance(l,list):
            raise ValueError('queryUnit must be a list but is a ' + str(type(l)))
        for qterm in l:
            if not isinstance(qterm,qt):
                raise ValueError('queryUnit must consist of qt-s but contains a ' + str(type(qterm)))
            if qterm.attribute not in ['word','lemma','tag','lc']:
                raise ValueError('qterm attribute must be word, lc, lemma or tag but is ' + qterm.attribute)

class tsQueryHelper():
    def __init__(self,deftag="word"):
        self.deftag = deftag
        
    def tsqueryexec(self, query, sent):
        """ Matches taggedString and querypattern. Returns a list of one tuple per hit. 
            The tuple consists of first the position, of the hit, than a list of the matching words in 
            the taggedString """
        sentres = []
        minlength = sum(1 for qtm in query if qtm.nopt is None)
        if minlength > 0:
            minlength -= 1
        if query.anchored is None:
            for i in range(len(sent) - minlength):
#                print(1, sent[i].word)
                out = self.matchtw(query,sent[i:])
#                print(2, 'out: ', out)
                if out:
#                    print(3)
                    sentres.append((i,out))
        else: 
            out = self.matchtw(query,sent)
#            print(4,'out: ', out)
            if out:
#                print(5)
                sentres.append((0,out))
        if not sentres:
            return []
        else:
            return self.removeoverlap(sentres)

    def tsqueryexecdummy(self, query, sent):
        print(sent[0])
        return []

        
    def removeoverlap(self,results):
#        print()
#        for r in results:
#            print(r)
        if len(results) == 1:
           return results
        sresults = sorted(results, reverse = True, key=self.removeoverlapsort)
        restemp = sresults[0]
        del sresults[0:1]
        toberemoved = set()
#        print('restemp',restemp)
        for i in range(len(sresults)):
#            print('sr i',sresults[i])
            if (sresults[i][0] >= restemp[0]) \
                and (sresults[i][0] + len(sresults[i][1]) <= restemp[0] + len(restemp[1])):
                toberemoved.add(i)
#        print('remove', toberemoved)
        for i in sorted(toberemoved,reverse=True):
            del sresults[i]
        if len(sresults) == 0:
            return [restemp]
        elif len(sresults) == 1:
            return [restemp,sresults[0]]
        else:
            return [restemp] + self.removeoverlap(sresults)
        
    def removeoverlapsort(self,result):
        return len(result[1])
    
    def matchtw(self,querypart,sentpart):
        """ Recursive function ('match taggend word'): matches one term from sentence and querypattern; 
        if there is a match, calls itself with as arguments the e=rest of the sentence and the rest 
        of the pattern. Returns a list containing the matching tokens in the sentence pattern"""
#        print('qt',querypart)
#        print('sp',sentpart)
        qtm = querypart[0]
        if qtm.nopt is None: 
#            print('g')
            if qtm.match(sentpart[0]):
                minlength = sum(1 for qtm in querypart[1:] if qtm.nopt is None)
                if len(querypart) == 1:
#                    print('d')
                    return [sentpart[0]]
                elif len(sentpart) == 1:
                    if minlength > 0:
                        return None
                    else:
                        return [sentpart[0]]
                else:
                    out = self.matchtw(querypart[1:],sentpart[1:])
#                    print('a',out)
                    if out:
#                        print('b')
                        return [sentpart[0]] + out
                    else:
#                        print('c')
                        if minlength == 0:
                            return [sentpart[0]]
                        else:
                            return None
            else:
                return None
        else:
            if qtm.match(sentpart[0]):
#                print('f')
                if len(querypart) == 1:
                    return [sentpart[0]]
                elif len(sentpart) == 1:
                    minlength = sum(1 for qtm in querypart[1:] if qtm.nopt is None)
                    if minlength == 0:
                        return [sentpart[0]]
                else:
                    if qtm.nopt == 1:
                        out = self.matchtw(querypart[1:],sentpart[1:])
                        if out:
                            return [sentpart[0]] + out
                    else:
                        newqt = qt(qtm.value, qtm.attribute, qtm.nopt - 1, qtm.oper)
                        out = self.matchtw([newqt] + querypart[1:],sentpart[1:])
                        if out:
                            return [sentpart[0]] + out
#       All remaining cases have nopt but the optionam term didnt work out 
#        print('e')
        if len(querypart) == 1:
            return None
        else:
            out = self.matchtw(querypart[1:],sentpart)
            if out:
                return out
            else:
                return None
            
    def translate(self,string):
        string = ''.join(string.split())
        print(string)
        top = queryUnit()
        inQt = False
        curUnit = [top]
        attribute = ''
        j = 0
        if len(string.strip()) == 0:
            raise cu.InputError("CQL string empty")
        while j in range(len(string)):
            if string[j] == '"':
                print('a',string[j],j)
                afterQt = False
                inRangeExp = False
                value = ""
                j += 1
                while j < len(string) and string[j] != '"':
                    value += string[j]
                    j += 1
                if len(value) == 0:
                    raise cu.InputError("Value is empty string")
                if j == len(string):
                    raise cu.InputError("unmatched double quotation marks")
                if not inQt:
                    curUnit[-1].append(qt(value,self.deftag,oper='='))
                else:
                    curUnit[-1].append(qt(value,attribute,oper=oper))
                j += 1
            elif string[j] == '[':
                print('b',string[j],j)
                inQt = True
                attribute = ""
                j += 1
                if string[j] == ']':
                    curUnit[-1].append(qt(None))
                    inQt = False
                    afterQt = True
                else:
                    while j < len(string) and string[j] not in '!=':
                        attribute += string[j]
                        j += 1
                    if j == len(string):
                        raise cu.InputError("unmatched double quotation marks")
                    if len(attribute) == 0:
                        raise cu.InputError("Attribute is empty string")
                    if attribute not in ['word','lemma','tag','lc']:
                        raise cu.InputError('attribute must be word, lc, lemma or tag but is ' + attribute)
                    if string[j] == '!':
                        oper = '!='
                        j += 1
                        if string[j] != '=':
                            raise cu.InputError('Unexpected operator ' + string[j] + ' at pos ' + str(j))
                    else:
                        oper = '='
                j += 1
            elif string[j] == ']':
                print('c',string[j],j)
                if not inQt:
                    raise cu.InputError("Closing bracket ']' that was not opened")
                inQt = False
                afterQt = True
                j += 1
            elif string[j] in ['*','+','?','{']:
                print('d',string[j],j)
                if not afterQt:
                    raise cu.InputError("Range not allowed here: character " + string[j] + ' at pos ' + str(j))
                afterQt = False
                nmin = 0
                nmax = 9999
                j += 1
                if string[j-1] == '+':
                    nmin = 1
                elif string[j-1] == '?':
                    nmax = 1
                elif string[j-1] == '{':
                    inRangeExp = True
                    value = '' 
                    while j < len(string) and string[j] != ',':
                        value += string[j]
                        j += 1
                    if j == len(string):
                        raise cu.InputError("Expected comma after range minimum")
                    if not bool(re.match('[0-9]+',value)):
                        raise cu.InputError("Range minimum non numeric or empty: " + value)
                    nmin = int(value)
                    j += 1
                    value = '' 
                    while j < len(string) and string[j] != '}':
                        value += string[j]
                        j += 1
                    if j == len(string):
                        raise cu.InputError("Expected } after range maximum")
                    if not bool(re.match('[0-9]+',value)):
                        raise cu.InputError("Range maximum non numeric or empty: " + value)
                    nmax = int(value)
                if nmin > nmax:
                    raise cu.InputError("Range maximum less than range minimum: min " + str(nmin) + ', max ' + str(nmax))
                if nmax == 0:
                    raise cu.InputError("Range maximum must be higher than zero")
                print(nmin,nmax)
#                if nmin == 1: # first term is already in expression
#                    nmin = nmin - 1
#                    nmax = nmax - 1
                while nmin > 0: # add same fixed terms (latest one added)to expression while nmin > 0
                    curUnit[-1].append(curUnit[-1][-1])
                    nmin = nmin - 1
                    nmax = nmax - 1
                if nmax > 0:
                    c = curUnit[-1][-1]
                    curUnit[-1][-1] = qt(c.value, c.attribute, nmax, c.oper)
                else:
                    del curUnit[-1][-1]
            elif string[j] == '}':
                print('e',string[j],j)
                if not inRangeExp:
                    raise cu.InputError("Closing bracket '}' that was not opened")
                inRangeExp = False
                j += 1
            elif string[j] == '<':
                if j != 0:
                    raise cu.InputError("Anchor only allowed in position 0, found at position " + str(j))
                if string[0:3] != '<s>':
                    raise cu.InputError("Anchor must be <s>")
                if len(string) < 4:
                    raise cu.InputError("No text found after anchor")
                curUnit[-1].setAnchor('start')
                j = j + 3
            else:
                print('f',string[j],j)
                if inQt:
                    raise cu.InputError("Opening bracket '[' has not been closed. Character is " + string[j])
                else:
                    raise cu.InputError("Unexpected character " + string[j] + " at pos " + str(j))
        if inQt:
            raise cu.InputError("Opening bracket '[' has not been closed")
        return top
