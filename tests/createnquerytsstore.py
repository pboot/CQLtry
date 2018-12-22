import CQLtry.taggedString as ts
import CQLtry.handleQuery as hq
import CQLtry.cqlutil as cu

from elasticsearch import Elasticsearch
from collections import namedtuple
import tempfile

# Creates a very simple taggedString Store and queries it
# assumes Elasticsearch is running

example1 = """Mexico/nounprop/Mexico ,/punc/, 1923/num__card/@card@ ./$./. In/prep/in 
Mexicali/nounsg/Mexicali bevindt/verbpressg/bevinden zich/pronrefl/zich 
een/det__art/een ondergrondse/adj/ondergronds stad/nounsg/stad ,/punc/, 
La/adj/La Chinesca/nounpl/Chinesca ,/punc/, die/pronrel/die volledig/adj/volledig 
in/prep/in handen/nounpl/hand is/verbpressg/zijn van/prep/van de/det__art/de Chinezen/nounpl/Chinees 
./$./. Pi/nounsg/pi Ying/nounsg/Ying ,/punc/, heer/nounsg/heer en/conjcoord/en 
meester/nounsg/meester over/prep/over deze/det__demo/deze stad/nounsg/stad 
/nounsg/ een/det__art/een bejaarde/adj/bejaard man/nounsg/man die/pronrel/die 
decennialang/adj/decennialang illegaal/adj/illegaal opium/nounsg/opium en/conjcoord/en 
alcohol/nounsg/alcohol verhandelde/verbpapa/verhandelen /nounsg/ draagt/verbpressg/dragen 
al/det__indef/al jaren/nounpl/jaar een/det__art/een geheim/nounsg/geheim met/prep/met 
zich/pronrefl/zich mee/adv/mee ,/punc/, sinds/prep/sinds zijn/det__poss/zijn 
vlucht/nounsg/vlucht uit/prep/uit Shanghai/nounsg/Shanghai toen/conjsubo/toen 
hij/pronpers/hij nog/adv/nog een/det__art/een kind/nounsg/kind was/verbpastsg/wezen|zijn ./$./. 
Geruchten/nounpl/gerucht over/prep/over dit/det__demo/dit geheim/nounsg/geheim zorgen/verbinf/zorgen 
voor/prep/voor een/det__art/een toeloop/nounsg/toeloop van/prep/van de/det__art/de meest/adj/meest 
wonderlijke/adj/wonderlijk personages:/nounsg/personages: een/det__art/een ambitieuze/adj/ambitieus 
jager/nounsg/jager van/prep/van exotische/adj/exotisch diersoorten/nounpl/diersoort die/pronrel/die 
geobsedeerd/verbpapa/obsederen is/verbpressg/zijn door/prep/door het/det__art/het spoor/nounsg/spoor 
van/prep/van een/det__art/een enorm/adj/enorm beest;/nounsg/beest; 
een/det__art/een dierenarts/nounsg/dierenarts en/conjcoord/en zijn/det__poss/zijn 
zoontje/nounsg/zoon ,/punc/, die/pronrel/die geneeskrachtige/adj/geneeskrachtig elixers/nounpl/elixer 
verkopen/verbprespl/verkopen op/prep/op hun/det__poss/hun vlucht/nounsg/vlucht voor/prep/voor 
de/det__art/de Revolutie/nounsg/revolutie en/conjcoord/en hun/det__poss/hun 
herinneringen/nounpl/herinnering ./$./. Maar/conjcoord/maar ook/adv/ook een/det__art/een 
afgevaardigde/nounsg/afgevaardigde van/prep/van keizer/nounsg/keizer Wilhelm/nounsg/Wilhelm 
II/num__ord/II ,/punc/, die/pronrel/die vanwege/prep/vanwege politieke/adj/politiek 
belangen/nounpl/belang naar/prep/naar Mexicali/nounsg/Mexicali afreist/verbpressg/afreizen 
,/punc/, maakt/verbpressg/maken zijn/det__poss/zijn entree/nounsg/entree ./$./. En/conjcoord/en 
allemaal/pronindef/allemaal zijn/verbprespl/zijn ze/pronpers/ze naar/prep/naar 
iets/adv/iets op/prep/op zoek/adv/zoek ./$./. ./$./. ./$./. Drakenogen/nounsg/Drakenogen 
omspant/verbpressg/omspannen verschillende/adj/verschillend continenten/nounpl/continent 
,/punc/, tijden/nounpl/tijd en/conjcoord/en perspectieven/nounpl/perspectief ,/punc/, 
heeft/verbpressg/hebben een/det__art/een razende/adj/razend vaart/nounsg/vaart en/conjcoord/en 
eindigt/verbpressg/eindigen met/prep/met een/det__art/een overdonderende/verbpresp/overdonderen 
finale/nounsg/finale ./$./.'"""

example2 = """In/prep/in Mexicali/nounsg/Mexicali bevindt/verbpressg/bevinden zich/pronrefl/zich 
een/det__art/een """

td = tempfile.TemporaryDirectory()

tss = ts.taggedStringStore(td.name)
tss.reset()
tss.tssOpen()

meta = namedtuple('meta', ['collection', 'genre', 'nur', 'rating'])
metaex = meta('testa','testb','testc','testd')

j = 1
for c in [example2]:
    tsc = ts.taggedStringCreator()
    s = tsc.create(1,c,metaex)
    i = 1
    ident = str(j) + '.' + str(i)
    sent = ts.taggedString(ident,'',metaex)
    for tw in s:
        sent.append(tw)
        if tw.lemma in ['.','?','!']:
            sent.repos()
            tss.tssAdd(ident,sent)
            i += 1
            ident = str(j) + '.' + str(i)
            sent = ts.taggedString(ident,'',metaex)
    if len(sent) > 0:
        sent.repos()
        tss.tssAdd(ident,sent)
    j += 1

tss.tssClose()

# sleept for some time because elastic's indexing may take a while
import time
time.sleep(10)

tss = ts.taggedStringStore(td.name)
tssShelve = tss.tssOpen()

for k in tssShelve.keys():
    print(tssShelve[k].id)
    assert tssShelve[k].id == '1.1'
    print(tssShelve[k].meta)
    print(tssShelve[k].flatstring())
    assert tssShelve[k].flatstring() == 'in/prep/in mexicali/nounsg/mexicali bevindt/verbpressg/bevinden zich/pronrefl/zich een/det__art/een'
print()

# First run queries without Elastic

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("in")]))
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'in' and len(o[0]) == 1

q = hq.tssQuery(tssShelve, hq.queryUnit([hq.qt("in")],None))
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'in' and len(o[0]) == 1

q = hq.tssQuery(tssShelve, hq.queryUnit([hq.qt("in")],'start'))
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'in' and len(o[0]) == 1

q = hq.tssQuery(tssShelve, hq.queryUnit([hq.qt("mexicali")],'start'))
o = q.execute()
assert len(o[0]) == 0

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt(None)]))
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'in' and len(o[0]) == 5

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("in",oper='!=')]))
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'mexicali' and len(o[0]) == 4

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("in|out",oper='!=')]))
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'mexicali' and len(o[0]) == 4

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("a|b",oper='!=')]))
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'in' and len(o[0]) == 5

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("in"), hq.qt("mexicali",nopt=1)]))
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'in mexicali' and len(o[0]) == 1

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("in"), hq.qt(None,nopt=1), hq.qt("mexicali")]))
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'in mexicali' and len(o[0]) == 1

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("in"), hq.qt("mexicali")]))
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'in mexicali' and len(o[0]) == 1

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("in"), hq.qt(None,nopt=1), hq.qt("bevindt")]))
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'in mexicali bevindt' and len(o[0]) == 1

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("in"), hq.qt(None,nopt=1), hq.qt("bevindt"), hq.qt(None,nopt=1)]))
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'in mexicali bevindt zich' and len(o[0]) == 1

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("in"), hq.qt(value="a|b",nopt=2,oper='!='), hq.qt("zich")]))
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'in mexicali bevindt zich' and len(o[0]) == 1

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("in"), hq.qt(value="a|bevindt",nopt=2,oper='!='), hq.qt("zich")]))
o = q.execute()
assert len(o[0]) == 0

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("in"), hq.qt(None,nopt=31), hq.qt("abc")]))
o = q.execute()
assert len(o[0]) == 0

dirname = td.name.replace('\\','/')
ixname = dirname.rstrip('/').rpartition('/')[2]
print(ixname)

# Two tests using the elastic index 

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("mexicali")]),True,elindex=ixname)
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'mexicali' and len(o[0]) == 1

q = hq.tssQuery(tssShelve,hq.queryUnit([hq.qt("in"), hq.qt(value="a|b",nopt=2,oper='!='), hq.qt("zich")]), \
                elastic=True,elindex=ixname)
o = q.execute()
assert ' '.join(o[0][0].hitString.getwords()) == 'in mexicali bevindt zich' and len(o[0]) == 1

tssShelve.close()

# remove index

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
es.indices.delete(index=ixname, ignore=[400, 404])

# remove temporary directory

td.cleanup()