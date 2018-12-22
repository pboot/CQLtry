import CQLtry.taggedString as ts
import CQLtry.handleQuery as hq
import CQLtry.cqlutil as cu

tsqh = hq.tsQueryHelper('lc')

out = tsqh.translate(' "naam" ')
print(out)
assert out == [hq.qt(value='naam', attribute='lc', oper='=')]
assert out.anchored == None

out = tsqh.translate('<s>"naam" ')
print(out)
assert out == [hq.qt(value='naam', attribute='lc', oper='=')]
assert out.anchored == 'start'

out = tsqh.translate('[word!="rommel"]')
print(out)
assert out == [hq.qt(value='rommel', attribute='word', oper='!=')]

out = tsqh.translate(' "naam" [word="rommel"] "geen" ')
print(out)
assert out == [hq.qt(value='naam', attribute='lc', oper='='), hq.qt(value='rommel', attribute='word', oper='='), hq.qt(value='geen', attribute='lc', oper='=')]

out = tsqh.translate(' "naam" [word="rommel"]+ "geen" ')
print(out)
assert out == [hq.qt(value='naam', attribute='lc', nopt=None, oper='='), hq.qt(value='rommel', attribute='word', nopt=None, oper='='), hq.qt(value='rommel', attribute='word', nopt=9998, oper='='), hq.qt(value='geen', attribute='lc', nopt=None, oper='=')]

out = tsqh.translate(' "naam" [word="rommel"]{1,10} "geen" ')
print(out)
assert out == [hq.qt(value='naam', attribute='lc', nopt=None, oper='='), hq.qt(value='rommel', attribute='word', nopt=None, 
               oper='='), hq.qt(value='rommel', attribute='word', nopt=9, oper='='), 
               hq.qt(value='geen', attribute='lc', nopt=None, oper='=')]

out = tsqh.translate(' "naam" [word="rommel"]* "geen" ')
print(out)
assert out == [hq.qt(value='naam', attribute='lc', nopt=None, oper='='), 
               hq.qt(value='rommel', attribute='word', nopt=9999, oper='='), 
               hq.qt(value='geen', attribute='lc', nopt=None, oper='=')]

out = tsqh.translate(' "naam" [word="rommel"]? "geen" ')
print(out)
assert out == [hq.qt(value='naam', attribute='lc', nopt=None, oper='='), 
               hq.qt(value='rommel', attribute='word', nopt=1, oper='='), 
               hq.qt(value='geen', attribute='lc', nopt=None, oper='=')]

out = tsqh.translate('[word="rommel"]{0,10}')
print(out)
assert out == [hq.qt(value='rommel', attribute='word', nopt=10, oper='=')]

out = tsqh.translate('[word="rommel"]{2,2}')
print(out)
assert out == [hq.qt(value='rommel', attribute='word', nopt=None, oper='='), 
               hq.qt(value='rommel', attribute='word', nopt=None, oper='=')]

out = tsqh.translate('[word="rommel"]{1,1}')
print(out)
assert out == [hq.qt(value='rommel', attribute='word', nopt=None, oper='=')]

out = tsqh.translate('[]?')
print(out)
assert out == [hq.qt(value=None, attribute='word', nopt=1)]

out = tsqh.translate('"de" []? "man"')
print(out)
assert out == [hq.qt(value='de', attribute='lc', nopt=None, oper='='), 
               hq.qt(value=None, attribute='word', nopt=1), 
               hq.qt(value='man', attribute='lc', nopt=None, oper='=')]

try:
    out = tsqh.translate(' "naam')
except cu.InputError as error:
    print(error)
    assert str(error) == "unmatched double quotation marks"
    pass

try:
    out = tsqh.translate('"boek"]')
except cu.InputError as error:
    print(error)
    assert str(error) == "Closing bracket ']' that was not opened"
    pass

try:
    out = tsqh.translate('[lc = "boek"')
except cu.InputError as error:
    print(error)
    assert str(error) == "Opening bracket '[' has not been closed"
    pass

try:
    out = tsqh.translate('"boek"*')
except cu.InputError as error:
    print(error)
    assert str(error) == "Range not allowed here: character * at pos 6"
    pass

try:
    out = tsqh.translate('[lc = "boek"]}')
except cu.InputError as error:
    print(error)
    assert str(error) == "Closing bracket '}' that was not opened"
    pass

try:
    out = tsqh.translate('[lc = "boek"] /')
except cu.InputError as error:
    print(error)
    assert str(error) == "Unexpected character / at pos 11"
    pass

try:
    out = tsqh.translate('[lc = "boek"]{a,3}')
except cu.InputError as error:
    print(error)
    assert str(error) == "Range minimum non numeric or empty: a"
    pass

try:
    out = tsqh.translate('[lc = "boek"]{999,}')
except cu.InputError as error:
    print(error)
    assert str(error) == "Range maximum non numeric or empty: "
    pass

try:
    out = tsqh.translate('[lc = "boek"]{9,8}')
except cu.InputError as error:
    print(error)
    assert str(error) == "Range maximum less than range minimum: min 9, max 8"
    pass

try:
    out = tsqh.translate('[lc = "boek"]{0,0}')
except cu.InputError as error:
    print(error)
    assert str(error) == "Range maximum must be higher than zero"
    pass

try:
    out = tsqh.translate('[lc ! "boek"]')
except cu.InputError as error:
    print(error)
    assert str(error) == """Unexpected operator " at pos 4"""
    pass

try:
    out = tsqh.translate('      ')
except cu.InputError as error:
    print(error)
    assert str(error) == "CQL string empty"
    pass

try:
    out = tsqh.translate('"naam"<')
except cu.InputError as error:
    print(error)
    assert str(error) == "Anchor only allowed in position 0, found at position 6"
    pass

try:
    out = tsqh.translate('<g>')
except cu.InputError as error:
    print(error)
    assert str(error) == "Anchor must be <s>"
    pass

try:
    out = tsqh.translate('<s>')
except cu.InputError as error:
    print(error)
    assert str(error) == "No text found after anchor"
    pass
