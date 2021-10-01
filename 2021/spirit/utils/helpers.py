from sys import exit, stderr
from collections import defaultdict, namedtuple

def eprint(*args, code=0, **kwargs):
    print(*args, file=stderr, **kwargs)
    if code > 0:
        exit(code)

def infinitedict():
    return defaultdict(infinitedict)

def freezedict(value):
    assert isinstance(value, dict), "Can only freeze dictionary"
    FrozenDict = namedtuple("FrozenDict", value)
    return FrozenDict(**value)
