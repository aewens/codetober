from collections.abc import MutableMapping
from pickle import loads, dumps

class Frozen:
    def __init__(self, value):
        self.set_value(value)

    @property
    def value(self):
        return loads(self._value)

    def set_value(self, value):
        self._value = dumps(value)

class Ref(MutableMapping):
    # Using freeze will make data immutable, but also slow operations
    def __init__(self, store, freeze=True):
        self._store = store
        self._freeze = freeze
        self._refs = dict()

    def __getitem__(self, key):
        #return self._transform(self._refs.get(key))
        ref = self._refs.get(key)
        if self._freeze:
            return ref.value

        return ref

    def __setitem__(self, key, value):
        #value = self._transform(value)
        #self._refs[key] = value
        if self._freeze:
            ref = self._refs.get(key)
            if ref:
                ref.set_value(value)

            else:
                self._refs[key] = Frozen(value)

        else:
            self._refs[key] = value

        self._store.log(key, value)

    def __delitem__(self, key):
        del self._refs[key]
        self._store.silence(key, clean=True)

    def __iter__(self):
        return iter(self._refs)

    def __len__(self):
        return len(self._refs)

    def _transform(self, value):
        if isinstance(value, dict):
            return {**value}

        if isinstance(value, list):
            return [*value]

        return value

    def pop(self, key, default=None):
        value = self[key]
        del self[key]
        return default if value is None else value
