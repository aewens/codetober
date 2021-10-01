from spirit.events.ref import Ref

from typing import NamedTuple, Any
from datetime import datetime, timedelta

class Event(NamedTuple):
    kind: str
    when: datetime
    data: Any

class Store:
    def __init__(self, listeners=dict(), debug=False):
        self._listeners = listeners
        self._events = list()
        self._ref = Ref(self)

        self._stats = dict()
        self._stats["start"] = datetime.utcnow()
        self._stats["events"] = 0

        self._debug = debug

    @property
    def ref(self):
        return self._ref

    @property
    def events(self):
        for event in self._events:
            if event is None:
                continue

            yield event

    def use(self, key):
        value = self._ref[key]
        change = lambda v: self._ref.update({key: v})
        return value, change

    def silence(self, kind, clean=False):
        listeners = self._listeners.pop(kind, None)
        if clean:
            self.log("CLEAN", kind)

        return listeners

    def subscribe(self, kinds, listener):
        if not callable(listener):
            return None

        patched = kinds + ["CLEAN"]
        for kind in patched:
            listeners = self._listeners.get(kind, list())
            if listener not in listeners:
                listeners.append(listener)

            self._listeners[kind] = listeners

        return self.unsubscribe(kinds, listener)

    def unsubscribe(self, kinds, listener):
        def execute():
            for kind, listeners in self._listeners.items():
                if kind in kinds:
                    listeners.remove(listener)

        return execute

    def process(self, event):
        if self._debug:
            print(event.when, event.kind, event.data)

        for kind, listeners in self._listeners.items():
            if event.kind == kind:
                for listener in listeners:
                    listener(event)

    def get_events(self, after=timedelta(0)):
        if isinstance(after, timedelta):
            after = datetime.utcnow() - after

        for event in self.events:
            if event.when >= after:
                yield event

    def log(self, kind, data):
        when = datetime.utcnow()
        event = Event(kind, when, data)
        self.process(event)

        self._events.append(event)
        self._stats["events"] = self._stats["events"] + 1

    def replay(self, after=timedelta(0)):
        events = 0
        for event in self.get_events(after):
            self.process(event)
            events = events + 1

        return events

