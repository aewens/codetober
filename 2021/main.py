from spirit.utils import eprint
from spirit.events import Ref, Store
from spirit.storage import Memory, BaseModel, Metadata, Reference

from typing import Optional
from datetime import datetime

now = lambda: datetime.now().timestamp()

class Author(BaseModel):
    name: Optional[str]

class Note(BaseModel):
    created: int = Metadata(default=now)
    updated: int = Metadata(default=now)
    author: Author = Metadata(placeholder=True)
    author_id: int = Reference(table=Author, field="id", placeholder="author")
    content: str

def main():
    store = Store({
        "a": [
            lambda e: print(e)
        ]
    })

    store.log("a", {"a": 0})

    ref = Ref(store)
    q = {"a": 1}
    ref["a"] = q
    q["a"] = 2
    ref["a"] = q
    p = ref["a"]
    p["a"] = 1
    ref["b"] = p
    assert ref["a"] != ref["b"], "Data is not immutable"
    p2 = ref.pop("b")
    assert p is not p2, "Data is not immutable"

    mem = Memory(":memory:")
    author_mem = mem.meditate(Author)
    note_mem = mem.meditate(Note)

    author_id = author_mem.remember(name="First Last")
    author = author_mem.recall(author_id)

    note_ids = note_mem.focus([
        {"author": author, "content": "Hello, world!"},
        {"author": author, "content": "Lorem ipsum"}
    ])
    #note = note_mem.recall(note_ids[0])

    #assert isinstance(author, Author)
    #assert isinstance(note, Note)

    #note.alter(content="Hello, universe!")

    #note.forget()
    #note_mem.forget(note_ids[1])

if __name__ == "__main__":
    main()
