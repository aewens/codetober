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

    mem = Memory("/tmp/data.db")
    author_mem = mem.meditate(Author)
    note_mem = mem.meditate(Note)

    author_id = author_mem.remember(name="First Last")
    print(author_id)
    assert isinstance(author_id, int) and author_id > 0, "Author ID is not uint"

    author = author_mem.recall(author_id)
    print(author)
    assert isinstance(author, Author), "Invalid author recall value"
    assert author.id == author_id, "Author ID not set properly"

    note_ids = note_mem.focus([
        {"author": author, "content": "Hello, world!"},
        {"author": author, "content": "Lorem ipsum"}
    ])
    print(note_ids)
    assert isinstance(note_ids, list), "Invalid type for note ids"
    assert len(note_ids) == 2, "Invalid count of note ids"

    note = note_mem.recall(note_ids[0])
    print(note)
    assert isinstance(note, Note), "Invalid note recall value"
    assert note.id == note_ids[0], "Note ID not set properly"
    assert note.author_id == author_id, "Invalid author ID for note"
    assert note.author == author, "Invalid author value for note"

    #assert isinstance(author, Author)
    #assert isinstance(note, Note)

    #note.alter(content="Hello, universe!")

    #note.forget()
    #note_mem.forget(note_ids[1])

if __name__ == "__main__":
    main()
