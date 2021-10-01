from spirit.utils import eprint
from spirit.events import Ref, Store

def main():
    store = Store({
        "a": [
            #lambda e: print(e.data["a"] + 1)
            lambda e: print(e)
        ]
    })
    store.log("test", "Hello, world!")
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

if __name__ == "__main__":
    main()
