from collections import OrderedDict
from typing import NamedTuple, NamedTupleMeta

UNDEFINED = "__UNDEFINED__"

class ModelMeta(NamedTupleMeta):
    def __new__(mcs, typename, bases, namespace):
        # DEBUG
        #print(mcs)
        #print(typename)
        #print(bases)
        #print(namespace)

        if namespace.get("_root") is not None:
            # The created class is Model, skip
            return super().__new__(mcs, typename, bases, namespace)

        fields = OrderedDict()
        field_sources = dict()
        field_defaults = dict()
        curr_fields = namespace.get("__annotations__", dict())

        for base in bases:
            base_fields = getattr(base, "_fields", None)
            base_defaults = getattr(base, "_field_defaults", dict())
            base_annotations = getattr(base, "__annotations__", dict())
            if not issubclass(base, Model) or base_fields is None:
                continue

            for field in base_fields:
                if curr_fields.get(field) is not None:
                    continue

                if fields.get(field) is not None:
                    source = field_sources[field]
                    raise TypeError(f"{base}.{field} conflicts with {source}")

                fields[field] = base_annotations[field]
                field_sources[field] = base
                default = base_defaults.get(field, UNDEFINED)
                if default != UNDEFINED:
                    field_defaults[field] = default

        fields.update(curr_fields)
        if len(fields) == 0:
            raise ValueError("Model must contain at least one field")

        for key, value in field_defaults.items():
            namespace.setdefault(key, value)

        # NOTE: Here be dragons

        # Place fields with default values at the end
        default_fields = [field for field in fields if field in namespace]
        value_fields = set(fields).difference(default_fields)
        ordered_fields = (sorted(value_fields) + sorted(default_fields))

        namespace["__annotations__"] = OrderedDict([
            (field, fields[field]) for field in ordered_fields
        ])

        # Allows NamedTupleMeta to create annotations
        template = super().__new__(mcs, typename, None, namespace)

        # Hack to modify __new__ to be keyword arguments only
        field_args = "".join(field + "," for field in ordered_fields)
        rewrite = f"""
        def __new__(_cls, *args, {field_args}):
            if len(args) > 0:
                raise TypeError("Model uses keyword arguments only")

            return _tuple_new(_cls, ({field_args}))
        """
        rewrite_env = dict()
        rewrite_env["_tuple_new"] = tuple.__new__
        rewrite_env["__name__"] = f"namedtuple_{typename}"

        # Do never ever actually do this; Do as I say, not as I do
        exec(rewrite.strip(), rewrite_env)

        # Hard-wire this abomination into something we can use
        __new__ = rewrite_env["__new__"]
        __new__.__qualname__ = f"{typename}.__new__"
        __new__.__doc__ = template.__new__.__doc__
        __new__.__annotations__ = template.__new__.__annotations__
        __new__.__kwdefaults__ = {df: namespace[df] for df in default_fields}

        # I will atone for this later, for now be grateful this works
        template.__new__ = __new__

        # NOTE: For the code above, you may want to avert your eyes

        template_bases = template.__bases__
        bases = bases + template_bases

        new_namespace = template.__dict__.copy()
        new_namespace["_bases"] = bases

        model_type = type.__new__(mcs, typename, template_bases, new_namespace)

        # Modifying __bases__ triggers MRO
        # This happens *after* class creation
        model_type.__bases__ = tuple(template_bases)
        return model_type

    def mro(cls):
        default_mro = super().mro()
        bases = getattr(cls, "_bases", None)
        if bases is None:
            return default_mro

        # default_mro should be [cls, tuple, object]
        sequences = [
            default_mro[:1],
            *[base.__mro__ for base in bases],
            default_mro[1:]
        ]

        # Create copy
        seqs = [list(seq) for seq in sequences]

        # c3merge from C3 linearization algorithm
        new_default_mro = list()
        while True:
            # Remove blanks
            seqs = [seq for seq in seqs if seq]
            if not seqs:
                return new_default_mro

            for seq in seqs:
                head = seq[0]
                if not any(head in s[1:] for s in seqs):
                    break

            else:
                raise TypeError("Inconsistent hierarchy")

            new_default_mro.append(head)
            for seq in seqs:
                if seq[0] == head:
                    del seq[0]

        return new_default_mro

class Model(metaclass=ModelMeta):
    _root = True
    _meta = dict()

    def __new__(cls, *args, **kwargs):
        if cls is Model:
            raise TypeError("Model can only be used as a base class")

        return super().__new__(cls, *args, **kwargs)
