from spirit.storage.database import Database
from spirit.utils import Model, UNDEFINED, eprint

from collections import namedtuple
from typing import Union, Optional, List, Any
from uuid import uuid4

NoneType = None.__class__
make_uuid = lambda: uuid4().bytes

class Metadata(Model):
    placeholder: Optional[bool] = None
    size: Optional[int] = None
    #nullable: Optional[bool] = None ; use Optional type to set nullable
    unique: Optional[bool] = None
    primary: Optional[bool] = None
    increment: Optional[bool] = None
    index: Optional[bool] = None
    default: Optional[Any] = UNDEFINED

class Reference(Model):
    table: Model
    field: str
    placeholder: Optional[str] = None
    cascade: Optional[str] = None

class BaseModel(Model):
    uuid: bytes = Metadata(size=16, default=make_uuid)

    def alter(self, **kwargs):
        pass

    def forget(self):
        pass

class MemoryFactory:
    def __init__(self, db, table, model):
        self._db = db
        self._table = table
        self._model = model

    # TODO: Implement features functions
    def remember(self, **entry):
        return None

    def focus(self, entries):
        # remember but with insert_many
        return list()

    def recall(self, entry_id):
        pass

    def forget(self):
        pass

class Memory:
    def __init__(self, path, **preload):
        self._db = Database(path, preload)
        self._tables = dict()

    def _process_model(self, model):
        table = model.__name__.lower()
        if self._tables.get(table):
            return table

        reassign = dict()
        reassign[bool] = "integer"
        reassign[int] = "integer"
        reassign[float] = "real"
        reassign[str] = "text"
        reassign[bytes] = "blob"

        references = list()

        fields = list()
        field_types = model._field_types
        field_defaults = model._field_defaults
        for field_name in model._fields:
            nullable = False

            field_type = field_types.get(field_name)
            if field_type.__class__ is Union.__class__:
                # Extract types from union / optional types
                union_types = field_type.__args__
                field_type = union_types[0]
                if union_types[1] is NoneType:
                    nullable = True

            field_default = field_defaults.get(field_name)
            field_default_type = None
            if field_default is not None:
                field_default_type = field_default.__class__

            assigned_type = reassign.get(field_type, "null")
            attributes = [field_name, assigned_type]
            if field_default_type is Reference:
                ref_table = field_default.table.__name__
                ref_field = field_default.field
                reference = [
                    f"foreign key({field_name})",
                    f"references {ref_table}({ref_field})"
                ]

                cascade = field_default.cascade
                if cascade is not None:
                    reference.append(f"on {cascade} cascade")

                references.append(" ".join(reference))

            if field_default_type is not Metadata:
                fields.append(" ".join(attributes))
                continue

            if field_default.placeholder:
                continue

            if field_default.unique is True:
                attributes.append("unique")

            if field_default.primary is True:
                attributes.append("primary key")

            if field_default.increment is True:
                attributes.append("autoincroment")

            if field_default.index is True:
                attributes.append("autoindex")

            if not nullable:
                attributes.append("not null")

            default = field_default.default
            if default != UNDEFINED:
                if not callable(default):
                    remap = dict()
                    remap[None] = "null"
                    def_value = remap.get(default, default)
                    attributes.append(f"default {def_value}")

            fields.append(" ".join(attributes))

        fields = fields + references
        self._db.create(table, fields)
        self._tables[table] = True
        return table

    def meditate(self, model):
        table = self._process_model(model)
        return MemoryFactory(self._db, table, model)
