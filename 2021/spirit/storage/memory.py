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

    @property
    def id(self):
        return self._meta.get("id")

    def assign(self, model_id):
        self._meta["id"] = model_id

    def alter(self, **kwargs):
        pass

    def forget(self):
        pass

class MemoryFactory:
    def __init__(self, mem, model, table, template):
        self._mem = mem
        self._db = self._mem._db
        self._model = model
        self._table = table
        self._template = template

    # TODO: Implement features functions
    def remember(self, **entry):
        # Apply default values from callables
        for key, value in self._template["defaults"].items():
            if entry.get(key) is not None:
                continue

            entry[key] = value()

        for field, placeholder in self._template["placeholders"].items():
            value = entry.pop(placeholder, None)
            if value is not None:
                entry[field] = value.id

        entry_id = self._db.insert(self._table, **entry)
        return entry_id

    def focus(self, entries, uuids=False):
        """
        Implements remember but for multiple entries
        """
        entry_ids = dict() if uuids else list()
        for entry in entries:
            # generating uuid here so we know it ahead of time
            if uuids:
                uuid = make_uuid()
                entry["uuid"] = uuid

            # NOTE - using insert over insert_many to get entry id
            entry_id = self.remember(**entry)
            if uuids:
                entry_ids[uuid] = entry_id

            else:
                entry_ids.append(entry_id)

        return entry_ids

    def recall(self, entry_id):
        keys = list()
        model_keys = self._model._fields

        placeholders = self._template["placeholders"]
        placeholder_fields = list(placeholders.values())
        for key in model_keys:
            if key in placeholder_fields:
                continue

            keys.append(key)

        where = dict()
        where["id"] = entry_id
        values = self._db.select_one(self._table, keys=keys, where=where)
        if values is None:
            return None

        fields = dict()
        for i, key in enumerate(keys):
            value = values[i]
            fields[key] = value

        for field, placeholder in placeholders.items():
            placeholder_id = fields.get(field)
            if placeholder_id is None:
                continue

            model = self._template["dependencies"].get(placeholder)
            if model is None:
                continue

            factory = self._mem.meditate(model)
            placeholder_entry = factory.recall(placeholder_id)
            fields[placeholder] = placeholder_entry

        entry = self._model(**fields)
        entry.assign(entry_id)
        return entry

    def forget(self):
        pass

class Memory:
    def __init__(self, path, **preload):
        self._db = Database(path, preload)
        self._tables = dict()
        self._templates = dict()

    def _process_model(self, model):
        table = model.__name__.lower()
        template = self._templates.get(table, dict())
        if self._tables.get(table):
            return table, template

        defaults = template["defaults"] = dict()
        placeholders = template["placeholders"] = dict()
        dependencies = template["dependencies"] = dict()

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

                placeholder = field_default.placeholder
                if placeholder is not None:
                    placeholders[field_name] = placeholder

            if field_default_type is not Metadata:
                fields.append(" ".join(attributes))
                continue

            if field_default.placeholder:
                dependencies[field_name] = field_type
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
                if callable(default):
                    defaults[field_name] = default

                else:
                    remap = dict()
                    remap[None] = "null"
                    def_value = remap.get(default, default)
                    attributes.append(f"default {def_value}")

            fields.append(" ".join(attributes))

        fields = fields + references
        self._db.create(table, fields)
        self._tables[table] = True
        self._templates[table] = template
        return table, template

    def meditate(self, model):
        table, template = self._process_model(model)
        return MemoryFactory(self, model, table, template)
