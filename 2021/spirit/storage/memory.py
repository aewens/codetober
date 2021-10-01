# NOTE - This file is under construction and only being imported for reference
#        DO NOT USE IN CURRENT STATE WHILE THIS NOTICE IS PRESENT!!!

from spirit.storage.database import Database
from spirit.utils import Model, UNDEFINED, eprint

from collections import namedtuple
from typing import Union, Optional, List, Any
from uuid import uuid4

make_uuid = lambda: uuid4().bytes

class Metadata(Model):
    size: Optional[int] = None
    nullable: Optional[bool] = None
    unique: Optional[bool] = None
    primary: Optional[bool] = None
    increment: Optional[bool] = None
    index: Optional[bool] = None
    default: Optional[Any] = UNDEFINED

class Reference(Model):
    table: Model
    field: str
    cascade: Optional[str] = None

class BaseModel(Model):
    uuid: bytes = Metadata(size=16, nullable=False, default=make_uuid)

class Template:
    def __init__(self, model):
        self._model = model

        self._validators = defaultdict(list)
        self._resolvers = defaultdict(lambda v: v)
        self._nullable = defaultdict(lambda: False)
        self._dependencies = dict()

        self._compute_helpers()
        self._factory = self._create_factory()

    def _compute_helpers(self):
        field_types = self._model._field_types
        field_defaults = self._model._field_defaults
        for name in self._model._fields:
            validator = self._validators[name]

            field_type = field_types.get(name)
            if field_type.__class__ is Union.__class__:
                # Extract types from union / optional types
                field_type = field_type.__args__

            field_default = field_defaults.get(name)
            field_default_type = None
            if field_default is not None:
                field_default_type = field_default.__class__

            assigned_type = reassign.get(field_type, "null")
            attributes = [name, assigned_type]
            if field_default_type is Reference:
                ref_table = field_default.table

                self._resolver[name] = lambda v: getattr(v, "_id", UNDEFINED)
                #validator.append(lambda v: isinstance(v, Model))

                dependency = Template(ref_table)
                self._dependencies[name] = dependency

            if field_default_type is Metadata:
                self._nullable[name] = field_default.nullable
                #if field_default.nullable is False:
                #    validator.append(lambda v: v is not None)

            resolver = self._resolver[name]
            validator.append(lambda v: isinstance(resolver(v), field_type))

    def _create_factory(self, validate=True):
        def factory(**kwargs):
            fields = dict()
            for name in self._model._fields:
                field = kwargs.get(name)
                if validate and not self.validate_field(name, field):
                    return False, None

                dependency = self._dependencies[name]
                if dependency:
                    resolver = self._resolver[name]
                    fields[name] = 

        return factory

    def validate_field(self, key, value):
        validator = self._validators[key]
        for validate in validator:
            valid = validate(value)
            if not valid:
                return False

        return True

    def validate(self, entity):
        for name in model._fields:
            field = getattr(entity, name, None)
            if field is None:
                if self._nullable[name]:
                    continue

                return False

            if not self.validate_field(name, field):
                return False

        return True

    def resolve(self, entity, field_name):
        field = getattr(entity, field_name, None)
        resolver = self._resolvers.get(field)
        if resolver is None:
            return field

        return None if resolver is None else resolver(field)

    def create(self, **kwargs):
        try:
            return self._factory(**kwargs)

        except Exception as e:
            return None

    #def safe_create(self, **kwargs):
    #    entity = self.create(**kwargs)
    #    if entity is None:
    #        return False, None

    #    return self.validate(entity), entity

class Memory:
    def __init__(self, path, models=dict(), **preload):
        self._db = Database(path, preload)
        self._models = dict()
        self._templates = dict()
        self._tables = dict()

        for model in models:
            self.meditate(model)

    def _extract_db_data(self, model):
        table = model.__name__.lower()

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
            field_type = field_types.get(field_name)
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

            if field_default.nullable is False:
                attributes.append("not null")

            if field_default.unique is True:
                attributes.append("unique")

            if field_default.primary is True:
                attributes.append("primary key")

            if field_default.increment is True:
                attributes.append("autoincroment")

            if field_default.index is True:
                attributes.append("autoindex")

            default = field_default.default
            if default != UNDEFINED:
                remap = dict()
                remap[None] = "null"
                attributes.append(f"default {remap.get(default, default)}")

            fields.append(" ".join(attributes))

        return table, fields + references

    def meditate(self, model):
        # NOTE: May be able to remove model_name
        model_name = model.__name__
        table, fields = self._extract_db_data(model)
        self._db.create(table, fields)

        template = self._make_template(model)
        self._templates[table] = template
        self._templates[model_name] = template

        self._models[table] = model
        self._models[model_name] = model

    def focus(self, table, template=None, entries):
        # remember but with insert_many
        pass

    def remember(self, table, validate=True, debug=False, **entry):
        if not validate:
            self._db.insert(table, **data)
            return True

        template = self._template.get(table)
        if template is None:
            if debug:
                eprint(f"No tempate for {table} to validate with")

            return False

        valid = template.validate(entry)

    def recall(self):
        pass

    def alter(self):
        pass

    def forget(self):
        pass

    def export(self, fns):
        return tuple(getattr(self, fn, None) for fn in fns)

