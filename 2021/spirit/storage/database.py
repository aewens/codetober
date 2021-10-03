from spirit.utils import UNDEFINED, eprint

from typing import NamedTuple
from enum import Enum

from pathlib import Path
from threading import Lock
from contextlib import closing, contextmanager
from traceback import format_exc
from sqlite3 import connect, PARSE_DECLTYPES, PARSE_COLNAMES, IntegrityError

class Database:
    def __init__(self, path, tables, indices=dict(), lock=Lock(), debug=False):
        self._path = path
        self._lock = lock
        self._debug = debug
        self._conn = None
        # NOTE: current use case of :memory: does not really work here
        #if self._path == ":memory:" or not Path(self._path).exists():
        if not Path(self._path).exists():
            self.create_many(tables)
            self.create_indices(**indices)

    @contextmanager
    def _connection(self):
        settings = dict()
        settings["check_same_thread"] = False
        settings["isolation_level"] = "DEFERRED"
        settings["detect_types"] = PARSE_DECLTYPES | PARSE_COLNAMES

        # Open connection
        #print(self._path)
        self._conn = connect(str(self._path), **settings)

        try:
            # Give cotnrol back to caller
            yield

        except Exception as e:
            raise

        else:
            self._conn.close()

        self._conn = None

    @contextmanager
    def _transaction(self):
        with self._lock:
            # Initiate auto-commit mode
            self._conn.execute("BEGIN")
            try:
                # Give control back to caller
                yield

            except Exception as e:
                # Rollback on failure
                self._conn.rollback()
                raise

            else:
                # Commit on success
                self._conn.commit()

    def close(self):
        self._conn.close()

    def try_exec(self, cursor, args):
        try:
            cursor.execute(*args)
            return False

        except IntegrityError as e:
            err = format_exc() if self._debug else str(e)
            eprint(err)
            return err

    def execute(self, command, *args, fetch=False, default=None, commit=False):
        with self._connection():
            with closing(self._conn.cursor()) as cursor:
                #cursor.execute(f"USE {self._database};")
                arguments = (command, args) if len(args) > 0 else (command,)
                err = False
                if commit:
                    with self._transaction():
                        err = self.try_exec(cursor, arguments)

                else:
                    err = self.try_exec(cursor, arguments)

                if fetch:
                    if err:
                        return default

                    if "insert" == command.split()[0].lower():
                        return cursor.lastrowid

                    result = cursor.fetchall()
                    if not result or len(result) == 0:
                        return default

                    return result

                else:
                    return cursor

        return default

    def execute_many(self, command, params):
        with self._connection():
            with closing(self._conn.cursor()) as cursor:
                #cursor.execute(f"USE {self._database};")
                with self._transaction():
                    cursor.executemany(command, params)

    def get_one(self, result):
        if result is None:
            return result

        return result[0] if len(result) > 0 else None

    def read(self, command, *args, default=None):
        kwargs = dict()
        kwargs["fetch"] = True
        kwargs["default"] = default
        kwargs["commit"] = False
        return self.execute(command, *args, **kwargs)

    def read_one(self, command, *args, default=None):
        result = self.read(command, *args, default=default)
        return self.get_one(result)

    def write(self, command, *args, **kwargs):
        kwargs["fetch"] = kwargs.get("fetch", False)
        kwargs["commit"] = True
        return self.execute(command, *args, **kwargs)

    def write_many(self, command, params):
        return self.execute_many(command, params)

    def insert(self, table, **kwargs):
        keys = ",".join(kwargs.keys())
        values = ",".join("?" * len(kwargs))
        statement = f"INSERT INTO {table} ({keys}) VALUES ({values});"
        args = tuple(kwargs.values())
        return self.write(statement, *args, fetch=True)

    def insert_many(self, table, fields, inserts):
        params = list()
        keys = ",".join(fields)
        values = ",".join("?" * len(fields))
        statement = f"INSERT INTO {table} ({keys}) VALUES ({values});"
        for insert in inserts:
            args = list()
            for field in fields:
                args.append(insert.get(field))
            params.append(tuple(args))

        self.write_many(statement, params)

    def update(self, table, where=dict(), **kwargs):
        variables = ",".join(f"{k} = ?" for k in kwargs.keys())
        clause = " AND ".join(f"{k} = ?" for k in where.keys())
        statement = f"UPDATE {table} SET {variables} WHERE {clause};"
        values = list(kwargs.values())
        conditions = list(where.values())
        args = tuple(values + conditions)
        self.write(statement, *args)

    def select(self, table, keys=True, where=dict(), default=None):
        fields = "*" if keys is True else ",".join(keys)
        clause = ""
        args = tuple()
        if len(where) > 0:
            clause = " WHERE " + " AND ".join(f"{k} = ?" for k in where.keys())
            args = tuple(where.values())

        query = f"SELECT {fields} FROM {table}{clause};"
        #DEBUG
        #print(query)
        return self.read(query, *args)

    def select_one(self, table, keys=True, where=dict(), default=None):
        result = self.select(table, keys=keys, where=where, default=default)
        return self.get_one(result)

    def delete(self, table, where=dict()):
        clause = " AND ".join(f"{k} = ?" for k in where.keys())
        args = tuple(where.values())
        query = f"DELETE FROM {table}{clause};"
        self.write(query, *args)

    def destroy(self, tables):
        for table in tables:
            self.write(f"DROP TABLE {table};")

    def create(self, table, fields):
        table_id = "id integer primary key"
        props = ", ".join([table_id] + fields)
        #self._conn.execute("PRAGMA journal_mode=wal;")
        self.write(f"create table if not exists {table} ({props});")

    def create_many(self, tables):
        #self._conn.execute("PRAGMA journal_mode=wal;")
        for table, fields in tables.items():
            self.create(table, fields)

    def create_index(self, table, field):
        #self._conn.execute("PRAGMA journal_mode=wal;")
        index = f"{table}_{field}"
        self.write(f"create index if not exists {index} on {table}({field});")

    def create_indices(self, **indices):
        #self._conn.execute("PRAGMA journal_mode=wal;")
        for table, fields in indices.items():
            if not isinstance(fields, list):
                fields = [fields]

            for field in fields:
                self.create_index(table, field)

    @staticmethod
    def field(name, kind, size=None, nullable=False, default=UNDEFINED):
        result = [name, kind if size is None else f"{kind}({size})"]
        if not nullable:
            result.append("not NULL")

        if default != UNDEFINED:
            if default is None:
                default = "NULL"

            result.append(f"default {default}")

        return " ".join(result)

    @staticmethod
    def reference(key, table, field, cascade=False):
        result = f"FOREIGN KEY({key}) REFERENCES {table}({field})"
        if cascade:
            result = f"{result} ON {cascade.upper()} CASCADE"

        return result
