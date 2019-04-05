from yapic.entity._entity cimport EntityType
from yapic.entity._field cimport Field, PrimaryKey, ForeignKey, collect_foreign_keys
from yapic.entity._field_impl cimport StorageType

from ._dialect cimport Dialect


cdef class DDLCompiler:
    def __cinit__(self, Dialect dialect):
        self.dialect = dialect

    def compile_entity(self, EntityType entity):
        cdef list elements = []
        cdef list table_parts = []
        cdef list requirements = []

        table_parts.append(f"CREATE TABLE {self.dialect.table_qname(entity)} (\n")

        length = len(entity.__fields__)
        for i, field in enumerate(entity.__fields__):
            elements.append(self.compile_field(<Field>field, length - 1 <= i, requirements))

        cdef dict fks = collect_foreign_keys(entity)
        if fks:
            elements.extend(self.compile_foreign_keys(fks))

        if elements:
            table_parts.append("  ")
            table_parts.append(",\n  ".join(elements))
            table_parts.append("\n")

        table_parts.append(");")

        if requirements:
            requirements.append("")
            table_parts.insert(0, "\n".join(requirements))

        return "".join(table_parts)

    def compile_field(self, Field field, bint is_last, list requirements):
        cdef StorageType type = self.gues_type(field)
        if type is None:
            raise ValueError("Cannot determine the sql type of %r" % field)

        req = type.requirements()
        if req:
            if not isinstance(req, str):
                raise TypeError("StorageType.requirements must returns with str or None")
            requirements.append(req)

        cdef str res = f"{self.dialect.quote_ident(field._name_)} {type.name}"

        if field.get_ext(PrimaryKey):
            res += " PRIMARY KEY"

        if not field.nullable:
            res += " NOT NULL"

        return res

    def compile_foreign_keys(self, dict fks):
        cdef str res
        cdef int length
        cdef list foreign_keys
        cdef ForeignKey fk

        for key_name, foreign_keys in fks.items():
            res = f"CONSTRAINT {self.dialect.quote_ident(key_name)} FOREIGN KEY ("

            length = len(foreign_keys)
            for i in range(length):
                fk = foreign_keys[i]
                res += self.dialect.quote_ident(fk.attr._name_)
                if i != length - 1:
                    res += ", "

            res += f") REFERENCES {self.dialect.table_qname(foreign_keys[0].ref._entity_)} ("

            for i in range(length):
                fk = foreign_keys[i]
                res += self.dialect.quote_ident(fk.ref._name_)
                if i != length - 1:
                    res += ", "

            res += ")"

            yield res

    cpdef StorageType guess_type(self, Field field):
        raise NotImplementedError()
