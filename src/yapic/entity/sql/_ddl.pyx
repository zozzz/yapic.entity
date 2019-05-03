from yapic.entity._entity cimport EntityType
from yapic.entity._entity_diff cimport EntityDiff
from yapic.entity._entity_diff import EntityDiffKind
from yapic.entity._registry cimport RegistryDiff
from yapic.entity._registry import RegistryDiffKind
from yapic.entity._field cimport Field, PrimaryKey, ForeignKey, collect_foreign_keys, StorageType
from yapic.entity._expression cimport Expression

from ._dialect cimport Dialect


cdef class DDLCompiler:
    def __cinit__(self, Dialect dialect):
        self.dialect = dialect

    def compile_entity(self, EntityType entity):
        cdef list elements = []
        cdef list table_parts = []
        cdef list requirements = []
        is_type = entity.__meta__.get("is_type", False) is True

        if is_type is True:
            table_parts.append(f"CREATE TYPE {self.dialect.table_qname(entity)} AS (\n")
        else:
            table_parts.append(f"CREATE TABLE {self.dialect.table_qname(entity)} (\n")

        length = len(entity.__fields__)
        for i, field in enumerate(entity.__fields__):
            elements.append(self.compile_field(<Field>field, requirements))

        primary_keys = entity.__pk__
        if primary_keys:
            if is_type:
                raise ValueError("Primary Key is not supported on Composite Types")
            pk_names = [self.dialect.quote_ident(pk._name_) for pk in primary_keys]
            elements.append(f"PRIMARY KEY({', '.join(pk_names)})")

        cdef dict fks = collect_foreign_keys(entity)
        if fks:
            if is_type:
                raise ValueError("Foreign keys is not supported on Composite Types")
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

    def compile_field(self, Field field, list requirements):
        cdef StorageType type = self.dialect.get_field_type(field)
        if type is None:
            raise ValueError("Cannot determine the sql type of %r" % field)

        # req = type.requirements()
        # if req is not None:
        #     if not isinstance(req, str):
        #         raise TypeError("StorageType.requirements must returns with str or None")
        #     requirements.append(req)

        cdef str res = f"{self.dialect.quote_ident(field._name_)} {type.name}"

        if not field.nullable:
            res += " NOT NULL"

        if field._default_ is not None:
            if isinstance(field._default_, Expression):
                qc = self.dialect.create_query_compiler()
                res += f" DEFAULT {qc.visit(field._default_)}"
            else:
                res += f" DEFAULT {self.dialect.quote_value(type.encode(field._default_))}"

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

            res += f" ON UPDATE {foreign_keys[0].on_update} ON DELETE {foreign_keys[0].on_delete}"

            yield res

    def compile_registry_diff(self, RegistryDiff diff):
        lines = []
        schemas_created = {ent.__meta__.get("schema", "public") for ent in diff.a.values()}

        for kind, param in diff:
            if kind is RegistryDiffKind.REMOVED:
                lines.append(self.drop_entity(param))
            elif kind is RegistryDiffKind.CREATED:
                schema = param.__meta__.get("schema", "public")
                if schema not in schemas_created:
                    schemas_created.add(schema)
                    lines.append(f"CREATE SCHEMA IF NOT EXISTS {self.dialect.quote_ident(schema)};")

                lines.append(self.compile_entity(param))
            elif kind is RegistryDiffKind.CHANGED:
                if param.b.__meta__.get("is_type", False) is True:
                    lines.append(self.compile_type_diff(param))
                else:
                    lines.append(self.compile_entity_diff(param))
            elif kind is RegistryDiffKind.INSERT_ENTITY:
                lines.append(self.dialect.compile_insert(param[0], param[1]))
            elif kind is RegistryDiffKind.UPDATE_ENTITY:
                lines.append(self.dialect.compile_update(param[0], param[1]))
            elif kind is RegistryDiffKind.REMOVE_ENTITY:
                lines.append(self.dialect.compile_delete(param[0], param[1]))

        return "\n".join(lines)

    def compile_entity_diff(self, EntityDiff diff):
        requirements = []
        alter = []

        for kind, param in diff:
            if kind == EntityDiffKind.REMOVED:
                alter.append(f"DROP COLUMN {self.dialect.quote_ident(param._name_)}")
            elif kind == EntityDiffKind.CREATED:
                alter.append(f"ADD COLUMN {self.compile_field(param, requirements)}")
            elif kind == EntityDiffKind.CHANGED:
                alter.extend(self.compile_field_diff(param[1], param[2]))

        if alter:
            alter = ',\n  '.join(alter)
            return f"ALTER TABLE {self.dialect.table_qname(diff.b)}\n  {alter};"
        else:
            return ""

    def compile_type_diff(self, EntityDiff diff):
        requirements = []
        alter = []

        for kind, param in diff:
            if kind == EntityDiffKind.REMOVED:
                alter.append(f"DROP ATTRIBUTE {self.dialect.quote_ident(param._name_)}")
            elif kind == EntityDiffKind.CREATED:
                alter.append(f"ADD ATTRIBUTE {self.compile_field(param, requirements)}")
            elif kind == EntityDiffKind.CHANGED:
                field = param[1]
                changes = param[2]
                if "_impl_" in changes or "size" in changes:
                    type = self.dialect.get_field_type(field)
                    col_name = self.dialect.quote_ident(field._name_)
                    if type is None:
                        raise ValueError("Cannot determine the sql type of %r" % field)
                    alter.append(f"ALTER ATTRIBUTE {col_name} TYPE {type.name}")

        if alter:
            alter = ',\n  '.join(alter)
            return f"ALTER TYPE {self.dialect.table_qname(diff.b)}\n  {alter};"
        else:
            return ""

    def compile_field_diff(self, Field field, dict diff):
        cdef StorageType type = self.dialect.get_field_type(field)
        cdef str col_name = self.dialect.quote_ident(field._name_)
        result = []

        if type is None:
            raise ValueError("Cannot determine the sql type of %r" % field)

        if "_impl_" in diff or "size" in diff:
            result.append(f"ALTER COLUMN {col_name} TYPE {type.name} USING {col_name}::{type.name}")

        if "nullable" in diff:
            if diff["nullable"]:
                result.append(f"ALTER COLUMN {col_name} DROP NOT NULL")
            else:
                result.append(f"ALTER COLUMN {col_name} SET NOT NULL")

        if "_default_" in diff:
            if diff["_default_"] is None:
                result.append(f"ALTER COLUMN {col_name} DROP DEFAULT")
            else:
                if isinstance(diff["_default_"], Expression):
                    qc = self.dialect.create_query_compiler()
                    default = qc.visit(diff['_default_'])
                else:
                    default = self.dialect.quote_value(type.encode(diff["_default_"]))
                result.append(f"ALTER COLUMN {col_name} SET DEFAULT {default}")

        return result

    def drop_entity(self, EntityType entity):
        if entity.__meta__.get("is_type", False) is True:
            return f"DROP TYPE {self.dialect.table_qname(entity)} CASCADE;"
        else:
            return f"DROP TABLE {self.dialect.table_qname(entity)} CASCADE;"


cdef class DDLReflect:
    def __cinit__(self, EntityType entity_base):
        self.entity_base = entity_base

    async def get_entities(self, Connection conn, Registry registry):
        raise NotImplementedError()
