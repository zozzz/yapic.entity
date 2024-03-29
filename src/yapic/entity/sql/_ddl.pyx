from yapic import json

from yapic.entity._entity cimport EntityType, EntityAttributeExt, EntityAttributeExtGroup
from yapic.entity._entity_diff cimport EntityDiff
from yapic.entity._entity_diff import EntityDiffKind
from yapic.entity._registry cimport RegistryDiff
from yapic.entity._registry import RegistryDiffKind
from yapic.entity._field cimport Field, PrimaryKey, ForeignKey, Index, Check, Unique, AutoIncrement, StorageType
from yapic.entity._expression cimport Expression
from yapic.entity._trigger cimport Trigger

from ._dialect cimport Dialect


cdef class DDLCompiler:
    def __cinit__(self, Dialect dialect):
        self.dialect = dialect

    def compile_entity(self, EntityType entity):
        cdef list elements = []
        cdef list table_parts = []
        cdef list requirements = []
        cdef list deferred = []
        is_type = entity.get_meta("is_type", False) is True
        is_sequence = entity.get_meta("is_sequence", False) is True

        if is_type is True:
            table_parts.append(f"CREATE TYPE {self.dialect.table_qname(entity)} AS (\n")
        elif is_sequence is True:
            return f"CREATE SEQUENCE {self.dialect.table_qname(entity)};", deferred
        else:
            table_parts.append(f"CREATE TABLE {self.dialect.table_qname(entity)} (\n")

        length = len(entity.__fields__)
        for i, field in enumerate(entity.__fields__):
            if not field._virtual_:
                elements.append(self.compile_field(<Field>field, requirements))

        primary_keys = entity.__pk__
        if primary_keys:
            if is_type:
                raise ValueError("Primary Key is not supported on Composite Types")
            pk_names = [self.dialect.quote_ident(pk._name_) for pk in primary_keys]
            elements.append(f"PRIMARY KEY({', '.join(pk_names)})")


        if elements:
            table_parts.append("  ")
            table_parts.append(",\n  ".join(elements))
            table_parts.append("\n")

        table_parts.append(");")

        if requirements:
            requirements.append("")
            table_parts.insert(0, "\n".join(requirements))

        compiled_constraint = []
        comments = []
        for value in entity.__extgroups__.values():
            if value.type is Index:
                deferred.append(self.compile_create_index(value))
            elif value.type is Check:
                check, comment = self.compile_create_check(value)
                compiled_constraint.append(f"ADD {check}")
                comments.append(comment)
            elif value.type is ForeignKey:
                compiled_constraint.append(f"ADD {self.compile_foreign_key(value)}")
            elif value.type is Unique:
                compiled_constraint.append(f"ADD {self.compile_create_unique(value)}")

        if compiled_constraint:
            if is_type:
                raise ValueError(f"Constraints {compiled_constraint} is not supported on Composite Types")
            alter = ',\n  '.join(compiled_constraint)
            deferred.append(f"ALTER TABLE {self.dialect.table_qname(entity)}\n  {alter};")

        if comments:
            deferred.extend(comments)

        for trigger in entity.__triggers__:
            deferred.append(self.create_trigger(entity, trigger))

        return "".join(table_parts), deferred

    def compile_field(self, Field field, list requirements):
        cdef StorageType type = self.dialect.get_field_type(field)
        cdef EntityAttributeExt ext
        cdef EntityType seq

        if type is None:
            raise ValueError("Cannot determine the sql type of %r" % field)

        # req = type.requirements()
        # if req is not None:
        #     if not isinstance(req, str):
        #         raise TypeError("StorageType.requirements must returns with str or None")
        #     requirements.append(req)

        cdef str res = f"{self.dialect.quote_ident(field._name_)} {type.name}"

        if field.nullable is False:
            res += " NOT NULL"

        if field._default_ is not None:
            if isinstance(field._default_, Expression):
                qc = self.dialect.create_query_compiler()
                res += f" DEFAULT {qc.visit(field._default_)}"
            if callable(field._default_):
                pass  # no default value
            else:
                res += f" DEFAULT {self.dialect.quote_value(type.encode(field._default_))}"
        else:
            ext = field.get_ext(AutoIncrement)
            if ext:
                seq = (<AutoIncrement>ext).sequence
                res += f" DEFAULT nextval({self.dialect.quote_value(self.dialect.table_qname(seq))}::regclass)"

        return res

    def compile_foreign_key(self, EntityAttributeExtGroup key):
        cdef str res = f"CONSTRAINT {self.dialect.quote_ident(key.name)} FOREIGN KEY ("
        cdef int length = len(key.items)
        cdef ForeignKey fk

        for i in range(length):
            fk = key.items[i]
            res += self.dialect.quote_ident(fk.attr._name_)
            if i != length - 1:
                res += ", "

        res += f") REFERENCES {self.dialect.table_qname(key.items[0].ref._entity_)} ("

        for i in range(length):
            fk = key.items[i]
            res += self.dialect.quote_ident(fk.ref._name_)
            if i != length - 1:
                res += ", "

        return res + f") ON UPDATE {key.items[0].on_update} ON DELETE {key.items[0].on_delete}"

    def compile_create_index(self, EntityAttributeExtGroup group):
        cdef Index main = group.items[0]
        cdef str res = f"CREATE {'UNIQUE ' if main.unique else ''}INDEX" \
            f" {self.dialect.quote_ident(group.name)} ON {self.dialect.table_qname(main.attr._entity_)}" \
            f" USING {main.method} "

        cdef exprs = []
        cdef Index idx

        for idx in group.items:
            if idx.expr:
                expr = f"{idx.expr}"
            else:
                expr = f"{self.dialect.quote_ident(idx.attr._name_)}"
            if idx.collate:
                expr += f" COLLATE \"{idx.collate}\""
            exprs.append(expr)

        return res + "(" + ", ".join(exprs) + ");"

    def compile_create_check(self, EntityAttributeExtGroup group):
        cdef Check main = group.items[0]
        cdef str constraint = f"CONSTRAINT {self.dialect.quote_ident(group.name)} CHECK ("
        cdef list checks = []

        for item in group.items:
            checks.append(item.expr)

        if not checks:
            raise RuntimeError("Something went wrong with chekcs")

        if len(checks) > 1:
            constraint += f"({') AND ('.join(checks)})"
        else:
            constraint += checks[0]

        constraint += ")"

        cdef Check chk
        comments = json.dumps([chk.props for chk in group.items])
        cdef str comment = f"COMMENT ON CONSTRAINT {self.dialect.quote_ident(group.name)} ON {self.dialect.table_qname(main.attr._entity_)} IS {self.dialect.quote_value(comments)};"
        return constraint, comment

    def compile_create_unique(self, EntityAttributeExtGroup group):
        cdef Unique item
        cdef list fields = []

        for item in group.items:
            fields.append(self.dialect.quote_ident(item.attr._name_))

        return f"CONSTRAINT {self.dialect.quote_ident(group.name)} UNIQUE ({', '.join(fields)})"

    def compile_registry_diff(self, RegistryDiff diff):
        lines = []
        deferred = []
        entities_recrated = set()
        schemas_created = {ent.get_meta("schema", "public") for ent in diff.a.values()}
        schemas_created.add("public")

        # TODO: better method to determine which entities need to recreate
        for kind, param in diff:
            if kind is RegistryDiffKind.CHANGED:
                if param.b.get_meta("is_type", False) is False:
                    for ek, ep in param:
                        if ek is EntityDiffKind.CHANGED:
                            if "_index_" in ep[2]:
                                entities_recrated.add(self.dialect.table_qname(param.b))
                                break


        for kind, param in diff:
            if kind is RegistryDiffKind.REMOVED:
                lines.append(self.drop_entity(param))
            elif kind is RegistryDiffKind.CREATED:
                schema = param.get_meta("schema", "public")
                if schema and schema not in schemas_created:
                    schemas_created.add(schema)
                    lines.append(f"CREATE SCHEMA IF NOT EXISTS {self.dialect.quote_ident(schema)};")

                _tbl, _deferred = self.compile_entity(param)
                lines.append(_tbl)
                deferred.extend(_deferred)
            elif kind is RegistryDiffKind.CHANGED:
                if param.b.get_meta("is_type", False) is True:
                    lines.append(self.compile_type_diff(param))
                else:
                    _updates, _deferred = self.compile_entity_diff(param, entities_recrated)
                    lines.append(_updates)
                    deferred.extend(_deferred)
            elif kind is RegistryDiffKind.INSERT_ENTITY:
                qc = self.dialect.create_query_compiler()
                q, p = qc.compile_insert_or_update(param[0], param[1], param[2], param[3], True)
                if q:
                    lines.append(q + ";")
            elif kind is RegistryDiffKind.UPDATE_ENTITY:
                qc = self.dialect.create_query_compiler()
                q, p = qc.compile_update(param[0], param[1], param[2], param[3], param[4], True)
                if q:
                    lines.append(q + ";")
            elif kind is RegistryDiffKind.REMOVE_ENTITY:
                qc = self.dialect.create_query_compiler()
                q, p = qc.compile_delete(param[0], param[1], param[2], param[3], param[4], True)
                if q:
                    lines.append(q + ";")

        res = "\n".join(lines)
        if deferred:
            res += "\n" + "\n".join(deferred)
        return res

    def compile_entity_diff(self, EntityDiff diff, set entities_recrated):
        cdef EntityAttributeExtGroup group
        requirements = []
        alter = []
        pre = []
        post = []
        deferred = []

        for kind, param in diff:
            if kind == EntityDiffKind.REMOVED:
                alter.append(f"DROP COLUMN {self.dialect.quote_ident(param._name_)}")
            elif kind == EntityDiffKind.CREATED:
                alter.append(f"ADD COLUMN {self.compile_field(param, requirements)}")
            elif kind == EntityDiffKind.CHANGED:
                if "_index_" in param[2]:
                    return self.recreate_entity(param[0]._entity_, param[1]._entity_, entities_recrated)
                alter.extend(self.compile_field_diff(param[1], param[2]))
            elif kind == EntityDiffKind.REMOVE_PK:
                alter.append(f"DROP CONSTRAINT IF EXISTS {self.dialect.quote_ident(param.__name__ + '_pkey')}")
            elif kind == EntityDiffKind.CREATE_PK:
                pk_names = [self.dialect.quote_ident(pk._name_) for pk in param.__pk__]
                alter.append(f"ADD PRIMARY KEY({', '.join(pk_names)})")
            elif kind == EntityDiffKind.REMOVE_EXTGROUP:
                group = param
                if group.type is ForeignKey:
                    alter.append(f"DROP CONSTRAINT IF EXISTS {self.dialect.quote_ident(group.name)}")
                elif group.type is Check:
                    alter.append(f"DROP CONSTRAINT IF EXISTS {self.dialect.quote_ident(group.name)}")
                elif group.type is Unique:
                    alter.append(f"DROP CONSTRAINT IF EXISTS {self.dialect.quote_ident(group.name)}")
                elif group.type is Index:
                    schema = group.items[0].attr._entity_.__meta__.get("schema", "public")
                    schema = f"{self.dialect.quote_ident(schema)}." if schema != "public" else ""
                    pre.append(f"DROP INDEX IF EXISTS {schema}{self.dialect.quote_ident(group.name)};")
            elif kind == EntityDiffKind.CREATE_EXTGROUP:
                group = param
                if group.type is ForeignKey:
                    alter.append(f"ADD {self.compile_foreign_key(group)}")
                elif group.type is Index:
                    post.append(self.compile_create_index(group))
                elif group.type is Check:
                    check, comment = self.compile_create_check(group)
                    if check:
                        alter.append(f"ADD {check}")
                    if comment:
                        post.append(comment)
                elif group.type is Unique:
                    alter.append(f"ADD {self.compile_create_unique(group)}")
            elif kind == EntityDiffKind.REMOVE_TRIGGER:
                pre.append(self.remove_trigger(param[0], param[1]))
            elif kind == EntityDiffKind.CREATE_TRIGGER:
                post.append(self.create_trigger(param[0], param[1]))

        if alter:
            alter = ',\n  '.join(alter)
            alter = f"ALTER TABLE {self.dialect.table_qname(diff.b)}\n  {alter};"
        else:
            alter = ""

        return '\n'.join(filter(bool, ['\n'.join(pre), alter, '\n'.join(post)])), deferred

    # def compile_create_extgroup(self, EntityAttributeExtGroup group):
    #     if group.type is ForeignKey:
    #         pass
    #     elif group.type is Index:
    #         pass
    #     else:
    #         pass

    # def compile_remove_extgroup(self, EntityAttributeExtGroup group):
    #     if group.type is ForeignKey:
    #         yield
    #     elif group.type is Index:
    #         pass
    #     else:
    #         pass

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

        if "nullable" in diff and diff["nullable"]:
            result.append(f"ALTER COLUMN {col_name} DROP NOT NULL")

        if "_impl_" in diff or "size" in diff:
            # TODO: better handling of serial
            if type.name.startswith("SERIAL"):
                xt = f"INT{type.name[6] or ''}"
                result.append(f"ALTER COLUMN {col_name} TYPE {xt} USING {col_name}::{xt}")
            else:
                result.append(f"ALTER COLUMN {col_name} TYPE {type.name} USING {col_name}::{type.name}")

        if "_default_" in diff:
            _default = diff["_default_"]
            if _default is None:
                result.append(f"ALTER COLUMN {col_name} DROP DEFAULT")
            elif isinstance(_default, AutoIncrement):
                seq = (<AutoIncrement>_default).sequence
                default = f"nextval({self.dialect.quote_value(self.dialect.table_qname(seq))}::regclass)"
                result.append(f"ALTER COLUMN {col_name} SET DEFAULT {default}")
            else:
                if isinstance(_default, Expression):
                    qc = self.dialect.create_query_compiler()
                    default = qc.visit(diff['_default_'])
                elif callable(_default):
                    if field.nullable:
                        default = "NULL"
                    else:
                        default = None
                else:
                    default = self.dialect.quote_value(type.encode(_default))

                if default is None:
                    result.append(f"ALTER COLUMN {col_name} DROP DEFAULT")
                else:
                    result.append(f"ALTER COLUMN {col_name} SET DEFAULT {default}")

        if "nullable" in diff and not diff["nullable"]:
            result.append(f"ALTER COLUMN {col_name} SET NOT NULL")

        return result

    def drop_entity(self, EntityType entity):
        cdef bint is_type = entity.get_meta("is_type", False) is True
        cdef bint is_sequence = entity.get_meta("is_sequence", False) is True
        if is_type:
            return f"DROP TYPE {self.dialect.table_qname(entity)} CASCADE;"
        elif is_sequence:
            return f"DROP SEQUENCE {self.dialect.table_qname(entity)} CASCADE;"
        else:
            return f"DROP TABLE {self.dialect.table_qname(entity)} CASCADE;"

    def create_trigger(self, EntityType entity, Trigger trigger):
        raise NotImplementedError()

    def remove_trigger(self, EntityType entity, Trigger trigger):
        raise NotImplementedError()

    def recreate_entity(self, EntityType old_entity, EntityType new_entity, set entities_recrated):
        cdef list result = []
        cdef EntityType entity
        cdef Field field
        cdef EntityAttributeExtGroup fk_group
        cdef Registry old_registry = old_entity.__registry__
        cdef dict strip_entity = {}
        cdef dict fix_entity = {}

        # drop referenced fks
        # TODO: ha a jelenlegi RegistryDiff-ben módosult az fk_group akkor ne hozzuk létre újra
        for field in old_entity.__fields__:
            references = old_registry.get_referenced_foreign_keys(field)
            for entity, fk_groups in references:
                entity_qname = self.dialect.table_qname(entity)

                for fk_group in fk_groups:
                    strip_entity.setdefault(entity_qname, [])
                    strip_entity[entity_qname].append(
                        f"DROP CONSTRAINT IF EXISTS {self.dialect.quote_ident(fk_group.name)}"
                    )

                    # if entity recreated, dont need to restore foreign key, because recrate keys at the end of script
                    if entity_qname not in entities_recrated:
                        fix_entity.setdefault(entity_qname, [])
                        fix_entity[entity_qname].append(
                            f"ADD {self.compile_foreign_key(fk_group)}",
                        )

        if strip_entity:
            result.append(compile_alters(strip_entity))

        for trigger in old_entity.__triggers__:
            result.append(self.remove_trigger(old_entity, trigger))

        # rename
        old_entity_original_name = old_entity.__name__
        old_qname = self.dialect.table_qname(old_entity)
        old_entity.__name__ = f"_{old_entity.__name__}"
        result.append(f"ALTER TABLE {old_qname} RENAME TO {self.dialect.quote_ident(old_entity.__name__)};")

        # create
        entity_create, entity_deferred = self.compile_entity(new_entity)
        result.append(entity_create)

        # copy
        old_fields = {field._name_ for field in old_entity.__fields__ if not field._virtual_}
        new_fields = {field._name_ for field in new_entity.__fields__ if not field._virtual_}
        field_indexes = {field._name_: field._index_ for field in new_entity.__fields__ if not field._virtual_}
        field_names = []
        for name in sorted(old_fields & new_fields, key=field_indexes.get):
            field_names.append(self.dialect.quote_ident(name))

        if field_names:
            result.append(f"INSERT INTO {self.dialect.table_qname(new_entity)} ({', '.join(field_names)})")
            result.append(f"  SELECT {', '.join(field_names)} FROM {self.dialect.table_qname(old_entity)};")

        # delete renamed
        result.append(f"DROP TABLE {self.dialect.table_qname(old_entity)} CASCADE;")

        # restore deleted fks
        if fix_entity:
            result.append(compile_alters(fix_entity))

        # restore old entity original name
        old_entity.__name__ = old_entity_original_name
        return "\n".join(result), entity_deferred


cdef class DDLReflect:
    def __cinit__(self, Dialect dialect, EntityType entity_base):
        self.dialect = dialect
        self.entity_base = entity_base

    async def get_entities(self, conn, Registry registry):
        raise NotImplementedError()


cdef compile_alter(str entity, list alters):
    alter = ',\n  '.join(alters)
    return f"ALTER TABLE {entity}\n  {alter};"


cdef compile_alters(dict alters):
    return "\n".join(compile_alter(k, v) for k, v in alters.items())
