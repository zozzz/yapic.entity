# from hashids import Hashids

from yapic.entity._entity import Entity
from yapic.entity._entity cimport EntityType, EntityAttribute, EntityAttributeExtGroup
from yapic.entity._field cimport Field, PrimaryKey, ForeignKey, Index, AutoIncrement, StorageType
from yapic.entity._registry cimport Registry
from yapic.entity._expression cimport RawExpression
from yapic.entity._trigger cimport Trigger, PolymorphParentDeleteTrigger
from yapic.entity._field_impl cimport (
    IntImpl,
    StringImpl,
    BytesImpl,
    ChoiceImpl,
    BoolImpl,
    DateImpl,
    DateTimeImpl,
    DateTimeTzImpl,
    TimeImpl,
    TimeTzImpl,
    NumericImpl,
    FloatImpl,
    UUIDImpl,
    JsonImpl,
    CompositeImpl,
    ArrayImpl,
)
from yapic.entity._geom_impl cimport (
    PointImpl,
)
from .postgis._impl cimport (
    PostGISPointImpl,
    PostGISLatLngImpl,
)


from .._ddl cimport DDLCompiler, DDLReflect
from .._connection cimport Connection
from ._trigger cimport PostgreTrigger


cdef class PostgreDDLCompiler(DDLCompiler):
    def create_trigger(self, EntityType entity, Trigger trigger):
        trigger = self._special_trigger(entity, trigger)

        if not isinstance(trigger, PostgreTrigger):
            return

        schema = entity.__meta__.get("schema", "public")
        schema = f"{self.dialect.quote_ident(schema)}." if schema != "public" else ""
        trigger_name = f"{schema}{self.dialect.quote_ident(trigger.get_unique_name(entity))}"
        res = [f"CREATE OR REPLACE FUNCTION {trigger_name}() RETURNS TRIGGER AS $$ BEGIN"]
        res.extend(reident_lines(trigger.body))
        res.append("END; $$ language 'plpgsql' ;")

        res.append(f"CREATE TRIGGER {self.dialect.quote_ident(trigger.name)}")
        if trigger.before:
            res.append(f"  BEFORE {trigger.before} ON {self.dialect.table_qname(entity)}")
        elif trigger.after:
            res.append(f"  AFTER {trigger.after} ON {self.dialect.table_qname(entity)}")

        if trigger.for_each:
            res.append(f"  FOR EACH {trigger.for_each}")

        if trigger.when:
            res.append(f"  WHEN ({trigger.when})")

        res.append(f"  EXECUTE FUNCTION {trigger_name}();")

        return '\n'.join(res)

    def remove_trigger(self, EntityType entity, Trigger trigger):
        trigger = self._special_trigger(entity, trigger)
        if not isinstance(trigger, PostgreTrigger):
            return

        schema = entity.__meta__.get("schema", "public")
        schema = f"{self.dialect.quote_ident(schema)}." if schema != "public" else ""
        trigger_name = f"{schema}{self.dialect.quote_ident(trigger.get_unique_name(entity))}"

        res = [
            f"DROP TRIGGER IF EXISTS {self.dialect.quote_ident(trigger.name)} ON {self.dialect.table_qname(entity)};",
            f"DROP FUNCTION IF EXISTS {trigger_name};"
        ]
        return '\n'.join(res)

    def _special_trigger(self, EntityType entity, Trigger trigger):
        cdef PolymorphParentDeleteTrigger poly_delete

        if isinstance(trigger, PolymorphParentDeleteTrigger):
            poly_delete = <PolymorphParentDeleteTrigger>trigger
            where = [f'"parent".{self.dialect.quote_ident(pk._name_)}=OLD.{self.dialect.quote_ident(pk._name_)}' for pk in poly_delete.parent_entity.__pk__]

            return PostgreTrigger(
                name=poly_delete.name,
                before=poly_delete.before,
                after=poly_delete.after,
                for_each=poly_delete.for_each,
                unique_name=trigger.get_unique_name(entity),
                body=f"""
                    DELETE FROM {self.dialect.table_qname(poly_delete.parent_entity)} "parent" WHERE {' AND '.join(where)};
                    RETURN OLD;
                """
            )
        else:
            return trigger


JSON_ENTITY_UID = 0


cdef class PostgreDDLReflect(DDLReflect):
    async def get_extensions(self, Connection conn):
        exts = await conn.conn.fetch("""SELECT extname, extconfig FROM pg_catalog.pg_extension""")

        result = {}
        for name, config in exts:
            result[name] = config or []

        return result

    async def get_entities(self, Connection conn, Registry registry):
        types = await conn.conn.fetch("""
            SELECT
                "pg_type"."typrelid" as "id",
                "pg_namespace"."nspname" as "schema",
                "pg_type"."typname" as "name",
                "pg_class"."relkind" as "kind"
            FROM pg_type
                INNER JOIN pg_namespace ON pg_namespace.oid=pg_type.typnamespace
                INNER JOIN pg_class ON pg_class.oid=pg_type.typrelid
            WHERE "pg_class"."relkind" IN('r', 'c', 'S')
                AND pg_namespace.nspname != 'information_schema'
                AND pg_namespace.nspname NOT LIKE 'pg_%'
            ORDER BY "pg_class"."relkind" = 'r' ASC, pg_type.typrelid ASC""")

        cdef EntityAttribute attr

        extensions = await self.get_extensions(conn)
        not_sync_ids = []
        not_sync_names = []
        for name, tables in extensions.items():
            if tables:
                not_sync_ids.extend(tables)

            if name == "postgis":
                not_sync_names.append("geometry_dump")
                not_sync_names.append("valid_detail")

        types = [v for v in types if v[0] not in not_sync_ids and v[2] not in not_sync_names]

        for id, schema, table, kind in types:
            if kind == b"S":
                fields = []
                entity = await self.create_entity(conn, registry, schema, table, fields)
            else:
                fields = await self.get_fields(conn, registry, extensions, schema, table, id)
                entity = await self.create_entity(conn, registry, schema, table, fields)
            entity.__meta__["is_type"] = kind == b"c"
            entity.__meta__["is_sequence"] = kind == b"S"

            for attr in entity.__fields__:
                if attr._default_ is not None:
                    type_impl = conn.dialect.get_field_type(attr)
                    try:
                        attr._default_ = type_impl.decode(attr._default_)
                    except:
                        attr._default_ = RawExpression(str(attr._default_))

            entity.__triggers__ = await self.get_triggers(conn, registry, schema, table, id)

        for id, schema, table, kind in types:
            await self.update_foreign_keys(conn, schema, table, registry)
            await self.update_indexes(conn, schema, table, id, registry)

    async def create_entity(self, Connection conn, Registry registry, str schema, str table, list fields):
        schema = None if schema == "public" else schema
        class ReflectedEntity(self.entity_base, registry=registry, __fields__=fields, name=table, schema=schema):
            pass
        return ReflectedEntity

    async def get_primary_keys(self, Connection conn, str schema, str table):
        rows = await conn.conn.fetch(f"""
            SELECT "kcu"."column_name"
            FROM "information_schema"."table_constraints" "tc"
                INNER JOIN "information_schema"."key_column_usage" "kcu"
                    ON "tc"."table_name" = "kcu"."table_name"
                    AND "tc"."table_schema" = "kcu"."table_schema"
                    AND "tc"."constraint_name" = "kcu"."constraint_name"
            WHERE "tc"."constraint_type" = 'PRIMARY KEY'
                AND "tc"."table_schema" = '{schema}'
                AND "tc"."table_name" = '{table}'
            """)
        return [row[0] for row in rows]

    async def get_foreign_keys(self, Connection conn, str schema, str table):
        return await conn.conn.fetch(f"""
            SELECT
                "tc"."constraint_name",
                "ccu"."table_schema",
                "ccu"."table_name",
                "ccu"."column_name",
                "kcu"."column_name" as "field_name",
                "rc"."update_rule",
                "rc"."delete_rule"
            FROM "information_schema"."table_constraints" "tc"
                INNER JOIN "information_schema"."key_column_usage" "kcu"
                    ON "tc"."constraint_name" = "kcu"."constraint_name"
                    AND "tc"."constraint_catalog" = "kcu"."constraint_catalog"
                    AND "tc"."constraint_schema" = "kcu"."constraint_schema"
                    AND "tc"."table_name" = "kcu"."table_name"
                INNER JOIN "information_schema"."constraint_column_usage" "ccu"
                    ON "ccu"."constraint_name" = "tc"."constraint_name"
                    AND "ccu"."constraint_catalog" = "tc"."constraint_catalog"
                    AND "ccu"."constraint_schema" = "tc"."constraint_schema"
                INNER JOIN "information_schema"."referential_constraints" "rc"
                    ON "rc"."constraint_name" = "tc"."constraint_name"
                    AND "rc"."constraint_catalog" = "tc"."constraint_catalog"
                    AND "rc"."constraint_schema" = "tc"."constraint_schema"
            WHERE "tc"."constraint_type" = 'FOREIGN KEY'
                AND "tc"."table_schema" = '{schema}'
                AND "tc"."table_name" = '{table}'
            ORDER BY "kcu"."ordinal_position"
            """)

    async def get_indexes(self, Connection conn, int table_id):
        return await conn.conn.fetch(f"""
        SELECT
            pg_class.relname as "name",
            pg_am.amname as "method",
            pg_attribute.attname as field,
            pg_collation.collcollate as "collation",
            pg_index.indisunique as is_unique
        FROM pg_index
            INNER JOIN pg_class ON pg_class.oid=pg_index.indexrelid
            INNER JOIN pg_am ON pg_am.oid=pg_class.relam
            INNER JOIN pg_attribute ON pg_attribute.attrelid = pg_index.indrelid
                AND pg_attribute.attnum = ANY(pg_index.indkey)
            LEFT JOIN pg_collation ON pg_collation.oid = ANY(pg_index.indcollation)
        WHERE pg_index.indrelid={table_id}
            AND pg_index.indisprimary IS FALSE
            AND pg_index.indislive IS TRUE
        """)

    async def get_fields(self, Connection conn, registry, extensions, str schema, str table, typeid):
        if "postgis" in extensions:
            postgis_select = f""",
                "geom"."type" as "geom_type",
                "geom"."srid" as "geom_srid",
                "geom"."coord_dimension" as "geom_dim",
                "geog"."type" as "geog_type",
                "geog"."srid" as "geog_srid",
                "geog"."coord_dimension" as "geog_dim"
            """
            postgis_join = f"""
                LEFT JOIN "geometry_columns" "geom" ON
                    "geom"."f_table_schema"={self.dialect.quote_value(schema)}
                    AND "geom"."f_table_name"={self.dialect.quote_value(table)}
                    AND "geom"."f_geometry_column"="pg_attribute"."attname"
                LEFT JOIN "geography_columns" "geog" ON
                    "geog"."f_table_schema"={self.dialect.quote_value(schema)}
                    AND "geog"."f_table_name"={self.dialect.quote_value(table)}
                    AND "geog"."f_geography_column"="pg_attribute"."attname" """
        else:
            postgis_select = f""
            postgis_join = f""

        fields = await conn.conn.fetch(f"""
            SELECT
                "pg_attribute"."attname" as "name",
                pg_get_expr(pg_attrdef.adbin, pg_attrdef.adrelid) as "default",
                (
                    CASE
                        WHEN "pg_attribute"."attnotnull" OR ("pg_type"."typtype" = 'd' AND "pg_type"."typnotnull") THEN 'NO'
                        ELSE 'YES'
                    END
                ) as "is_nullable",
                typens.nspname as "typeschema",
                pg_type.typname as "typename",
                information_schema._pg_char_max_length(information_schema._pg_truetypid(pg_attribute.*, pg_type.*), information_schema._pg_truetypmod(pg_attribute.*, pg_type.*)) AS character_maximum_length,
                information_schema._pg_numeric_precision(information_schema._pg_truetypid(pg_attribute.*, pg_type.*), information_schema._pg_truetypmod(pg_attribute.*, pg_type.*)) AS numeric_precision,
                information_schema._pg_numeric_scale(information_schema._pg_truetypid(pg_attribute.*, pg_type.*), information_schema._pg_truetypmod(pg_attribute.*, pg_type.*)) AS numeric_scale,
                pg_attribute.attlen as "size",
                pg_type.typcategory as "category"
                {postgis_select}
            FROM pg_attribute
                INNER JOIN pg_type ON pg_type.oid=pg_attribute.atttypid
                INNER JOIN pg_namespace typens ON typens.oid=pg_type.typnamespace
                LEFT JOIN pg_attrdef ON pg_attribute.attrelid = pg_attrdef.adrelid AND pg_attribute.attnum = pg_attrdef.adnum
                {postgis_join}
            WHERE attrelid={typeid} AND attnum > 0 ORDER BY attnum""")

        pks = await self.get_primary_keys(conn, schema, table)

        result = []
        for record in fields:
            result.append(await self.create_field(conn, registry, schema, table, record["name"] in pks, record))

        return result

    async def get_triggers(self, Connection conn, registry, str schema, str table, typeid):
        triggers = await conn.conn.fetch(f"""
            SELECT
                "pg_trigger"."tgname",
                "it"."action_timing",
                "it"."event_manipulation",
                "it"."action_orientation",
                "pg_proc"."proname"
            FROM "pg_trigger"
                INNER JOIN "pg_proc" ON "pg_proc"."oid" = "pg_trigger"."tgfoid"
                INNER JOIN "information_schema"."triggers" "it"
                    ON "it"."trigger_schema" = '{schema}'
                    AND it."trigger_name" = "pg_trigger"."tgname"
            WHERE "pg_trigger"."tgrelid" = {typeid}
                AND "pg_proc"."proname" LIKE 'YT-%'
        """)

        cdef Trigger trigger
        result = []

        for record in triggers:
            trigger = PostgreTrigger(name=record[0], for_each=record[3])
            if record[1] == "BEFORE":
                trigger.before = record[2].upper()
            else:
                trigger.after = record[2].upper()
            trigger.unique_name = record[4]
            result.append(trigger)

        return result

    async def create_field(self, Connection conn, registry, str schema, str table, bint primary, record):
        global JSON_ENTITY_UID

        cdef Field field
        cdef StorageType type_impl
        cdef bint skip_default = False
        cdef bint skip_primary = False
        cdef str typename = record["typename"]
        cdef str typeschema = record["typeschema"]
        cdef bint is_nullable = record["is_nullable"] == "YES"
        default = record["default"]

        if typename in ("int2", "_int2", "int4", "_int4", "int8", "_int8"):
            field = Field(IntImpl(), size=int(record["size"]), nullable=is_nullable)

            if default is not None:
                if primary:
                    field // PrimaryKey()
                    skip_primary = True

                ac = await self.get_auto_increment(conn, registry, schema, table, record["name"], default)
                if ac:
                    field // ac
                    skip_default = True
        elif typename in ("text", "_text"):
            field = Field(StringImpl(), nullable=is_nullable)
        elif typename == "bytea":
            field = Field(BytesImpl(), nullable=is_nullable)
        elif typename == "varchar":
            field = Field(StringImpl(), size=record["character_maximum_length"], nullable=is_nullable)
        elif typename == "bpchar":
            l = record["character_maximum_length"]
            field = Field(StringImpl(), size=[l, l], nullable=is_nullable)
        elif typename == "bool":
            field = Field(BoolImpl(), nullable=is_nullable)
        elif typename == "date":
            field = Field(DateImpl(), nullable=is_nullable)
        elif typename == "timestamptz":
            field = Field(DateTimeTzImpl(), nullable=is_nullable)
        elif typename == "timestamp":
            field = Field(DateTimeImpl(), nullable=is_nullable)
        elif typename == "time":
            field = Field(TimeImpl(), nullable=is_nullable)
        elif typename == "timetz":
            field = Field(TimeTzImpl(), nullable=is_nullable)
        elif typename == "numeric":
            field = Field(NumericImpl(), size=(record["numeric_precision"], record["numeric_scale"]), nullable=is_nullable)
        elif typename == "float4" or typename == "float8":
            field = Field(FloatImpl(), size=record["size"], nullable=is_nullable)
        elif typename == "uuid":
            field = Field(UUIDImpl(), nullable=is_nullable)
        elif typename == "jsonb":
            JSON_ENTITY_UID += 1

            class JsonEntity(self.entity_base, name=f"JsonEntity{JSON_ENTITY_UID}"):
                pass
            field = Field(JsonImpl(JsonEntity), nullable=is_nullable)
        elif typename == "point":
            field = Field(PointImpl(), nullable=is_nullable)
        elif typename == "geometry":
            geom_type = record["geom_type"].lower()
            if geom_type == "point":
                field = Field(PostGISPointImpl(record["geom_srid"]), nullable=is_nullable)
            else:
                raise ValueError(f"Unhandled geometry type: {record['geom_type']}")
        elif typename == "geography":
            geog_type = record["geog_type"].lower()
            if geog_type == "point":
                field = Field(PostGISLatLngImpl(record["geog_srid"]), nullable=is_nullable)
            else:
                raise ValueError(f"Unhandled geometry type: {record['geog_type']}")
        elif typeschema != "pg_catalog" and typeschema != "information_schema" and record["category"] == b"C":
            ctypename = f"{typeschema}.{typename}" if typeschema != "public" else f"{typename}"
            centity = registry[ctypename]
            field = Field(CompositeImpl(centity), nullable=is_nullable)
        else:
            raise TypeError("Can't determine type from sql type: %r" % typename)

        if record["category"] == b"A":
            field = Field(ArrayImpl(field._impl_), nullable=field.nullable)

        field._name_ = record["name"]

        if skip_default is False and default is not None:
            # if isinstance(default, str):
            #     if default[0] == "'" and default.endswith(f"::{data_type}"):
            #         default = default[1:-(len(data_type) + 3)]

            if isinstance(default, str) and "::" in default:
                default = await conn.conn.fetchval(f"SELECT {default}")

            field._default_ = default

        if skip_primary is False and primary:
            field // PrimaryKey()

        return field

    async def update_foreign_keys(self, Connection conn, str schema, str table, Registry registry):
        cdef ForeignKey fk

        fks = await self.get_foreign_keys(conn, schema, table)
        cdef EntityType entity = registry[f"{schema}.{table}" if schema != "public" else table]
        cdef dict groups = {}

        for fk_desc in fks:
            field = getattr(entity, fk_desc["field_name"])

            if fk_desc["table_schema"] == "public":
                ref_entity = registry[fk_desc["table_name"]]
            else:
                ref_entity = registry[f"{fk_desc['table_schema']}.{fk_desc['table_name']}"]

            fk = ForeignKey(getattr(ref_entity, fk_desc["column_name"]),
                name=fk_desc["constraint_name"],
                on_update=fk_desc["update_rule"],
                on_delete=fk_desc["delete_rule"])

            field // fk
            fk.init(field)
            fk.bind()

            try:
                groups[fk.group_by].append(fk)
            except KeyError:
                groups[fk.group_by] = [fk]

        entity.__extgroups__ += create_ext_groups(groups)

    async def update_indexes(self, Connection conn, str schema, str table, table_id, Registry registry):
        indexes = await self.get_indexes(conn, table_id)

        cdef EntityType entity = registry[f"{schema}.{table}" if schema != "public" else table]
        cdef dict groups = {}
        cdef Index idx

        for idx_desc in indexes:
            if idx_desc["field"]:
                field = getattr(entity, idx_desc["field"])

                idx = Index(
                    name=idx_desc["name"],
                    method=idx_desc["method"],
                    unique=bool(idx_desc["is_unique"]),
                    collate=idx_desc["collation"],
                )
            else:
                raise NotImplementedError()

            field // idx
            idx.init(field)
            idx.bind()

            try:
                groups[idx.group_by].append(idx)
            except KeyError:
                groups[idx.group_by] = [idx]

        not_exists = []
        for g in create_ext_groups(groups):
            if g not in entity.__extgroups__:
                not_exists.append(g)

        if not_exists:
            entity.__extgroups__ += tuple(not_exists)


    async def get_auto_increment(self, Connection conn, Registry registry, str schema, str table, str field, default):
        if schema != "public":
            prefix = await self.real_quote_ident(conn, schema)
            prefix += "."
            table_qname = f"{schema}."
        else:
            prefix = ""
            table_qname = ""

        seq_name = f"{table}_{field}_seq"
        seq_name_q = await self.real_quote_ident(conn, seq_name)
        auto_increment_default = f"""nextval('{prefix}{seq_name_q}'::regclass)"""
        if auto_increment_default == default:
            table_qname += seq_name

            try:
                seq = registry[table_qname]
            except KeyError:
                seq = None

            return AutoIncrement(seq)
        else:
            return None

    async def real_quote_ident(self, Connection conn, ident):
        return await conn.conn.fetchval(f"SELECT quote_ident({self.dialect.quote_value(ident)})")


cdef tuple create_ext_groups(dict groups):
    cdef list res = []
    for grouped in groups.values():
        group = EntityAttributeExtGroup(grouped[0].name, type(grouped[0]))
        group.items = tuple(grouped)
        res.append(group)
    return tuple(res)



cdef list reident_lines(str data, int ident_size = 2):
    lines = list(filter(lambda l: bool(l.strip()), data.splitlines(False)))
    first_line = lines[0]
    initial_ident = first_line[:-len(first_line.lstrip())]
    return [" " * ident_size + line[len(initial_ident):] for line in lines]
