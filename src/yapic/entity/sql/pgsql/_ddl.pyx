# from hashids import Hashids
import re
import hashlib
from typing import Any
from yapic import json

from yapic.entity._entity import Entity
from yapic.entity._entity cimport EntityType, EntityAttribute, EntityAttributeExtGroup
from yapic.entity._field cimport Field, PrimaryKey, ForeignKey, Index, Check, Unique, AutoIncrement, StorageType
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
from ._trigger cimport PostgreTrigger


RE_NEXTVAL = re.compile(r"""nextval\('([^']+)'(?:::regclass)?\)""", re.I)


BULTIN_FUNCTIONS = {
    ("public", "yapic_entity_typmod"): """(attr pg_attribute, typ pg_type) RETURNS INT[] AS $$
        DECLARE typmodstr TEXT;
        DECLARE typmodarr INT[];
        BEGIN
            IF typ.typmodout = '-'::regproc OR attr.atttypmod < 0 THEN
                IF typ.typlen > 0 THEN
                    RETURN ARRAY[typ.typlen];
                ELSE
                    RETURN NULL;
                END IF;
            END IF;

            IF typ.typcategory NOT IN ('A', 'B', 'G', 'N', 'S') THEN
                RETURN NULL;
            END IF;

            EXECUTE 'SELECT ' || typ.typmodout || '(' || attr.atttypmod || ')' INTO typmodstr;

            SELECT replace(replace(typmodstr, '(', '{'), ')', '}')::INT[] INTO typmodarr;
            RETURN typmodarr;
        END $$ LANGUAGE plpgsql;
    """
}


cdef class PostgreDDLCompiler(DDLCompiler):
    def create_trigger(self, EntityType entity, Trigger trigger):
        trigger = self._special_trigger(entity, trigger)

        if not isinstance(trigger, PostgreTrigger):
            return

        schema = entity.get_meta("schema", "public")
        schema = f"{self.dialect.quote_ident(schema)}." if schema != "public" else ""
        trigger_name = f"{schema}{self.dialect.quote_ident(trigger.get_unique_name(entity))}"
        res = [f"CREATE OR REPLACE FUNCTION {trigger_name}() RETURNS TRIGGER AS $$"]
        if trigger.declare:
            res.append(trigger.declare)
        res.append("BEGIN")
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

        schema = entity.get_meta("schema", "public") or "public"
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
    async def get_extensions(self, conn):
        exts = await conn.fetch("""SELECT extname, extconfig FROM pg_catalog.pg_extension""")

        result = {}
        for name, config in exts:
            result[name] = config or []

        return result

    async def get_entities(self, conn, Registry registry):
        cdef EntityType entity

        for bultin_name, builtin_body in BULTIN_FUNCTIONS.items():
            await self._ensure_builtin_function(conn, bultin_name, builtin_body)

        # "pg_class"."relkind" IN('r', 'c', 'S')
        types = await conn.fetch("""
            SELECT
                "pg_type"."typrelid" as "id",
                "pg_namespace"."nspname" as "schema",
                "pg_type"."typname" as "name",
                "pg_class"."relkind" as "kind"
            FROM pg_type
                INNER JOIN pg_namespace ON pg_namespace.oid=pg_type.typnamespace
                INNER JOIN pg_class ON pg_class.oid=pg_type.typrelid
            WHERE "pg_class"."relkind" IN('r', 'c')
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

        sequences = await conn.fetch("""
            SELECT
                0 as "id",
                sequence_schema as "schema",
                sequence_name as "name",
                'S'::bytea as "kind"
            FROM information_schema.sequences
        """)
        types = sequences + types

        for id, schema, table, kind in types:
            if kind == b"S":
                fields = []
                entity = await self.create_entity(conn, registry, schema, table, fields)
            else:
                fields = await self.get_fields(conn, registry, extensions, schema, table, id)
                entity = await self.create_entity(conn, registry, schema, table, fields)
            entity.set_meta("is_type", kind == b"c")
            entity.set_meta("is_sequence", kind ==b"S")

            for attr in entity.__fields__:
                if attr._default_ is not None:
                    type_impl = conn.dialect.get_field_type(attr)
                    try:
                        attr._default_ = type_impl.decode(attr._default_)
                    except:
                        attr._default_ = RawExpression(str(attr._default_))

            entity.__triggers__ = await self.get_triggers(conn, registry, schema, table, id)

        for id, schema, table, kind in types:
            entity = registry[f"{schema}.{table}" if schema != "public" else table]
            await self.update_indexes(conn, schema, table, id, entity, registry)
            await self.update_foreign_keys(conn, schema, table, id, entity, registry)
            await self.update_checks(conn, schema, table, id, entity, registry)
            await self.update_uniques(conn, schema, table, id, entity, registry)

    async def create_entity(self, conn, Registry registry, str schema, str table, list fields):
        schema = None if schema == "public" else schema
        class ReflectedEntity(self.entity_base, registry=registry, __fields__=fields, name=table, schema=schema):
            pass
        return ReflectedEntity

    async def get_primary_keys(self, conn, str schema, str table):
        rows = await conn.fetch(f"""
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

    async def get_foreign_keys(self, conn, str schema, str table):
        return await conn.fetch(f"""
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

    async def get_indexes(self, conn, int table_id):
        return await conn.fetch(f"""
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
                AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint pc WHERE pc.conname = pg_class.relname)
        """)

    async def get_checks(self, conn, str schema, str table):
        return await conn.fetch(f"""
            SELECT
                pc.conname AS "name",
                pd.description AS "comment"
            FROM pg_catalog.pg_constraint pc
                INNER JOIN pg_catalog.pg_class cls ON cls."oid" = pc.conrelid
                INNER JOIN pg_catalog.pg_namespace sch ON sch."oid" = cls.relnamespace
                LEFT JOIN pg_catalog.pg_description pd ON pd.objoid = pc."oid"
            WHERE pc.contype = 'c'
                AND sch.nspname = '{schema}'
                AND cls.relname = '{table}'
        """)

    async def get_uniques(self, conn, str schema, str table):
        return await conn.fetch(f"""
            SELECT
                pc.conname AS "name",
                pg_attribute.attname as "field"
            FROM pg_catalog.pg_constraint pc
                INNER JOIN pg_catalog.pg_class cls ON cls."oid" = pc.conrelid
                INNER JOIN pg_catalog.pg_namespace sch ON sch."oid" = cls.relnamespace
                INNER JOIN pg_attribute ON pg_attribute.attrelid = pc.conrelid
					AND pg_attribute.attnum = ANY(pc.conkey)
            WHERE pc.contype = 'u'
                AND sch.nspname = '{schema}'
                AND cls.relname = '{table}'
        """)

    async def get_fields(self, conn, registry, extensions, str schema, str table, typeid):
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
                    AND "geom"."f_geometry_column"="main_attr"."attname"
                LEFT JOIN "geography_columns" "geog" ON
                    "geog"."f_table_schema"={self.dialect.quote_value(schema)}
                    AND "geog"."f_table_name"={self.dialect.quote_value(table)}
                    AND "geog"."f_geography_column"="main_attr"."attname" """
        else:
            postgis_select = f""
            postgis_join = f""

        # query = f"""
        #     SELECT
        #         "pg_attribute"."attname" as "name",
        #         pg_get_expr(pg_attrdef.adbin, pg_attrdef.adrelid) as "default",
        #         (
        #             CASE
        #                 WHEN "pg_attribute"."attnotnull" OR ("pg_type"."typtype" = 'd' AND "pg_type"."typnotnull") THEN 'NO'
        #                 ELSE 'YES'
        #             END
        #         ) as "is_nullable",
        #         typens.nspname as "typeschema",
        #         pg_type.typname as "typename",
        #         information_schema._pg_char_max_length(information_schema._pg_truetypid(pg_attribute.*, pg_type.*), information_schema._pg_truetypmod(pg_attribute.*, pg_type.*)) AS character_maximum_length,
        #         information_schema._pg_numeric_precision(information_schema._pg_truetypid(pg_attribute.*, pg_type.*), information_schema._pg_truetypmod(pg_attribute.*, pg_type.*)) AS numeric_precision,
        #         information_schema._pg_numeric_scale(information_schema._pg_truetypid(pg_attribute.*, pg_type.*), information_schema._pg_truetypmod(pg_attribute.*, pg_type.*)) AS numeric_scale,
        #         pg_attribute.attlen as "size",
        #         pg_type.typcategory as "category"
        #         {postgis_select}
        #     FROM pg_attribute
        #         INNER JOIN pg_type ON pg_type.oid=pg_attribute.atttypid
        #         INNER JOIN pg_namespace typens ON typens.oid=pg_type.typnamespace
        #         LEFT JOIN pg_attrdef ON pg_attribute.attrelid = pg_attrdef.adrelid AND pg_attribute.attnum = pg_attrdef.adnum
        #         {postgis_join}
        #     WHERE attrelid={typeid} AND attnum > 0 ORDER BY attnum
        # """

        query = f"""
        SELECT
            "main_attr"."attname" as "name",
            pg_get_expr(attr_def.adbin, attr_def.adrelid) as "default",
            (
                CASE
                    WHEN "main_attr"."attnotnull" OR ("main_type"."typtype" = 'd' AND "main_type"."typnotnull") THEN 'NO'
                    ELSE 'YES'
                END
            ) as "is_nullable",
            "typens"."nspname" as "typeschema",
            "main_type"."typname" as "main_typename",
            "attr_type"."typname" as "typename",
            yapic_entity_typmod(main_attr, attr_type) AS "size",
            main_type.typcategory as "category"
            {postgis_select}
        FROM pg_attribute main_attr
            INNER JOIN pg_type main_type ON main_type.oid = main_attr.atttypid
            LEFT JOIN pg_type item_type ON item_type.oid = main_type.typelem
            INNER JOIN pg_type attr_type ON
                (item_type.oid IS NOT NULL AND attr_type.oid = item_type.oid)
                OR
                (item_type.oid IS NULL AND attr_type.oid = main_type.oid)
            INNER JOIN pg_namespace typens ON typens.oid=attr_type.typnamespace
            LEFT JOIN pg_attrdef attr_def ON attr_def.adrelid = main_attr.attrelid
                AND attr_def.adnum = main_attr.attnum
            {postgis_join}
        WHERE
            main_attr.attrelid={typeid}
            AND main_attr.attnum > 0
        ORDER BY main_attr.attnum
        """
        fields = await conn.fetch(query)

        pks = await self.get_primary_keys(conn, schema, table)

        result = []
        for record in fields:
            result.append(await self.create_field(conn, registry, schema, table, record["name"] in pks, record))

        return result

    async def get_triggers(self, conn, registry, str schema, str table, typeid):
        # TODO: event_object_table ala database
        triggers = await conn.fetch(f"""
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
                    AND "it"."event_object_table" = '{table}'
                    AND "it"."trigger_name" = "pg_trigger"."tgname"
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

    async def create_field(self, conn, registry, str schema, str table, bint primary, record):
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
            field = Field(IntImpl(), size=record["size"][0], nullable=is_nullable)

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
            field = Field(StringImpl(), size=record["size"][0], nullable=is_nullable)
        elif typename == "bpchar":
            l = record["size"][0]
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
            field = Field(NumericImpl(), size=record["size"], nullable=is_nullable)
        elif typename == "float4" or typename == "float8":
            field = Field(FloatImpl(), size=record["size"][0], nullable=is_nullable)
        elif typename == "uuid":
            field = Field(UUIDImpl(), nullable=is_nullable)
        elif typename == "jsonb":
            field = Field(JsonImpl(Any), nullable=is_nullable)
        # elif typename == "point":
        #     field = Field(PointImpl(), nullable=is_nullable)
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
            min_size = field.min_size
            max_size = field.max_size
            field = Field(ArrayImpl(field._impl_), nullable=field.nullable, size=(min_size, max_size))
        elif record["category"] == b"G":
            if record["main_typename"] == "point":
                field = Field(PointImpl(), nullable=is_nullable)

        field._name_ = record["name"]

        if skip_default is False and default is not None:
            # if isinstance(default, str):
            #     if default[0] == "'" and default.endswith(f"::{data_type}"):
            #         default = default[1:-(len(data_type) + 3)]

            if isinstance(default, str) and "::" in default:
                default = await conn.fetchval(f"SELECT {default}")

            field._default_ = default

        if skip_primary is False and primary:
            field // PrimaryKey()

        return field

    async def update_foreign_keys(self, conn, str schema, str table, table_id, EntityType entity, Registry registry):
        fks = await self.get_foreign_keys(conn, schema, table)

        cdef ForeignKey fk
        cdef Field field

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

    async def update_indexes(self, conn, str schema, str table, table_id, EntityType entity, Registry registry):
        indexes = await self.get_indexes(conn, table_id)

        cdef Index idx
        cdef Field field

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

    async def update_checks(self, conn, str schema, str table, table_id, EntityType entity, Registry registry):
        cdef list checks = await self.get_checks(conn, schema, table)

        for check in checks:
            if not check["comment"]:
                continue

            name = check["name"]
            props = json.loads(check["comment"])

            for prop in props:
                field = getattr(entity, prop["field"])
                chk = Check("", name=name)
                chk.props = prop
                field // chk

    async def update_uniques(self, conn, str schema, str table, table_id, EntityType entity, Registry registry):
        cdef list uniques = await self.get_uniques(conn, schema, table)

        for unique in uniques:
            field = getattr(entity, unique["field"])
            field // Unique(name=unique["name"])

    async def get_auto_increment(self, conn, Registry registry, str schema, str table, str field, default):
        if not default:
            return None

        match = re.match(RE_NEXTVAL, default)
        if match:
            ident = self.dialect.unquote_ident(match[1])
            if len(ident) == 2:
                schema, name = ident
            elif len(ident) == 1:
                name = ident[0]
                schema = "public"
            else:
                raise ValueError(f"Unexpected value as ident: {ident}")

            try:
                seq = registry[name if schema == "public" else f"{schema}.{name}"]
            except:
                seq = None

            return AutoIncrement(seq)
        else:
            return None


    async def real_quote_ident(self, conn, ident):
        return await conn.fetchval(f"SELECT quote_ident({self.dialect.quote_value(ident)})")

    async def _ensure_builtin_function(self, conn, tuple name, str body):
        if len(name) != 2:
            raise ValueError(f"Invalid builtin function name: {name}, must be tuple with two items (schema, name)")

        body = "\n".join(reident_lines(body, 4))
        cdef dict existing = await self._get_builtin_funcion(conn, name)
        if not existing or "hash" not in existing:
            await self._create_builtin_function(conn, name, body)
        else:
            current_hash = hashlib.md5(body.encode("UTF-8")).hexdigest()
            if existing["hash"] != current_hash:
                await self._create_builtin_function(conn, name, body)

    async def _get_builtin_funcion(self, conn, tuple name):
        res = await conn.fetchrow(
            f"""
            SELECT
                pd.description
            FROM pg_proc
                INNER JOIN pg_catalog.pg_namespace sch ON sch."oid" = pg_proc.pronamespace
                LEFT JOIN pg_catalog.pg_description pd ON pd.objoid = pg_proc."oid"
            WHERE sch.nspname = '{name[0]}'
                AND pg_proc.proname= '{name[1]}';
            """
        )

        if res:
            return json.loads(res["description"])
        return None

    async def _create_builtin_function(self, conn, tuple name, str body):
        comment = json.dumps({"hash": hashlib.md5(body.encode("UTF-8")).hexdigest()})
        qname = ".".join(self.dialect.quote_ident(v) for v in name)
        query = f"DROP FUNCTION IF EXISTS {qname};"
        query += f"CREATE OR REPLACE FUNCTION {qname}{body};"
        query += f"COMMENT ON FUNCTION {qname} IS '{comment}';"
        await conn.execute(query)


cdef list reident_lines(str data, int ident_size = 2):
    lines = list(filter(lambda l: bool(l.strip()), data.splitlines(False)))
    first_line = lines[0]
    initial_ident = first_line[:-len(first_line.lstrip())]
    return [" " * ident_size + line[len(initial_ident):] for line in lines]
