# from hashids import Hashids

from yapic.entity._entity import Entity
from yapic.entity._entity cimport EntityType, EntityAttribute
from yapic.entity._field cimport Field, PrimaryKey, ForeignKey, AutoIncrement, StorageType
from yapic.entity._registry cimport Registry
from yapic.entity._expression cimport RawExpression
from yapic.entity._field_impl cimport (
    IntImpl,
    StringImpl,
    BytesImpl,
    ChoiceImpl,
    BoolImpl,
    DateImpl,
    DateTimeImpl,
    DateTimeTzImpl,
    NumericImpl,
    FloatImpl,
    UUIDImpl,
    JsonImpl,
    CompositeImpl,
)
from yapic.entity._geom_impl cimport (
    PointImpl,
)


from .._ddl cimport DDLCompiler, DDLReflect
from .._connection cimport Connection


cdef class PostgreDDLCompiler(DDLCompiler):
    pass


JSON_ENTITY_UID = 0


cdef class PostgreDDLReflect(DDLReflect):
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

        for id, schema, table, kind in types:
            if kind == b"S":
                fields = []
                entity = await self.create_entity(conn, registry, schema, table, fields)
            else:
                fields = await self.get_fields(conn, registry, schema, table, id)
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

        for id, schema, table, kind in types:
            fks = await self.update_foreign_keys(conn, schema, table, registry)

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

    async def get_fields(self, Connection conn, registry, str schema, str table, typeid):
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
                pg_attribute.attlen as "size"
            FROM pg_attribute
                INNER JOIN pg_type ON pg_type.oid=pg_attribute.atttypid
                INNER JOIN pg_namespace typens ON typens.oid=pg_type.typnamespace
                LEFT JOIN pg_attrdef ON pg_attribute.attrelid = pg_attrdef.adrelid AND pg_attribute.attnum = pg_attrdef.adnum
            WHERE attrelid={typeid} AND attnum > 0 ORDER BY attnum""")

        pks = await self.get_primary_keys(conn, schema, table)

        result = []
        for record in fields:
            result.append(await self.create_field(conn, registry, schema, table, record["name"] in pks, record))

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

        if typename == "int2" or typename == "int4" or typename == "int8":
            field = Field(IntImpl(), size=int(record["size"]), nullable=is_nullable)

            if default is not None:
                if primary:
                    field // PrimaryKey()
                    skip_primary = True

                ac = await self.get_auto_increment(conn, registry, schema, table, record["name"], default)
                if ac:
                    field // ac
                    skip_default = True
        elif typename == "text":
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
        elif typeschema != "pg_catalog" and typeschema != "information_schema":
            ctypename = f"{typeschema}.{typename}" if typeschema != "public" else f"{typename}"
            centity = registry[ctypename]
            field = Field(CompositeImpl(centity), nullable=is_nullable)
        else:
            raise TypeError("Can't determine type from sql type: %r" % typename)

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
        entity = registry[f"{schema}.{table}" if schema != "public" else table]

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
            fk.bind(field)

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



