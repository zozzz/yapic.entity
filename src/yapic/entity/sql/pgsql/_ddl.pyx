# from hashids import Hashids

from yapic.entity._entity import Entity
from yapic.entity._entity cimport EntityType, EntityAttribute
from yapic.entity._field cimport Field, PrimaryKey, ForeignKey, StorageType
from yapic.entity._field_impl cimport IntImpl, StringImpl, BytesImpl, ChoiceImpl, BoolImpl, DateImpl, DateTimeImpl, DateTimeTzImpl
from yapic.entity._registry cimport Registry
from yapic.entity._expression cimport RawExpression

from .._ddl cimport DDLCompiler, DDLReflect
from .._connection cimport Connection


cdef class PostgreDDLCompiler(DDLCompiler):
    pass


cdef class PostgreDDLReflect(DDLReflect):
    async def get_entities(self, Connection conn, Registry registry):
        tables = await conn.conn.fetch("""
            SELECT "table_schema", "table_name"
            FROM "information_schema"."tables"
            WHERE "table_schema" != 'information_schema'
                AND "table_schema" NOT LIKE 'pg_%'
                AND "table_type" = 'BASE TABLE'""")

        cdef EntityAttribute attr

        for schema, table in tables:
            fields = await self.get_fields(conn, schema, table)
            entity = await self.create_entity(conn, registry, schema, table, fields)

            for attr in entity.__fields__:
                if attr._default_ is not None:
                    type_impl = conn.dialect.get_field_type(attr)
                    try:
                        attr._default_ = type_impl.decode(attr._default_)
                    except:
                        attr._default_ = RawExpression(str(attr._default_))

        for schema, table in tables:
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
                    AND "tc"."constraint_schema" = "kcu"."constraint_schema"
                INNER JOIN "information_schema"."constraint_column_usage" "ccu"
                    ON "ccu"."constraint_name" = "tc"."constraint_name"
                    AND "ccu"."constraint_schema" = "tc"."constraint_schema"
                INNER JOIN "information_schema"."referential_constraints" "rc"
                    ON "rc"."constraint_name" = "tc"."constraint_name"
                    AND "rc"."constraint_schema" = "tc"."constraint_schema"
            WHERE "tc"."constraint_type" = 'FOREIGN KEY'
                AND "tc"."table_schema" = '{schema}'
                AND "tc"."table_name" = '{table}'
            ORDER BY "kcu"."ordinal_position"
            """)

    async def get_fields(self, Connection conn, str schema, str table):
        fields = await conn.conn.fetch(f"""
            SELECT *
            FROM "information_schema"."columns"
            WHERE "table_name" = '{table}' AND table_schema = '{schema}'
            ORDER BY "ordinal_position" """)

        pks = await self.get_primary_keys(conn, schema, table)

        result = []
        for record in fields:
            result.append(await self.create_field(conn, schema, table, record["column_name"] in pks, record))

        return result

    async def create_field(self, Connection conn, str schema, str table, bint primary, record):
        cdef Field field
        cdef StorageType type_impl
        cdef bint skip_default = False
        cdef bint skip_primary = False
        cdef str data_type = record["data_type"]
        cdef bint is_nullable = record["is_nullable"] == "YES"
        default = record["column_default"]

        if data_type == "integer" or data_type == "smallint" or data_type == "bigint":
            field = Field(IntImpl(), size=int(record["numeric_precision"] / 8), nullable=is_nullable)

            if default is not None:
                if primary:
                    prefix = f'"{schema}".' if schema != "public" else ''
                    name = record["column_name"]
                    auto_increment_default = f"""nextval('{prefix}"{table}_{name}_seq"'::regclass)"""
                    field // PrimaryKey(auto_increment=auto_increment_default == default)
                    skip_default = True
                    skip_primary = True
        elif data_type == "text":
            field = Field(StringImpl(), nullable=is_nullable)
        elif data_type == "bytea":
            field = Field(BytesImpl(), nullable=is_nullable)
        elif data_type == "character varying":
            field = Field(StringImpl(), size=record["character_maximum_length"], nullable=is_nullable)
        elif data_type == "character":
            l = record["character_maximum_length"]
            field = Field(StringImpl(), size=[l, l], nullable=is_nullable)
        elif data_type == "boolean":
            field = Field(BoolImpl(), nullable=is_nullable)
        elif data_type == "date":
            field = Field(DateImpl(), nullable=is_nullable)
        elif data_type == "timestamp with time zone":
            field = Field(DateTimeTzImpl(), nullable=is_nullable)
        elif data_type == "timestamp without time zone":
            field = Field(DateTimeImpl(), nullable=is_nullable)
        else:
            raise TypeError("Can't determine type from sql type: %r" % data_type)

        field._name_ = record["column_name"]

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
        fks = await self.get_foreign_keys(conn, schema, table)

        entity = registry[f"{schema}.{table}" if schema != "public" else table]

        for fk_desc in fks:
            field = getattr(entity, fk_desc["field_name"])

            if fk_desc["table_schema"] == "public":
                ref_entity = registry[fk_desc["table_name"]]
            else:
                ref_entity = registry[f"{fk_desc['table_schema']}.{fk_desc['table_name']}"]

            field // ForeignKey(getattr(ref_entity, fk_desc["column_name"]),
                name=fk_desc["constraint_name"],
                on_update=fk_desc["update_rule"],
                on_delete=fk_desc["delete_rule"])



