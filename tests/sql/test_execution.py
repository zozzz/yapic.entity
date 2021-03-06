# flake8: noqa: E501

from typing import List, TypedDict
import pytest
from datetime import datetime, date, time, tzinfo, timedelta
from decimal import Decimal
from yapic.entity.field import Choice
from yapic.entity.sql import wrap_connection, Entity, sync, PostgreDialect
from yapic.entity import (Field, Serial, Int, String, Bytes, Date, DateTime, DateTimeTz, Time, TimeTz, Bool, ForeignKey,
                          PrimaryKey, One, Query, func, EntityDiff, Registry, Json, JsonArray, Composite, Auto, Numeric,
                          Float, Point, UUID, virtual, StringArray, IntArray, CreatedTime, UpdatedTime, Enum)
from yapic import json

pytestmark = pytest.mark.asyncio
REGISTRY = Registry()
dialect = PostgreDialect()


@pytest.fixture
async def conn(pgsql):
    yield wrap_connection(pgsql, "pgsql")


class Address(Entity, schema="execution", registry=REGISTRY):
    id: Serial
    title: String


class User(Entity, schema="execution", registry=REGISTRY):
    id: Serial
    uuid: UUID
    name: String = Field(size=100)
    bio: String
    fixed_char: String = Field(size=[5, 5])
    secret: Bytes

    address_id: Auto = ForeignKey(Address.id)
    address: One[Address]

    salary: Numeric = Field(size=[15, 2])
    distance_mm: Float
    distance_km: Float = Field(size=8)

    point: Point

    is_active: Bool = True
    birth_date: Date
    naive_date: DateTime = datetime(2019, 1, 1, 12, 34, 55)
    created_time: DateTimeTz = func.now()
    updated_time: DateTimeTz
    time: Time
    time_tz: TimeTz

    @virtual
    def virtual_prop(self):
        return f"VIRTUAL:{self.id}"


class User2(Entity, schema="execution_private", name="User", registry=REGISTRY):
    id: Serial
    name: String
    email: String
    address_id: Auto = ForeignKey(Address.id)
    address: One[Address]


async def test_ddl(conn, pgclean):
    result = await sync(conn, REGISTRY)
    assert result == """CREATE SCHEMA IF NOT EXISTS "execution";
CREATE SEQUENCE "execution"."Address_id_seq";
CREATE TABLE "execution"."Address" (
  "id" INT4 NOT NULL DEFAULT nextval('"execution"."Address_id_seq"'::regclass),
  "title" TEXT,
  PRIMARY KEY("id")
);
CREATE SEQUENCE "execution"."User_id_seq";
CREATE TABLE "execution"."User" (
  "id" INT4 NOT NULL DEFAULT nextval('"execution"."User_id_seq"'::regclass),
  "uuid" UUID,
  "name" VARCHAR(100),
  "bio" TEXT,
  "fixed_char" CHAR(5),
  "secret" BYTEA,
  "address_id" INT4,
  "salary" NUMERIC(15, 2),
  "distance_mm" FLOAT4,
  "distance_km" FLOAT8,
  "point" POINT,
  "is_active" BOOLEAN NOT NULL DEFAULT TRUE,
  "birth_date" DATE,
  "naive_date" TIMESTAMP NOT NULL DEFAULT '2019-01-01 12:34:55.000000',
  "created_time" TIMESTAMPTZ NOT NULL DEFAULT now(),
  "updated_time" TIMESTAMPTZ,
  "time" TIME,
  "time_tz" TIMETZ,
  PRIMARY KEY("id")
);
CREATE SCHEMA IF NOT EXISTS "execution_private";
CREATE SEQUENCE "execution_private"."User_id_seq";
CREATE TABLE "execution_private"."User" (
  "id" INT4 NOT NULL DEFAULT nextval('"execution_private"."User_id_seq"'::regclass),
  "name" TEXT,
  "email" TEXT,
  "address_id" INT4,
  PRIMARY KEY("id")
);
CREATE INDEX "idx_User__address_id" ON "execution"."User" USING btree ("address_id");
ALTER TABLE "execution"."User"
  ADD CONSTRAINT "fk_User__address_id-Address__id" FOREIGN KEY ("address_id") REFERENCES "execution"."Address" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
CREATE INDEX "idx_User__address_id" ON "execution_private"."User" USING btree ("address_id");
ALTER TABLE "execution_private"."User"
  ADD CONSTRAINT "fk_User__address_id-Address__id" FOREIGN KEY ("address_id") REFERENCES "execution"."Address" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;"""
    await conn.conn.execute(result)

    result = await sync(conn, Address.__registry__)
    assert bool(result) is False


async def test_basic_insert_update(conn):
    u = User(name="Jhon Doe", secret=b"bytes", salary="1234.56", distance_mm=12345.25, distance_km=0.25)

    assert await conn.insert(u) is True
    assert u.id == 1
    assert u.name == "Jhon Doe"
    assert u.secret == b"bytes"
    assert u.salary == Decimal("1234.56")
    assert u.distance_mm == 12345.25
    assert u.distance_km == 0.25
    assert u.__state__.changes() == {}
    assert u.as_dict()["virtual_prop"] == f"VIRTUAL:{u.id}"

    u.name = "New Name"
    assert await conn.update(u) is True
    assert u.id == 1
    assert u.name == "New Name"
    assert u.__state__.changes() == {}

    assert await conn.delete(u) is True


async def test_insert_or_update(conn):
    u = User(name="Jhon Doe", secret=b"bytes")

    assert await conn.insert(u) is True
    assert u.id == 2
    assert u.name == "Jhon Doe"
    assert u.secret == b"bytes"
    assert u.__state__.changes() == {}

    u2 = User(id=u.id, name="Another Name")

    assert await conn.insert_or_update(u2) is True
    assert u2.id == 2
    assert u2.name == "Another Name"
    assert u2.secret == b"bytes"
    assert u2.__state__.changes() == {}


async def test_select(conn):
    u = User(id=1, name="Jhon Doe")

    assert await conn.insert(u) is True

    q = Query().select_from(User).where(User.id == 1)

    u = await conn.select(q).first()
    assert u.id == 1
    assert u.name == "Jhon Doe"
    assert u.is_active is True

    u.name = "New Name"
    await conn.update(u)
    assert u.name == "New Name"

    u2 = await conn.select(q).first()
    assert u2.id == 1
    assert u2.name == "New Name"


async def test_reflect(conn):
    ent_reg = await conn.reflect()

    def test_field(ent, field, impl, *, size=None, nullable=None, default=None):
        reflected = ent_reg[f"execution.{ent}"]
        attr = getattr(reflected, field)

        assert attr._name_ == field
        assert str(attr._impl_) == impl

        if size is not None:
            if isinstance(size, list):
                assert [attr.min_size, attr.max_size] == size
            else:
                assert attr.max_size == size

        if nullable is not None:
            assert attr.nullable is nullable

        if default is not None:
            assert attr._default_ == default

    test_field("User", "id", "Int", size=4, nullable=False)
    test_field("User", "name", "String", size=100, nullable=True)
    test_field("User", "bio", "String", size=-1, nullable=True)
    test_field("User", "fixed_char", "String", size=[5, 5], nullable=True)
    test_field("User", "secret", "Bytes", nullable=True)
    test_field("User", "address_id", "Int", size=4, nullable=True)
    test_field("User", "is_active", "Bool", nullable=False, default=True)
    test_field("User", "birth_date", "Date", nullable=True)
    test_field("User", "naive_date", "DateTime", nullable=False, default=datetime(2019, 1, 1, 12, 34, 55))
    test_field("User", "created_time", "DateTimeTz", nullable=False)
    test_field("User", "updated_time", "DateTimeTz", nullable=True)

    diff = EntityDiff(ent_reg["execution.User"], User)
    assert diff.changes == []


async def test_diff(conn):
    new_reg = Registry()

    class Address(Entity, schema="execution", registry=new_reg):
        id: Serial
        title: String

    class User(Entity, schema="execution", registry=new_reg):
        id: Serial
        uuid: UUID
        name_x: String = Field(size=100)
        bio: String = Field(size=200)
        fixed_char: Bytes
        secret: Bytes

        address_id: Auto = ForeignKey(Address.id)
        address: One[Address]

        salary: Numeric = Field(size=[15, 3])
        distance_mm: Float
        distance_km: Float = Field(size=4)

        point: Point

        is_active: Bool = True
        birth_date: String
        naive_date: DateTimeTz = func.now()
        created_time: DateTimeTz = func.CURRENT_TIMESTAMP
        updated_time: DateTimeTz
        time: Time
        time_tz: TimeTz

    class NewTable(Entity, registry=new_reg, schema="_private"):
        id: Serial

    class Gender(Entity, registry=new_reg, schema="execution"):
        value: String = PrimaryKey()
        title: String

    Gender.__fix_entries__ = [
        Gender(value="male", title="Male"),
        Gender(value="female", title="Female"),
        Gender(value="other", title="Other"),
    ]

    await conn.conn.execute("DROP SCHEMA IF EXISTS _private CASCADE")

    result = await sync(conn, new_reg)

    assert result == """DROP SEQUENCE "execution_private"."User_id_seq" CASCADE;
DROP TABLE "execution_private"."User" CASCADE;
CREATE SCHEMA IF NOT EXISTS "_private";
CREATE SEQUENCE "_private"."NewTable_id_seq";
CREATE TABLE "_private"."NewTable" (
  "id" INT4 NOT NULL DEFAULT nextval('"_private"."NewTable_id_seq"'::regclass),
  PRIMARY KEY("id")
);
CREATE TABLE "execution"."Gender" (
  "value" TEXT NOT NULL,
  "title" TEXT,
  PRIMARY KEY("value")
);
INSERT INTO "execution"."Gender" ("value", "title") VALUES ('male', 'Male') ON CONFLICT ("value") DO UPDATE SET "title"='Male';
INSERT INTO "execution"."Gender" ("value", "title") VALUES ('female', 'Female') ON CONFLICT ("value") DO UPDATE SET "title"='Female';
INSERT INTO "execution"."Gender" ("value", "title") VALUES ('other', 'Other') ON CONFLICT ("value") DO UPDATE SET "title"='Other';
ALTER TABLE "execution"."User"
  DROP COLUMN "name",
  ADD COLUMN "name_x" VARCHAR(100),
  ALTER COLUMN "bio" TYPE VARCHAR(200) USING "bio"::VARCHAR(200),
  ALTER COLUMN "fixed_char" TYPE BYTEA USING "fixed_char"::BYTEA,
  ALTER COLUMN "salary" TYPE NUMERIC(15, 3) USING "salary"::NUMERIC(15, 3),
  ALTER COLUMN "distance_km" TYPE FLOAT4 USING "distance_km"::FLOAT4,
  ALTER COLUMN "birth_date" TYPE TEXT USING "birth_date"::TEXT,
  ALTER COLUMN "naive_date" TYPE TIMESTAMPTZ USING "naive_date"::TIMESTAMPTZ,
  ALTER COLUMN "naive_date" SET DEFAULT now(),
  ALTER COLUMN "created_time" SET DEFAULT CURRENT_TIMESTAMP;"""

    await conn.conn.execute(result)

    result = await sync(conn, new_reg)
    assert bool(result) is False

    # remove gender value
    Gender.__fix_entries__ = [
        Gender(value="male", title="Male"),
        Gender(value="female", title="Female"),
    ]
    result = await sync(conn, new_reg)
    assert result == """DELETE FROM "execution"."Gender" WHERE "value"='other';"""
    await conn.conn.execute(result)

    Gender.__fix_entries__ = [
        Gender(value="male", title="MaleX"),
        Gender(value="female", title="FemaleY"),
        Gender(value="insert"),
    ]
    result = await sync(conn, new_reg)
    assert result == """INSERT INTO "execution"."Gender" ("value") VALUES ('insert') ON CONFLICT ("value") DO NOTHING;
UPDATE "execution"."Gender" SET "title"='MaleX' WHERE "value"='male';
UPDATE "execution"."Gender" SET "title"='FemaleY' WHERE "value"='female';"""
    await conn.conn.execute(result)


async def test_diff_defaults(conn, pgclean):
    reg = Registry()

    class Defaults(Entity, registry=reg, schema="execution"):
        int_w_def: Int = 0
        int2_w_def: Int = Field(size=2, default=1)
        string_w_def: String = "Hello"
        bool_w_def: Bool = True
        interval: Int = Field(nullable=False)

    result = await sync(conn, reg)
    assert result == """CREATE SCHEMA IF NOT EXISTS "execution";
CREATE TABLE "execution"."Defaults" (
  "int_w_def" INT4 NOT NULL DEFAULT 0,
  "int2_w_def" INT2 NOT NULL DEFAULT 1,
  "string_w_def" TEXT NOT NULL DEFAULT 'Hello',
  "bool_w_def" BOOLEAN NOT NULL DEFAULT TRUE,
  "interval" INT4 NOT NULL
);"""
    await conn.conn.execute(result)

    result = await sync(conn, reg)
    assert bool(result) is False


async def test_json(conn, pgclean):
    reg_a = Registry()
    reg_b = Registry()

    class JsonTyped(TypedDict):
        field1: str
        field2: int

    class JsonXY(Entity, registry=reg_a, schema="execution"):
        x: Int
        y: Int

    class JsonName(Entity, registry=reg_a, schema="execution"):
        given: String
        family: String
        xy: Json[JsonXY]

    class JsonUser(Entity, registry=reg_a, schema="execution"):
        id: Serial
        name: Json[JsonName]
        points: Json[List[JsonXY]]
        typed: Json[JsonTyped]

    result = await sync(conn, reg_a)
    assert result == """CREATE SCHEMA IF NOT EXISTS "execution";
CREATE SEQUENCE "execution"."JsonUser_id_seq";
CREATE TABLE "execution"."JsonUser" (
  "id" INT4 NOT NULL DEFAULT nextval('"execution"."JsonUser_id_seq"'::regclass),
  "name" JSONB,
  "points" JSONB,
  "typed" JSONB,
  PRIMARY KEY("id")
);"""

    await conn.conn.execute(result)

    points = [
        {
            "x": 1,
            "y": 2
        },
        {
            "x": 10,
            "y": 20
        },
        {
            "x": 30,
            "y": 42
        },
    ]

    user = JsonUser(
        name={
            "given": "Given",
            "family": "Family",
            "xy": {
                "x": 1,
                "y": 2
            }
        },
        points=points,
        typed={
            "field1": "str",
            "field2": 42
        },
    )
    await conn.insert(user)
    assert user.name.given == "Given"
    assert user.name.family == "Family"
    assert user.name.xy.x == 1
    assert user.name.xy.y == 2

    assert isinstance(user.points[0], JsonXY)
    assert user.points[0].x == 1
    assert user.points[0].y == 2

    assert isinstance(user.points[1], JsonXY)
    assert user.points[1].x == 10
    assert user.points[1].y == 20

    assert isinstance(user.points[2], JsonXY)
    assert user.points[2].x == 30
    assert user.points[2].y == 42

    assert isinstance(user.typed, dict)
    assert user.typed["field1"] == "str"
    assert user.typed["field2"] == 42

    result = await sync(conn, reg_a)
    assert bool(result) is False


async def test_json_fix(conn, pgclean):
    reg_a = Registry()

    class JsonName(Entity, registry=reg_a, schema="execution"):
        given: String
        family: String

    class JsonUser(Entity, registry=reg_a, schema="execution"):
        id: Serial
        name: Json[JsonName]

    JsonUser.__fix_entries__ = [
        JsonUser(id=1, name={
            "given": "Given",
            "family": "Family"
        }),
        JsonUser(id=2),
    ]
    result = await sync(conn, reg_a)
    assert result == """CREATE SCHEMA IF NOT EXISTS "execution";
CREATE SEQUENCE "execution"."JsonUser_id_seq";
CREATE TABLE "execution"."JsonUser" (
  "id" INT4 NOT NULL DEFAULT nextval('"execution"."JsonUser_id_seq"'::regclass),
  "name" JSONB,
  PRIMARY KEY("id")
);
INSERT INTO "execution"."JsonUser" ("id", "name") VALUES (1, '{"given":"Given","family":"Family"}') ON CONFLICT ("id") DO UPDATE SET "name"='{"given":"Given","family":"Family"}';
INSERT INTO "execution"."JsonUser" ("id", "name") VALUES (2, '{"given":null,"family":null}') ON CONFLICT ("id") DO UPDATE SET "name"='{"given":null,"family":null}';"""

    await conn.conn.execute(result)

    result = await sync(conn, reg_a)
    assert result is None


async def test_composite(conn):
    await conn.conn.execute("DROP SCHEMA IF EXISTS _private CASCADE")
    await conn.conn.execute("DROP SCHEMA IF EXISTS execution CASCADE")

    reg_a = Registry()
    reg_b = Registry()

    class CompXY(Entity, registry=reg_a, schema="execution"):
        x: String
        y: String

    class CompName(Entity, registry=reg_a, schema="execution"):
        given: String
        family: String
        xy: Composite[CompXY]

    class CompUser(Entity, registry=reg_a, schema="execution"):
        id: Serial
        name: Composite[CompName]

    class Article(Entity, registry=reg_a, schema="execution"):
        id: Serial
        author_id: Auto = ForeignKey(CompUser.id)
        author: One[CompUser]

    result = await sync(conn, reg_a)
    assert result == """CREATE SCHEMA IF NOT EXISTS "execution";
CREATE SEQUENCE "execution"."Article_id_seq";
CREATE TYPE "execution"."CompXY" AS (
  "x" TEXT,
  "y" TEXT
);
CREATE TYPE "execution"."CompName" AS (
  "given" TEXT,
  "family" TEXT,
  "xy" "execution"."CompXY"
);
CREATE SEQUENCE "execution"."CompUser_id_seq";
CREATE TABLE "execution"."CompUser" (
  "id" INT4 NOT NULL DEFAULT nextval('"execution"."CompUser_id_seq"'::regclass),
  "name" "execution"."CompName",
  PRIMARY KEY("id")
);
CREATE TABLE "execution"."Article" (
  "id" INT4 NOT NULL DEFAULT nextval('"execution"."Article_id_seq"'::regclass),
  "author_id" INT4,
  PRIMARY KEY("id")
);
CREATE INDEX "idx_Article__author_id" ON "execution"."Article" USING btree ("author_id");
ALTER TABLE "execution"."Article"
  ADD CONSTRAINT "fk_Article__author_id-CompUser__id" FOREIGN KEY ("author_id") REFERENCES "execution"."CompUser" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;"""
    await conn.conn.execute(result)

    # TODO: kitalálni, hogyan lehet módosítani a composite typeot
    #     class CompName(Entity, registry=reg_b, schema="execution"):
    #         _given: String
    #         family: String = Field(size=50)
    #         new_column: Int

    #     class CompUser(Entity, registry=reg_b, schema="execution"):
    #         id: Serial
    #         name: Composite[CompName]

    #     result = await sync(conn, reg_b)
    #     assert result == """ALTER TYPE "execution"."CompName"
    #   DROP ATTRIBUTE "given",
    #   ADD ATTRIBUTE "_given" TEXT,
    #   ADD ATTRIBUTE "new_column" INT4,
    #   ALTER ATTRIBUTE "family" TYPE VARCHAR(50);"""

    #     await conn.conn.execute(result)
    #     result = await sync(conn, reg_b)
    #     assert result is None

    user = CompUser(name={"family": "Family", "given": "Given", "xy": {"x": "X", "y": "Y"}})
    assert user.name.family == "Family"
    assert user.name.given == "Given"
    assert user.name.xy.x == "X"
    assert user.name.xy.y == "Y"

    await conn.insert(user)
    assert user.name.family == "Family"
    assert user.name.given == "Given"
    assert user.name.xy.x == "X"
    assert user.name.xy.y == "Y"
    assert user.__state__.changes() == {}
    assert user.name.__state__.changes() == {}

    user.name.family = "FamilyModified"
    user.name.xy.y = "Y MOD"
    assert user.__state__.changes() == {"name": user.name}
    assert user.name.__state__.changes() == {"family": "FamilyModified", "xy": user.name.xy}

    await conn.update(user)
    assert user.name.family == "FamilyModified"
    assert user.name.given == "Given"
    assert user.name.xy.x == "X"
    assert user.name.xy.y == "Y MOD"
    assert user.__state__.changes() == {}
    assert user.name.__state__.changes() == {}

    user.name.family = "Family IOU"
    user.name.xy.y = "Y IOU"
    await conn.insert_or_update(user)
    assert user.name.family == "Family IOU"
    assert user.name.given == "Given"
    assert user.name.xy.x == "X"
    assert user.name.xy.y == "Y IOU"
    assert user.__state__.changes() == {}
    assert user.name.__state__.changes() == {}

    q = Query().select_from(CompUser).where(CompUser.id == user.id)
    user = await conn.select(q).first()
    assert user.name.family == "Family IOU"
    assert user.name.given == "Given"
    assert user.name.xy.x == "X"
    assert user.name.xy.y == "Y IOU"

    q = Query().select_from(CompUser).columns(CompUser.id, CompUser.name.xy.y).where(CompUser.id == user.id)
    res = await conn.select(q).first()
    assert res == (user.id, "Y IOU")

    q = Query().select_from(CompUser).columns(CompUser.id, CompUser.name).where(CompUser.id == user.id)
    res = await conn.select(q).first()
    assert res[0] == user.id
    assert res[1].family == "Family IOU"
    assert res[1].given == "Given"
    assert res[1].xy.x == "X"
    assert res[1].xy.y == "Y IOU"

    q = Query().select_from(CompUser).columns(CompUser.id, CompUser.name.xy).where(CompUser.id == user.id)
    res = await conn.select(q).first()
    assert res[0] == user.id
    assert res[1].x == "X"
    assert res[1].y == "Y IOU"

    article = Article()
    article.author_id = user.id
    await conn.save(article)

    q = Query().select_from(Article).columns(Article.id, Article.author.name).where(Article.id == article.id)
    res = await conn.select(q).first()
    assert res[0] == article.id
    assert res[1].family == "Family IOU"
    assert res[1].given == "Given"
    assert res[1].xy.x == "X"
    assert res[1].xy.y == "Y IOU"


async def test_callable_default(conn):
    await conn.conn.execute("DROP SCHEMA IF EXISTS _private CASCADE")
    await conn.conn.execute("DROP SCHEMA IF EXISTS execution CASCADE")
    await conn.conn.execute("DROP SCHEMA IF EXISTS execution_private CASCADE")

    reg = Registry()

    class CallableDefault(Entity, registry=reg, schema="execution"):
        id: Serial
        creator_id: Int = lambda: 1

    result = await sync(conn, reg)
    assert result == """CREATE SCHEMA IF NOT EXISTS "execution";
CREATE SEQUENCE "execution"."CallableDefault_id_seq";
CREATE TABLE "execution"."CallableDefault" (
  "id" INT4 NOT NULL DEFAULT nextval('"execution"."CallableDefault_id_seq"'::regclass),
  "creator_id" INT4,
  PRIMARY KEY("id")
);"""

    await conn.conn.execute(result)

    result = await sync(conn, reg)
    assert bool(result) is False

    reg = Registry()

    class CallableDefault(Entity, registry=reg, schema="execution"):
        id: Serial
        creator_id: Int = Field(size=4, nullable=False, default=lambda: 1)

    result = await sync(conn, reg)
    assert result == """ALTER TABLE "execution"."CallableDefault"
  ALTER COLUMN "creator_id" SET NOT NULL;"""


async def test_pk_change(conn):
    await conn.conn.execute("DROP SCHEMA IF EXISTS _private CASCADE")
    await conn.conn.execute("DROP SCHEMA IF EXISTS execution CASCADE")
    await conn.conn.execute("DROP SCHEMA IF EXISTS execution_private CASCADE")

    reg = Registry()

    class Address(Entity, registry=reg, schema="execution"):
        id: Int

    result = await sync(conn, reg)
    assert result == """CREATE SCHEMA IF NOT EXISTS "execution";
CREATE TABLE "execution"."Address" (
  "id" INT4
);"""
    await conn.conn.execute(result)

    # ADD PRIMARY KEY

    reg = Registry()

    class Address(Entity, registry=reg, schema="execution"):
        id: Serial

    result = await sync(conn, reg)
    assert result == """CREATE SEQUENCE "execution"."Address_id_seq";
ALTER TABLE "execution"."Address"
  ALTER COLUMN "id" SET DEFAULT nextval('"execution"."Address_id_seq"'::regclass),
  ALTER COLUMN "id" SET NOT NULL,
  ADD PRIMARY KEY("id");"""
    await conn.conn.execute(result)

    # DROP PRIMARY KEY

    reg = Registry()

    class Address(Entity, registry=reg, schema="execution"):
        id: String

    result = await sync(conn, reg)
    assert result == """DROP SEQUENCE "execution"."Address_id_seq" CASCADE;
ALTER TABLE "execution"."Address"
  DROP CONSTRAINT IF EXISTS "Address_pkey",
  ALTER COLUMN "id" DROP NOT NULL,
  ALTER COLUMN "id" TYPE TEXT USING "id"::TEXT,
  ALTER COLUMN "id" DROP DEFAULT;"""
    await conn.conn.execute(result)

    # CHANGE PRIMARY KEY

    reg = Registry()

    class Address(Entity, registry=reg, schema="execution"):
        id: Serial

    result = await sync(conn, reg)
    assert result == """CREATE SEQUENCE "execution"."Address_id_seq";
ALTER TABLE "execution"."Address"
  ALTER COLUMN "id" TYPE INT4 USING "id"::INT4,
  ALTER COLUMN "id" SET DEFAULT nextval('"execution"."Address_id_seq"'::regclass),
  ALTER COLUMN "id" SET NOT NULL,
  ADD PRIMARY KEY("id");"""
    await conn.conn.execute(result)

    reg = Registry()

    class Address(Entity, registry=reg, schema="execution"):
        id: Serial
        id2: Serial

    result = await sync(conn, reg)
    assert result == """CREATE SEQUENCE "execution"."Address_id2_seq";
ALTER TABLE "execution"."Address"
  DROP CONSTRAINT IF EXISTS "Address_pkey",
  ADD COLUMN "id2" INT4 NOT NULL DEFAULT nextval('"execution"."Address_id2_seq"'::regclass),
  ADD PRIMARY KEY("id", "id2");"""
    await conn.conn.execute(result)


async def test_fk_change(conn):
    await conn.conn.execute("DROP SCHEMA IF EXISTS _private CASCADE")
    await conn.conn.execute("DROP SCHEMA IF EXISTS execution CASCADE")
    await conn.conn.execute("DROP SCHEMA IF EXISTS execution_private CASCADE")

    reg = Registry()

    class Address(Entity, registry=reg, schema="execution"):
        id: Serial

    class User(Entity, registry=reg, schema="execution"):
        id: Serial
        address_id: Auto = ForeignKey(Address.id)

    result = await sync(conn, reg)
    assert result == """CREATE SCHEMA IF NOT EXISTS "execution";
CREATE SEQUENCE "execution"."Address_id_seq";
CREATE TABLE "execution"."Address" (
  "id" INT4 NOT NULL DEFAULT nextval('"execution"."Address_id_seq"'::regclass),
  PRIMARY KEY("id")
);
CREATE SEQUENCE "execution"."User_id_seq";
CREATE TABLE "execution"."User" (
  "id" INT4 NOT NULL DEFAULT nextval('"execution"."User_id_seq"'::regclass),
  "address_id" INT4,
  PRIMARY KEY("id")
);
CREATE INDEX "idx_User__address_id" ON "execution"."User" USING btree ("address_id");
ALTER TABLE "execution"."User"
  ADD CONSTRAINT "fk_User__address_id-Address__id" FOREIGN KEY ("address_id") REFERENCES "execution"."Address" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;"""
    await conn.conn.execute(result)

    # DROP FK

    reg = Registry()

    class Address(Entity, registry=reg, schema="execution"):
        id: Serial

    class User(Entity, registry=reg, schema="execution"):
        id: Serial
        address_id: Int

    result = await sync(conn, reg)
    assert result == """DROP INDEX IF EXISTS "execution"."idx_User__address_id";
ALTER TABLE "execution"."User"
  DROP CONSTRAINT IF EXISTS "fk_User__address_id-Address__id";"""
    await conn.conn.execute(result)

    # ADD FK

    reg = Registry()

    class Address(Entity, registry=reg, schema="execution"):
        id: Serial

    class User(Entity, registry=reg, schema="execution"):
        id: Serial
        address_id: Auto = ForeignKey(Address.id)

    result = await sync(conn, reg)
    assert result == """ALTER TABLE "execution"."User"
  ADD CONSTRAINT "fk_User__address_id-Address__id" FOREIGN KEY ("address_id") REFERENCES "execution"."Address" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
CREATE INDEX "idx_User__address_id" ON "execution"."User" USING btree ("address_id");"""
    await conn.conn.execute(result)


async def test_point(conn, pgclean):
    reg = Registry()

    class PointTest(Entity, registry=reg, schema="execution"):
        id: Serial
        point: Point

    result = await sync(conn, reg)
    await conn.conn.execute(result)

    # TODO: kezelni, hogy adatbázisból van-e betöltve vagy sem
    p = PointTest(id=1, point=(1.25, 2.25))
    assert p.point.x == 1.25
    assert p.point.y == 2.25
    assert await conn.save(p) is True

    ps = await conn.select(Query().select_from(PointTest).where(PointTest.id == 1)).first()
    assert ps.point.x == 1.25
    assert ps.point.y == 2.25

    p = PointTest(id=1, point=(5.25, 6.25))
    assert await conn.save(p) is True
    assert p.point.x == 5.25
    assert p.point.y == 6.25


async def test_date_types(conn):
    reg = Registry()

    class DateTest(Entity, registry=reg, schema="execution"):
        id: Serial
        date: Date
        date_time: DateTime
        date_time_tz: DateTimeTz
        time: Time
        time_tz: TimeTz

    class FixedTz(tzinfo):
        def __init__(self, utcoffset):
            self._utcoffset = timedelta(hours=utcoffset)
            self._dst = timedelta(hours=0)

        def utcoffset(self, dt):
            return self._utcoffset

        def dst(self, dt):
            return self._dst

    result = await sync(conn, reg)
    await conn.conn.execute(result)

    inst = DateTest(
        date=date(2001, 12, 21),
        date_time=datetime(2019, 6, 1, 12, 23, 34),
        date_time_tz=datetime(2019, 6, 1, 12, 23, 34, tzinfo=FixedTz(5)),
        time=time(12, 23, 34),
        time_tz=time(12, 23, 34, tzinfo=FixedTz(6)),
    )

    await conn.save(inst)
    obj = await conn.select(Query(DateTest)).first()
    assert obj.date == date(2001, 12, 21)
    assert obj.date_time == datetime(2019, 6, 1, 12, 23, 34)
    assert obj.date_time_tz == datetime(2019, 6, 1, 12, 23, 34, tzinfo=FixedTz(5))
    assert obj.time == time(12, 23, 34)
    assert obj.time_tz == time(12, 23, 34, tzinfo=FixedTz(6))


async def test_virtual_load(conn):
    registry = Registry()

    class VirtualLoad(Entity, registry=registry, schema="execution"):
        id: Int
        data_1: String
        data_2: String

        @virtual
        def data_concat(cls):
            return "NotLoaded"

        @data_concat.value
        def data_concat_val(cls, q):
            return func.CONCAT_WS(" / ", cls.data_1, cls.data_2)

    result = await sync(conn, registry)
    if result:
        await conn.conn.execute(result)

    inst = VirtualLoad(id=1, data_1="Hello", data_2="World")
    await conn.save(inst)

    query = Query(VirtualLoad).load(VirtualLoad.data_concat)
    sql, params = dialect.create_query_compiler().compile_select(query)
    assert sql == 'SELECT CONCAT_WS($1, "t0"."data_1", "t0"."data_2") FROM "execution"."VirtualLoad" "t0"'
    assert params == (" / ", )

    obj = await conn.select(query).first()
    assert obj.data_concat == "Hello / World"

    query = Query(VirtualLoad)
    sql, params = dialect.create_query_compiler().compile_select(query)
    assert sql == 'SELECT "t0"."id", "t0"."data_1", "t0"."data_2" FROM "execution"."VirtualLoad" "t0"'
    assert len(params) == 0

    obj = await conn.select(query).first()
    assert obj.data_concat == "NotLoaded"


async def test_array(conn, pgclean):
    registry = Registry()

    class ArrayTest(Entity, registry=registry, schema="execution"):
        strings: StringArray
        ints: IntArray

    result = await sync(conn, registry)
    assert result == """CREATE SCHEMA IF NOT EXISTS "execution";
CREATE TABLE "execution"."ArrayTest" (
  "strings" TEXT[],
  "ints" INT[]
);"""

    await conn.conn.execute(result)

    result = await sync(conn, registry)
    assert result is None

    registry2 = Registry()

    class ArrayTest(Entity, registry=registry2, schema="execution"):
        id: Serial
        strings: StringArray
        ints: StringArray

    result = await sync(conn, registry2)
    assert result == """CREATE SEQUENCE "execution"."ArrayTest_id_seq";
ALTER TABLE "execution"."ArrayTest"
  ADD COLUMN "id" INT4 NOT NULL DEFAULT nextval('"execution"."ArrayTest_id_seq"'::regclass),
  ALTER COLUMN "ints" TYPE TEXT[] USING "ints"::TEXT[],
  ADD PRIMARY KEY("id");"""

    await conn.conn.execute(result)

    result = await sync(conn, registry2)
    assert result is None

    inst = ArrayTest(strings=["Hello", "World"])
    await conn.save(inst)
    inst = await conn.select(Query(ArrayTest).where(ArrayTest.strings.contains("Hello"))).first()
    assert inst is not None
    assert inst.strings == ["Hello", "World"]

    inst.strings.append("Some Value")
    await conn.save(inst)
    inst = await conn.select(Query(ArrayTest).where(ArrayTest.strings.contains("Hello"))).first()
    assert inst.strings == ["Hello", "World", "Some Value"]

    inst.strings.append("42")
    await conn.save(inst)
    inst.strings.append("43")
    await conn.save(inst)
    inst = await conn.select(Query(ArrayTest).where(ArrayTest.strings.contains("43"))).first()
    assert inst.strings == ["Hello", "World", "Some Value", "42", "43"]


async def test_updated_time(conn, pgclean):
    R = Registry()

    class UT(Entity, registry=R, schema="execution"):
        id: Serial
        value: String
        created_time: CreatedTime
        updated_time: UpdatedTime

    result = await sync(conn, R)
    assert result == """CREATE SCHEMA IF NOT EXISTS "execution";
CREATE SEQUENCE "execution"."UT_id_seq";
CREATE TABLE "execution"."UT" (
  "id" INT4 NOT NULL DEFAULT nextval('"execution"."UT_id_seq"'::regclass),
  "value" TEXT,
  "created_time" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_time" TIMESTAMPTZ,
  PRIMARY KEY("id")
);
CREATE OR REPLACE FUNCTION "execution"."YT-UT-update-updated_time-8085b1-c1c14d"() RETURNS TRIGGER AS $$ BEGIN
  NEW."updated_time" = CURRENT_TIMESTAMP;
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "update-updated_time"
  BEFORE UPDATE ON "execution"."UT"
  FOR EACH ROW
  WHEN (OLD.* IS DISTINCT FROM NEW.*)
  EXECUTE FUNCTION "execution"."YT-UT-update-updated_time-8085b1-c1c14d"();"""

    await conn.conn.execute(result)
    result = await sync(conn, R)
    assert result is None

    inst = UT(id=1)
    await conn.save(inst)
    inst = await conn.select(Query(UT).where(UT.id == 1)).first()
    assert inst.updated_time is None

    inst.value = "Something"
    await conn.save(inst)
    inst = await conn.select(Query(UT).where(UT.id == 1)).first()
    assert inst.updated_time is not None
    ut1 = inst.updated_time

    await conn.save(inst)
    inst = await conn.select(Query(UT).where(UT.id == 1)).first()
    assert inst.updated_time == ut1

    inst.value = "Hello World"
    await conn.save(inst)
    inst = await conn.select(Query(UT).where(UT.id == 1)).first()
    assert inst.updated_time > ut1


async def test_on_update(conn, pgclean):
    R = Registry()

    async def get_user_id(entity):
        return 2

    class OnUpdate(Entity, registry=R, schema="execution"):
        id: Serial
        value: String
        updater_id: Int = Field(on_update=lambda entity: 1)
        updater_id2: Int = Field(on_update=get_user_id)

    result = await sync(conn, R)
    await conn.conn.execute(result)

    inst = OnUpdate(id=1)
    await conn.save(inst)
    inst = await conn.select(Query(OnUpdate).where(OnUpdate.id == 1)).first()
    assert inst.updater_id is None
    assert inst.updater_id2 is None

    inst.value = "X"
    await conn.save(inst)
    inst = await conn.select(Query(OnUpdate).where(OnUpdate.id == 1)).first()
    assert inst.updater_id == 1
    assert inst.updater_id2 == 2


async def test_enum(conn, pgclean):
    R = Registry()

    class Point(Entity, registry=R, schema="execution"):
        x: Int
        y: Int

    class StringEnum(Enum, registry=R, schema="execution"):
        PAUSED = "Paused"
        RUNNING = "Running"

    class IntEnum(Enum, registry=R, schema="execution"):
        value: Int = PrimaryKey()

        PAUSED = dict(value=1, label="Paused")
        RUNNING = dict(value=2, label="Running")

    class CompositeEnum(Enum, registry=R, schema="execution"):
        point: Composite[Point]

        ORIGO = dict(point=Point(x=0, y=0))

    class EnumTest(Entity, registry=R, schema="execution"):
        id: Serial
        str_enum: Choice[StringEnum]
        int_enum: Choice[IntEnum]
        point: Choice[CompositeEnum]

    result = await sync(conn, R)
    assert result == """CREATE SCHEMA IF NOT EXISTS "execution";
CREATE TYPE "execution"."Point" AS (
  "x" INT4,
  "y" INT4
);
CREATE TABLE "execution"."CompositeEnum" (
  "point" "execution"."Point",
  "value" TEXT NOT NULL,
  "label" TEXT,
  "index" INT4,
  PRIMARY KEY("value")
);
INSERT INTO "execution"."CompositeEnum" ("point"."x", "point"."y", "value", "index") VALUES (0, 0, 'ORIGO', 0) ON CONFLICT ("value") DO UPDATE SET "point"."x"=0, "point"."y"=0, "index"=0;
CREATE SEQUENCE "execution"."EnumTest_id_seq";
CREATE TABLE "execution"."IntEnum" (
  "value" INT4 NOT NULL,
  "label" TEXT,
  "index" INT4,
  PRIMARY KEY("value")
);
INSERT INTO "execution"."IntEnum" ("value", "label", "index") VALUES (1, 'Paused', 0) ON CONFLICT ("value") DO UPDATE SET "label"='Paused', "index"=0;
INSERT INTO "execution"."IntEnum" ("value", "label", "index") VALUES (2, 'Running', 1) ON CONFLICT ("value") DO UPDATE SET "label"='Running', "index"=1;
CREATE TABLE "execution"."StringEnum" (
  "value" TEXT NOT NULL,
  "label" TEXT,
  "index" INT4,
  PRIMARY KEY("value")
);
INSERT INTO "execution"."StringEnum" ("value", "label", "index") VALUES ('PAUSED', 'Paused', 0) ON CONFLICT ("value") DO UPDATE SET "label"='Paused', "index"=0;
INSERT INTO "execution"."StringEnum" ("value", "label", "index") VALUES ('RUNNING', 'Running', 1) ON CONFLICT ("value") DO UPDATE SET "label"='Running', "index"=1;
CREATE TABLE "execution"."EnumTest" (
  "id" INT4 NOT NULL DEFAULT nextval('"execution"."EnumTest_id_seq"'::regclass),
  "str_enum" TEXT,
  "int_enum" INT4,
  "point" TEXT,
  PRIMARY KEY("id")
);
CREATE INDEX "idx_EnumTest__str_enum" ON "execution"."EnumTest" USING btree ("str_enum");
CREATE INDEX "idx_EnumTest__int_enum" ON "execution"."EnumTest" USING btree ("int_enum");
CREATE INDEX "idx_EnumTest__point" ON "execution"."EnumTest" USING btree ("point");
ALTER TABLE "execution"."EnumTest"
  ADD CONSTRAINT "fk_EnumTest__str_enum-StringEnum__value" FOREIGN KEY ("str_enum") REFERENCES "execution"."StringEnum" ("value") ON UPDATE RESTRICT ON DELETE RESTRICT,
  ADD CONSTRAINT "fk_EnumTest__int_enum-IntEnum__value" FOREIGN KEY ("int_enum") REFERENCES "execution"."IntEnum" ("value") ON UPDATE RESTRICT ON DELETE RESTRICT,
  ADD CONSTRAINT "fk_EnumTest__point-CompositeEnum__value" FOREIGN KEY ("point") REFERENCES "execution"."CompositeEnum" ("value") ON UPDATE RESTRICT ON DELETE RESTRICT;"""

    await conn.conn.execute(result)

    result = await sync(conn, R)
    assert result is None

    inst = EnumTest(id=1, str_enum=StringEnum.PAUSED, int_enum=IntEnum.RUNNING)
    await conn.save(inst)
    assert inst.str_enum is StringEnum.PAUSED
    assert inst.int_enum == IntEnum.RUNNING

    inst = await conn.select(Query(EnumTest).where(EnumTest.id == 1)).first()
    assert inst.str_enum is StringEnum.PAUSED
    assert inst.int_enum == IntEnum.RUNNING

    json_str = json.dumps(inst)
    assert json_str == """{"id":1,"str_enum":{"value":"PAUSED","label":"Paused","index":0},"int_enum":{"value":2,"label":"Running","index":1}}"""

    inst = EnumTest(id=2, str_enum="PAUSED", int_enum=2)
    assert inst.str_enum is StringEnum.PAUSED
    assert inst.int_enum == IntEnum.RUNNING
    await conn.save(inst)
    assert inst.str_enum is StringEnum.PAUSED
    assert inst.int_enum == IntEnum.RUNNING
