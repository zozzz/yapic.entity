import pytest
from datetime import datetime
from yapic.entity.sql import wrap_connection, Entity, sync
from yapic.entity import (Field, Serial, Int, String, Bytes, Date, DateTime, DateTimeTz, Bool, ForeignKey, PrimaryKey,
                          One, Query, func, EntityDiff, Registry, Json, Composite, Auto)

pytestmark = pytest.mark.asyncio


@pytest.yield_fixture
async def conn(pgsql):
    yield wrap_connection(pgsql, "pgsql")


class Address(Entity, schema="execution"):
    id: Serial
    title: String


class User(Entity, schema="execution"):
    id: Serial
    name: String = Field(size=100)
    bio: String
    fixed_char: String = Field(size=[5, 5])
    secret: Bytes

    address_id: Auto = ForeignKey(Address.id)
    address: One[Address]

    is_active: Bool = True
    birth_date: Date
    naive_date: DateTime = datetime(2019, 1, 1, 12, 34, 55)
    created_time: DateTimeTz = func.now()
    updated_time: DateTimeTz


class User2(Entity, schema="execution_private", name="User"):
    id: Serial
    name: String
    email: String
    address_id: Auto = ForeignKey(Address.id)
    address: One[Address]


async def test_ddl(conn):
    await conn.conn.execute("""DROP SCHEMA IF EXISTS "execution" CASCADE""")
    await conn.conn.execute("""DROP SCHEMA IF EXISTS "execution_private" CASCADE""")

    await conn.create_entity(Address, drop=True)
    await conn.create_entity(User, drop=True)
    await conn.create_entity(User2, drop=True)


async def test_basic_insert_update(conn):
    u = User(name="Jhon Doe", secret=b"bytes")

    assert await conn.insert(u) is True
    assert u.id == 1
    assert u.name == "Jhon Doe"
    assert u.secret == b"bytes"
    assert u.__state__.changes() == {}

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

    class User(Entity, schema="execution", registry=new_reg):
        id: Serial
        name_x: String = Field(size=100)
        bio: String = Field(size=200)
        fixed_char: Bytes
        secret: Bytes

        address_id: Auto = ForeignKey(Address.id)
        address: One[Address]

        is_active: Bool = True
        birth_date: String
        naive_date: DateTimeTz = func.now()
        created_time: DateTimeTz = func.CURRENT_TIMESTAMP
        updated_time: DateTimeTz

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

    assert result == """DROP TABLE "execution"."Address" CASCADE;
DROP TABLE "execution_private"."User" CASCADE;
CREATE SCHEMA IF NOT EXISTS "_private";
CREATE TABLE "_private"."NewTable" (
  "id" SERIAL4 NOT NULL,
  PRIMARY KEY("id")
);
CREATE TABLE "execution"."Gender" (
  "value" TEXT NOT NULL,
  "title" TEXT,
  PRIMARY KEY("value")
);
INSERT INTO "execution"."Gender" ("value", "title") VALUES ('male', 'Male');
INSERT INTO "execution"."Gender" ("value", "title") VALUES ('female', 'Female');
INSERT INTO "execution"."Gender" ("value", "title") VALUES ('other', 'Other');
ALTER TABLE "execution"."User"
  DROP COLUMN "name",
  ADD COLUMN "name_x" VARCHAR(100),
  ALTER COLUMN "bio" TYPE VARCHAR(200) USING "bio"::VARCHAR(200),
  ALTER COLUMN "fixed_char" TYPE BYTEA USING "fixed_char"::BYTEA,
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
    assert result == """INSERT INTO "execution"."Gender" ("value") VALUES ('insert');
UPDATE "execution"."Gender" SET "title"='MaleX' WHERE "value"='male';
UPDATE "execution"."Gender" SET "title"='FemaleY' WHERE "value"='female';"""
    await conn.conn.execute(result)


async def test_json(conn):
    await conn.conn.execute("DROP SCHEMA IF EXISTS _private CASCADE")
    await conn.conn.execute("DROP SCHEMA IF EXISTS execution CASCADE")

    reg_a = Registry()
    reg_b = Registry()

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

    result = await sync(conn, reg_a)
    assert result == """CREATE SCHEMA IF NOT EXISTS "execution";
CREATE TABLE "execution"."JsonUser" (
  "id" SERIAL4 NOT NULL,
  "name" JSONB,
  PRIMARY KEY("id")
);"""

    await conn.conn.execute(result)

    user = JsonUser(name={"given": "Given", "family": "Family", "xy": {"x": 1, "y": 2}})
    await conn.insert(user)
    assert user.name.given == "Given"
    assert user.name.family == "Family"
    assert user.name.xy.x == 1
    assert user.name.xy.y == 2


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

    result = await sync(conn, reg_a)
    assert result == """CREATE SCHEMA IF NOT EXISTS "execution";
CREATE TYPE "execution"."CompXY" AS (
  "x" TEXT,
  "y" TEXT
);
CREATE TYPE "execution"."CompName" AS (
  "given" TEXT,
  "family" TEXT,
  "xy" "execution"."CompXY"
);
CREATE TABLE "execution"."CompUser" (
  "id" SERIAL4 NOT NULL,
  "name" "execution"."CompName",
  PRIMARY KEY("id")
);"""
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
