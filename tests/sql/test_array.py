# flake8: noqa: E501

import pytest
from asyncpg.exceptions import NotNullViolationError
from yapic.entity import Entity, Field, IntArray, Query, Registry, Serial, StringArray
from yapic.entity.sql import PostgreDialect, sync

dialect = PostgreDialect()


async def test_array_sync(conn, pgclean):
    registry = Registry()

    class ArrayTest(Entity, registry=registry, schema="array"):
        strings: StringArray
        ints: IntArray

    result = await sync(conn, registry)
    assert result == """CREATE SCHEMA IF NOT EXISTS "array";
CREATE TABLE "array"."ArrayTest" (
  "strings" TEXT[],
  "ints" INT4[]
);"""

    await conn.execute(result)

    result = await sync(conn, registry)
    assert result is None

    registry2 = Registry()

    class ArrayTest(Entity, registry=registry2, schema="array"):
        strings: IntArray
        ints: StringArray

    result = await sync(conn, registry2)
    assert result == """ALTER TABLE "array"."ArrayTest"
  ALTER COLUMN "strings" TYPE INT4[] USING "strings"::INT4[],
  ALTER COLUMN "ints" TYPE TEXT[] USING "ints"::TEXT[];"""

    await conn.execute(result)

    result = await sync(conn, registry2)
    assert result is None


async def test_insert(conn, pgclean):
    registry = Registry()

    class ArrayTest(Entity, registry=registry, schema="array"):
        id: Serial
        strings: StringArray
        ints: IntArray
        not_null_ints: IntArray = Field(nullable=False)

    await conn.execute(await sync(conn, registry))

    with pytest.raises(NotNullViolationError):
        await conn.save(ArrayTest(id=1))

    with pytest.raises(NotNullViolationError):
        await conn.save(ArrayTest(id=1, not_null_ints=None))

    test = ArrayTest(id=2, not_null_ints=[])
    await conn.save(test)
    test_db = await conn.select(Query(ArrayTest).where(ArrayTest.id == 2)).first()
    assert test_db.strings is None
    assert test_db.ints is None
    assert test_db.not_null_ints == []

    test = ArrayTest(id=3, strings=["Hello", "World"], ints=[1, 2], not_null_ints=[42])
    await conn.save(test)
    test_db = await conn.select(Query(ArrayTest).where(ArrayTest.id == 3)).first()
    assert test_db.strings == ["Hello", "World"]
    assert test_db.ints == [1, 2]
    assert test_db.not_null_ints == [42]

async def test_update(conn, pgclean):
    registry = Registry()

    class ArrayTest(Entity, registry=registry, schema="array"):
        id: Serial
        strings: StringArray
        ints: IntArray

    await conn.execute(await sync(conn, registry))

    test = ArrayTest(id=1, strings=["Hello", "World"], ints=[1])
    await conn.save(test)
    test_db = await conn.select(Query(ArrayTest).where(ArrayTest.id == 1)).first()
    assert test_db.strings == ["Hello", "World"]
    assert test_db.ints == [1]  # never change

    test.strings.append("Again")
    await conn.save(test)
    test_db = await conn.select(Query(ArrayTest).where(ArrayTest.id == 1)).first()
    assert test_db.strings == ["Hello", "World", "Again"]
    assert test_db.ints == [1]  # never change

    test_db.strings.remove("Again")
    await conn.save(test_db)
    test_db = await conn.select(Query(ArrayTest).where(ArrayTest.id == 1)).first()
    assert test_db.strings == ["Hello", "World"]
    assert test_db.ints == [1]  # never change

    test.strings = ["Overwrite"]
    await conn.save(test)
    test_db = await conn.select(Query(ArrayTest).where(ArrayTest.id == 1)).first()
    assert test_db.strings == ["Overwrite"]
    assert test_db.ints == [1]  # never change

    test_db.strings = ["Overwrite2"]
    await conn.save(test_db)
    test_db = await conn.select(Query(ArrayTest).where(ArrayTest.id == 1)).first()
    assert test_db.strings == ["Overwrite2"]
    assert test_db.ints == [1]  # never change

    test_db = await conn.select(Query(ArrayTest).load(ArrayTest.id, ArrayTest.strings).where(ArrayTest.id == 1)).first()
    test_db.strings.append("PartialLoad")
    await conn.save(test_db)
    test_db = await conn.select(Query(ArrayTest).where(ArrayTest.id == 1)).first()
    assert test_db.strings == ["Overwrite2", "PartialLoad"]
    assert test_db.ints == [1]  # never change


    test_db.strings = None
    await conn.save(test_db)
    test_db = await conn.select(Query(ArrayTest).where(ArrayTest.id == 1)).first()
    assert test_db.strings is None


def test_query():
    R = Registry()

    class A(Entity, registry=R):
        id: Serial
        ints: IntArray

    q = Query(A).where(A.ints[0] == 1)
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."ints" FROM "A" "t0" WHERE ("t0"."ints")[1] = $1"""
    assert params == (1, )

    q = Query(A).where(A.ints.contains(1))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."ints" FROM "A" "t0" WHERE $1=ANY("t0"."ints")"""
    assert params == (1, )

    q = Query(A).where(~A.ints.contains(1))
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."ints" FROM "A" "t0" WHERE NOT($1=ANY("t0"."ints"))"""
