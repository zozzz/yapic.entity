# flake8: noqa: E501

from datetime import date, datetime, time, timedelta, tzinfo
from decimal import Decimal

import pytest

from yapic.entity import (
    UUID,
    Auto,
    Bool,
    Bytes,
    Check,
    Composite,
    Date,
    DateTime,
    DateTimeTz,
    Entity,
    EntityDiff,
    Field,
    Float,
    ForeignKey,
    Int,
    Json,
    JsonArray,
    Numeric,
    One,
    Point,
    PrimaryKey,
    Query,
    Registry,
    Serial,
    String,
    Time,
    TimeTz,
    func,
    virtual,
)
from yapic.entity.sql import PostgreTrigger, sync

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("name", [None, "check-qty-ordred"])
async def test_new_single_ddl(conn, pgclean, name):
    reg = Registry()
    const_name = "chk_CheckT__qty_ordered" if not name else name

    class CheckT(Entity, schema="chk", registry=reg):
        id: Serial
        qty_ordered: Int = Check("qty_ordered > 0", name=name)

    result = await sync(conn, reg)
    assert (
        result
        == f"""CREATE SCHEMA IF NOT EXISTS "chk";
CREATE SEQUENCE "chk"."CheckT_id_seq";
CREATE TABLE "chk"."CheckT" (
  "id" INT4 NOT NULL DEFAULT nextval('"chk"."CheckT_id_seq"'::regclass),
  "qty_ordered" INT4,
  PRIMARY KEY("id")
);
ALTER TABLE "chk"."CheckT"
  ADD CONSTRAINT "{const_name}" CHECK (qty_ordered > 0);
COMMENT ON CONSTRAINT "{const_name}" ON "chk"."CheckT" IS '[{{"field":"qty_ordered","hash":"6d995e6a85b1831b28db66f19fd4a679"}}]';"""
    )
    await conn.execute(result)
    assert not await sync(conn, reg)


async def test_new_multi_ddl_named(conn, pgclean):
    reg = Registry()

    class CheckT(Entity, schema="chk", registry=reg):
        id: Serial
        invoiced: Bool = Check("invoiced is true and refunded is false", name="chk-invoice")
        refunded: Bool = Check("refunded is true and invoiced is false", name="chk-invoice")

    result = await sync(conn, reg)
    assert (
        result
        == """CREATE SCHEMA IF NOT EXISTS "chk";
CREATE SEQUENCE "chk"."CheckT_id_seq";
CREATE TABLE "chk"."CheckT" (
  "id" INT4 NOT NULL DEFAULT nextval('"chk"."CheckT_id_seq"'::regclass),
  "invoiced" BOOLEAN,
  "refunded" BOOLEAN,
  PRIMARY KEY("id")
);
ALTER TABLE "chk"."CheckT"
  ADD CONSTRAINT "chk-invoice" CHECK ((invoiced is true and refunded is false) AND (refunded is true and invoiced is false));
COMMENT ON CONSTRAINT "chk-invoice" ON "chk"."CheckT" IS '[{"field":"invoiced","hash":"722985734e272bc1e92be9914c7e92e0"},{"field":"refunded","hash":"3720d0f085a0ce2c45a82a3b666f121d"}]';"""
    )
    await conn.execute(result)
    assert not await sync(conn, reg)


async def test_new_multi_ddl_unamed(conn, pgclean):
    reg = Registry()

    class CheckT(Entity, schema="chk", registry=reg):
        id: Serial
        invoiced: Bool = Check("invoiced is true and refunded is false")
        refunded: Bool = Check("refunded is true and invoiced is false")

    result = await sync(conn, reg)
    assert (
        result
        == """CREATE SCHEMA IF NOT EXISTS "chk";
CREATE SEQUENCE "chk"."CheckT_id_seq";
CREATE TABLE "chk"."CheckT" (
  "id" INT4 NOT NULL DEFAULT nextval('"chk"."CheckT_id_seq"'::regclass),
  "invoiced" BOOLEAN,
  "refunded" BOOLEAN,
  PRIMARY KEY("id")
);
ALTER TABLE "chk"."CheckT"
  ADD CONSTRAINT "chk_CheckT__invoiced" CHECK (invoiced is true and refunded is false),
  ADD CONSTRAINT "chk_CheckT__refunded" CHECK (refunded is true and invoiced is false);
COMMENT ON CONSTRAINT "chk_CheckT__invoiced" ON "chk"."CheckT" IS '[{"field":"invoiced","hash":"722985734e272bc1e92be9914c7e92e0"}]';
COMMENT ON CONSTRAINT "chk_CheckT__refunded" ON "chk"."CheckT" IS '[{"field":"refunded","hash":"3720d0f085a0ce2c45a82a3b666f121d"}]';"""
    )
    await conn.execute(result)
    assert not await sync(conn, reg)


@pytest.mark.parametrize("name", [None, "check-qty-ordred"])
async def test_update_single_ddl(conn, pgclean, name):
    reg = Registry()
    const_name = "chk_CheckT__qty_ordered" if not name else name

    class CheckT(Entity, schema="chk", registry=reg):
        id: Serial
        qty_ordered: Int = Check("qty_ordered > 0", name=name)

    result = await sync(conn, reg)
    assert (
        result
        == f"""CREATE SCHEMA IF NOT EXISTS "chk";
CREATE SEQUENCE "chk"."CheckT_id_seq";
CREATE TABLE "chk"."CheckT" (
  "id" INT4 NOT NULL DEFAULT nextval('"chk"."CheckT_id_seq"'::regclass),
  "qty_ordered" INT4,
  PRIMARY KEY("id")
);
ALTER TABLE "chk"."CheckT"
  ADD CONSTRAINT "{const_name}" CHECK (qty_ordered > 0);
COMMENT ON CONSTRAINT "{const_name}" ON "chk"."CheckT" IS '[{{"field":"qty_ordered","hash":"6d995e6a85b1831b28db66f19fd4a679"}}]';"""
    )
    await conn.execute(result)
    assert not await sync(conn, reg)

    # CHECK UPDATE
    reg2 = Registry()

    class CheckT(Entity, schema="chk", registry=reg2):
        id: Serial
        qty_ordered: Int = Check("qty_ordered > 1", name=name)

    result = await sync(conn, reg2)
    assert (
        result
        == f"""ALTER TABLE "chk"."CheckT"
  DROP CONSTRAINT IF EXISTS "{const_name}",
  ADD CONSTRAINT "{const_name}" CHECK (qty_ordered > 1);
COMMENT ON CONSTRAINT "{const_name}" ON "chk"."CheckT" IS '[{{"field":"qty_ordered","hash":"bc04c2924825bc3fca199e23746ab9c7"}}]';"""
    )
    await conn.execute(result)
    assert not await sync(conn, reg2)


async def test_update_multiple_named_ddl(conn, pgclean):
    reg = Registry()

    class CheckT(Entity, schema="chk", registry=reg):
        id: Serial
        invoiced: Bool = Check("invoiced is true and refunded is false", name="chk-invoiced")
        refunded: Bool = Check("refunded is true and invoiced is false", name="chk-invoiced")

    result = await sync(conn, reg)
    assert (
        result
        == """CREATE SCHEMA IF NOT EXISTS "chk";
CREATE SEQUENCE "chk"."CheckT_id_seq";
CREATE TABLE "chk"."CheckT" (
  "id" INT4 NOT NULL DEFAULT nextval('"chk"."CheckT_id_seq"'::regclass),
  "invoiced" BOOLEAN,
  "refunded" BOOLEAN,
  PRIMARY KEY("id")
);
ALTER TABLE "chk"."CheckT"
  ADD CONSTRAINT "chk-invoiced" CHECK ((invoiced is true and refunded is false) AND (refunded is true and invoiced is false));
COMMENT ON CONSTRAINT "chk-invoiced" ON "chk"."CheckT" IS '[{"field":"invoiced","hash":"722985734e272bc1e92be9914c7e92e0"},{"field":"refunded","hash":"3720d0f085a0ce2c45a82a3b666f121d"}]';"""
    )
    await conn.execute(result)
    assert not await sync(conn, reg)

    reg2 = Registry()

    class CheckT(Entity, schema="chk", registry=reg2):
        id: Serial
        invoiced: Bool = Check("invoiced is false and refunded is false", name="chk-invoiced")
        refunded: Bool = Check("refunded is true and invoiced is false", name="chk-invoiced")

    result = await sync(conn, reg2)
    assert (
        result
        == """ALTER TABLE "chk"."CheckT"
  DROP CONSTRAINT IF EXISTS "chk-invoiced",
  ADD CONSTRAINT "chk-invoiced" CHECK ((invoiced is false and refunded is false) AND (refunded is true and invoiced is false));
COMMENT ON CONSTRAINT "chk-invoiced" ON "chk"."CheckT" IS '[{"field":"invoiced","hash":"d039af7bec68056b3ae64faa38c62e00"},{"field":"refunded","hash":"3720d0f085a0ce2c45a82a3b666f121d"}]';"""
    )
    await conn.execute(result)
    assert not await sync(conn, reg2)


async def test_update_multiple_unnamed_ddl(conn, pgclean):
    reg = Registry()

    class CheckT(Entity, schema="chk", registry=reg):
        id: Serial
        invoiced: Bool = Check("invoiced is true and refunded is false")
        refunded: Bool = Check("refunded is true and invoiced is false")

    result = await sync(conn, reg)
    assert (
        result
        == """CREATE SCHEMA IF NOT EXISTS "chk";
CREATE SEQUENCE "chk"."CheckT_id_seq";
CREATE TABLE "chk"."CheckT" (
  "id" INT4 NOT NULL DEFAULT nextval('"chk"."CheckT_id_seq"'::regclass),
  "invoiced" BOOLEAN,
  "refunded" BOOLEAN,
  PRIMARY KEY("id")
);
ALTER TABLE "chk"."CheckT"
  ADD CONSTRAINT "chk_CheckT__invoiced" CHECK (invoiced is true and refunded is false),
  ADD CONSTRAINT "chk_CheckT__refunded" CHECK (refunded is true and invoiced is false);
COMMENT ON CONSTRAINT "chk_CheckT__invoiced" ON "chk"."CheckT" IS '[{"field":"invoiced","hash":"722985734e272bc1e92be9914c7e92e0"}]';
COMMENT ON CONSTRAINT "chk_CheckT__refunded" ON "chk"."CheckT" IS '[{"field":"refunded","hash":"3720d0f085a0ce2c45a82a3b666f121d"}]';"""
    )
    await conn.execute(result)
    assert not await sync(conn, reg)

    reg2 = Registry()

    class CheckT(Entity, schema="chk", registry=reg2):
        id: Serial
        invoiced: Bool = Check("invoiced is false and refunded is false")
        refunded: Bool = Check("refunded is true and invoiced is false")

    result = await sync(conn, reg2)
    assert (
        result
        == """ALTER TABLE "chk"."CheckT"
  DROP CONSTRAINT IF EXISTS "chk_CheckT__invoiced",
  ADD CONSTRAINT "chk_CheckT__invoiced" CHECK (invoiced is false and refunded is false);
COMMENT ON CONSTRAINT "chk_CheckT__invoiced" ON "chk"."CheckT" IS '[{"field":"invoiced","hash":"d039af7bec68056b3ae64faa38c62e00"}]';"""
    )
    await conn.execute(result)
    assert not await sync(conn, reg2)


async def test_update_name(conn, pgclean):
    reg = Registry()

    class CheckT(Entity, schema="chk", registry=reg):
        id: Serial
        qty_ordered: Int = Check("qty_ordered > 0", name="name1")

    result = await sync(conn, reg)
    assert (
        result
        == """CREATE SCHEMA IF NOT EXISTS "chk";
CREATE SEQUENCE "chk"."CheckT_id_seq";
CREATE TABLE "chk"."CheckT" (
  "id" INT4 NOT NULL DEFAULT nextval('"chk"."CheckT_id_seq"'::regclass),
  "qty_ordered" INT4,
  PRIMARY KEY("id")
);
ALTER TABLE "chk"."CheckT"
  ADD CONSTRAINT "name1" CHECK (qty_ordered > 0);
COMMENT ON CONSTRAINT "name1" ON "chk"."CheckT" IS '[{"field":"qty_ordered","hash":"6d995e6a85b1831b28db66f19fd4a679"}]';"""
    )
    await conn.execute(result)
    assert not await sync(conn, reg)

    reg2 = Registry()

    class CheckT(Entity, schema="chk", registry=reg2):
        id: Serial
        qty_ordered: Int = Check("qty_ordered > 0", name="name2")

    result = await sync(conn, reg2)
    assert (
        result
        == """ALTER TABLE "chk"."CheckT"
  DROP CONSTRAINT IF EXISTS "name1",
  ADD CONSTRAINT "name2" CHECK (qty_ordered > 0);
COMMENT ON CONSTRAINT "name2" ON "chk"."CheckT" IS '[{"field":"qty_ordered","hash":"6d995e6a85b1831b28db66f19fd4a679"}]';"""
    )
    await conn.execute(result)
    assert not await sync(conn, reg2)


async def test_clone():
    reg = Registry()

    class CheckT(Entity, schema="chk", registry=reg):
        id: Serial
        qty_ordered: Int = Check("qty_ordered > 0", name="name1")

    alias = CheckT.alias()
    original_check = CheckT.qty_ordered.get_ext(Check)
    aliased_check = alias.qty_ordered.get_ext(Check)
    assert isinstance(original_check, Check)
    assert isinstance(aliased_check, Check)
    assert original_check is not aliased_check
    assert original_check.expr == aliased_check.expr
    assert original_check.name == aliased_check.name
    assert original_check.props == aliased_check.props
