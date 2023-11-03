# flake8: noqa: E501


import pytest
from yapic.entity import (
    Auto,
    Check,
    Entity,
    ForeignKey,
    Index,
    Int,
    Registry,
    Serial,
    Unique,
)
from yapic.entity.sql import sync

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("name", [None, "unique-qty-ordred"])
async def test_new_single_ddl(conn, pgclean, name):
    reg = Registry()
    const_name = "unique_UniqueT__qty_ordered" if not name else name

    class UniqueT(Entity, schema="uni", registry=reg):
        id: Serial
        indexed: Int = Index()
        qty_ordered: Int = Unique(name=name)

    result = await sync(conn, reg)
    assert (
        result
        == f"""CREATE SCHEMA IF NOT EXISTS "uni";
CREATE SEQUENCE "uni"."UniqueT_id_seq";
CREATE TABLE "uni"."UniqueT" (
  "id" INT4 NOT NULL DEFAULT nextval('"uni"."UniqueT_id_seq"'::regclass),
  "indexed" INT4,
  "qty_ordered" INT4,
  PRIMARY KEY("id")
);
CREATE INDEX "idx_UniqueT__indexed" ON "uni"."UniqueT" USING btree ("indexed");
ALTER TABLE "uni"."UniqueT"
  ADD CONSTRAINT "{const_name}" UNIQUE ("qty_ordered");"""
    )
    await conn.execute(result)
    assert not await sync(conn, reg)


async def test_new_multi_ddl_named(conn, pgclean):
    reg = Registry()

    class UniqueT(Entity, schema="uni", registry=reg):
        id: Serial
        value1: Int = Unique(name="unique-values")
        value2: Int = Unique(name="unique-values")

    result = await sync(conn, reg)
    assert (
        result
        == """CREATE SCHEMA IF NOT EXISTS "uni";
CREATE SEQUENCE "uni"."UniqueT_id_seq";
CREATE TABLE "uni"."UniqueT" (
  "id" INT4 NOT NULL DEFAULT nextval('"uni"."UniqueT_id_seq"'::regclass),
  "value1" INT4,
  "value2" INT4,
  PRIMARY KEY("id")
);
ALTER TABLE "uni"."UniqueT"
  ADD CONSTRAINT "unique-values" UNIQUE ("value1", "value2");"""
    )
    await conn.execute(result)
    assert not await sync(conn, reg)

async def test_with_fk(conn, pgclean):
    reg = Registry()

    class A(Entity, schema="uni", registry=reg):
        id: Serial

    class UniqueT(Entity, schema="uni", registry=reg):
        id: Serial
        value1: Int = Unique(name="unique-values")
        value2: Int = Unique(name="unique-values")
        ref: Auto = ForeignKey(A.id) // Unique() // Index() // Check("ref > 0")

    result = await sync(conn, reg)
    assert result == """CREATE SCHEMA IF NOT EXISTS "uni";
CREATE SEQUENCE "uni"."A_id_seq";
CREATE TABLE "uni"."A" (
  "id" INT4 NOT NULL DEFAULT nextval('"uni"."A_id_seq"'::regclass),
  PRIMARY KEY("id")
);
CREATE SEQUENCE "uni"."UniqueT_id_seq";
CREATE TABLE "uni"."UniqueT" (
  "id" INT4 NOT NULL DEFAULT nextval('"uni"."UniqueT_id_seq"'::regclass),
  "value1" INT4,
  "value2" INT4,
  "ref" INT4,
  PRIMARY KEY("id")
);
CREATE INDEX "idx_UniqueT__ref" ON "uni"."UniqueT" USING btree ("ref");
ALTER TABLE "uni"."UniqueT"
  ADD CONSTRAINT "fk_UniqueT__ref-A__id" FOREIGN KEY ("ref") REFERENCES "uni"."A" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  ADD CONSTRAINT "unique-values" UNIQUE ("value1", "value2"),
  ADD CONSTRAINT "unique_UniqueT__ref" UNIQUE ("ref"),
  ADD CONSTRAINT "chk_UniqueT__ref" CHECK (ref > 0);
COMMENT ON CONSTRAINT "chk_UniqueT__ref" ON "uni"."UniqueT" IS '[{"field":"ref","hash":"911c0322184a3e7807c93bffbd591054"}]';"""
    await conn.execute(result)
    assert not await sync(conn, reg)
