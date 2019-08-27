import operator
import pytest
from typing import Any

from yapic.entity.sql import wrap_connection, Entity, sync
from yapic.entity import (Field, Serial, Int, String, Bytes, Date, DateTime, DateTimeTz, Time, TimeTz, Bool, ForeignKey,
                          PrimaryKey, One, Query, func, EntityDiff, Registry, Json, JsonArray, Composite, Auto, Numeric,
                          Float, Point, UUID, virtual)

pytestmark = pytest.mark.asyncio

REGISTRY = Registry()


class Document(Entity, schema="circular_deps", registry=REGISTRY):
    id: Serial
    group_id: Auto = Field(nullable=False) // ForeignKey("DocumentGroup.id")


class DocumentGroup(Entity, schema="circular_deps", registry=REGISTRY):
    id: Serial
    primary_document_id: Int = Field(nullable=False)  # // ForeignKey(Document.id)
    primary_document: One[Document] = "DocumentGroup.primary_document_id == Document.id"


@pytest.yield_fixture
async def conn(pgsql):
    yield wrap_connection(pgsql, "pgsql")


async def test_sync(conn):
    await conn.conn.execute('DROP SCHEMA IF EXISTS "circular_deps" CASCADE')

    result = await sync(conn, REGISTRY)
    assert result == """CREATE SCHEMA IF NOT EXISTS "circular_deps";
CREATE SEQUENCE "circular_deps"."Document_id_seq";
CREATE SEQUENCE "circular_deps"."DocumentGroup_id_seq";
CREATE TABLE "circular_deps"."DocumentGroup" (
  "id" INT4 NOT NULL DEFAULT nextval('"circular_deps"."DocumentGroup_id_seq"'::regclass),
  "primary_document_id" INT NOT NULL,
  PRIMARY KEY("id")
);
CREATE TABLE "circular_deps"."Document" (
  "id" INT4 NOT NULL DEFAULT nextval('"circular_deps"."Document_id_seq"'::regclass),
  "group_id" INT4 NOT NULL,
  PRIMARY KEY("id"),
  CONSTRAINT "fk_Document__group_id-DocumentGroup__id" FOREIGN KEY ("group_id") REFERENCES "circular_deps"."DocumentGroup" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);"""
