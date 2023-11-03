# flake8: noqa: E501

import pytest
from yapic.entity import DateTimeTz, Entity, Registry, Serial
from yapic.entity.sql import PostgreTrigger, sync

pytestmark = pytest.mark.asyncio
REGISTRY = Registry()


class TriggerTable(Entity, schema="_trigger", registry=REGISTRY):
    id: Serial
    updated_time: DateTimeTz


original_trigger = PostgreTrigger(
    name="update_time",
    before="UPDATE",
    for_each="ROW",
    when="OLD.* IS DISTINCT FROM NEW.*",
    body="""
        NEW.updated_time = NOW();
        RETURN NEW;
    """,
)

TriggerTable.__triggers__ = [original_trigger]


async def test_sync(conn, pgclean):
    result = await sync(conn, REGISTRY)
    assert result == """CREATE SCHEMA IF NOT EXISTS "_trigger";
CREATE SEQUENCE "_trigger"."TriggerTable_id_seq";
CREATE TABLE "_trigger"."TriggerTable" (
  "id" INT4 NOT NULL DEFAULT nextval('"_trigger"."TriggerTable_id_seq"'::regclass),
  "updated_time" TIMESTAMPTZ,
  PRIMARY KEY("id")
);
CREATE OR REPLACE FUNCTION "_trigger"."YT-TriggerTable-update_time-8085b1-18ebfe"() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_time = NOW();
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "update_time"
  BEFORE UPDATE ON "_trigger"."TriggerTable"
  FOR EACH ROW
  WHEN (OLD.* IS DISTINCT FROM NEW.*)
  EXECUTE FUNCTION "_trigger"."YT-TriggerTable-update_time-8085b1-18ebfe"();"""

    await conn.execute(result)

    result = await sync(conn, REGISTRY)
    assert not result

    # CHANGE: before -> after
    TriggerTable.__triggers__[0] = PostgreTrigger(
        name="update_time",
        after="UPDATE",
        for_each="ROW",
        when='OLD.* IS DISTINCT FROM NEW.* AND (NEW."updated_time" IS NULL OR OLD."updated_time" = NEW."updated_time")',
        body="""
            NEW.updated_time = NOW();
            RETURN NEW;
        """,
    )

    result = await sync(conn, REGISTRY)
    assert result == """DROP TRIGGER IF EXISTS "update_time" ON "_trigger"."TriggerTable";
DROP FUNCTION IF EXISTS "_trigger"."YT-TriggerTable-update_time-8085b1-18ebfe";
CREATE OR REPLACE FUNCTION "_trigger"."YT-TriggerTable-update_time-386fb5-af0df2"() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_time = NOW();
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "update_time"
  AFTER UPDATE ON "_trigger"."TriggerTable"
  FOR EACH ROW
  WHEN (OLD.* IS DISTINCT FROM NEW.* AND (NEW."updated_time" IS NULL OR OLD."updated_time" = NEW."updated_time"))
  EXECUTE FUNCTION "_trigger"."YT-TriggerTable-update_time-386fb5-af0df2"();"""

    await conn.execute(result)

    # CHANGE: remove all triggers
    TriggerTable.__triggers__ = []

    result = await sync(conn, REGISTRY)
    assert result == """DROP TRIGGER IF EXISTS "update_time" ON "_trigger"."TriggerTable";
DROP FUNCTION IF EXISTS "_trigger"."YT-TriggerTable-update_time-386fb5-af0df2";"""

    await conn.execute(result)

    # CHANGE: when to original
    # CHANGE body
    TriggerTable.__triggers__ = [original_trigger]

    result = await sync(conn, REGISTRY)
    assert result == """CREATE OR REPLACE FUNCTION "_trigger"."YT-TriggerTable-update_time-8085b1-18ebfe"() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_time = NOW();
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "update_time"
  BEFORE UPDATE ON "_trigger"."TriggerTable"
  FOR EACH ROW
  WHEN (OLD.* IS DISTINCT FROM NEW.*)
  EXECUTE FUNCTION "_trigger"."YT-TriggerTable-update_time-8085b1-18ebfe"();"""

    await conn.execute(result)


async def test_create_later(conn, pgclean):
    r = Registry()

    class TriggerX(Entity, schema="_trigger", registry=r):
        id: Serial

    result = await sync(conn, r)
    assert result == """CREATE SCHEMA IF NOT EXISTS "_trigger";
CREATE SEQUENCE "_trigger"."TriggerX_id_seq";
CREATE TABLE "_trigger"."TriggerX" (
  "id" INT4 NOT NULL DEFAULT nextval('"_trigger"."TriggerX_id_seq"'::regclass),
  PRIMARY KEY("id")
);"""
    await conn.execute(result)

    TriggerX.__triggers__.append(
        PostgreTrigger(
            name="update_time",
            before="UPDATE",
            for_each="ROW",
            when="OLD.* IS DISTINCT FROM NEW.*",
            body="""
            NEW.updated_time = NOW();
            RETURN NEW;
        """,
        ))

    result = await sync(conn, r)
    assert result == """CREATE OR REPLACE FUNCTION "_trigger"."YT-TriggerX-update_time-8085b1-af0df2"() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_time = NOW();
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "update_time"
  BEFORE UPDATE ON "_trigger"."TriggerX"
  FOR EACH ROW
  WHEN (OLD.* IS DISTINCT FROM NEW.*)
  EXECUTE FUNCTION "_trigger"."YT-TriggerX-update_time-8085b1-af0df2"();"""

    await conn.execute(result)

    result = await sync(conn, r)
    assert not result
