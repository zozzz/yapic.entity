# flake8: noqa: E501

import pytest
from datetime import datetime, date, time, tzinfo, timedelta
from decimal import Decimal
from yapic.entity.sql import sync, PostgreTrigger
from yapic.entity import (Entity, Field, Serial, Int, String, Bytes, Date, DateTime, DateTimeTz, Time, TimeTz, Bool,
                          ForeignKey, PrimaryKey, One, Query, func, EntityDiff, Registry, Json, JsonArray, Composite,
                          Auto, Numeric, Float, Point, UUID, virtual)

pytestmark = pytest.mark.asyncio
REGISTRY = Registry()


class TriggerTable(Entity, schema="_trigger", registry=REGISTRY):
    id: Serial
    updated_time: DateTimeTz


TriggerTable.__triggers__.append(
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


async def test_sync(conn, pgclean):
    result = await sync(conn, REGISTRY)
    assert result == """CREATE SCHEMA IF NOT EXISTS "_trigger";
CREATE SEQUENCE "_trigger"."TriggerTable_id_seq";
CREATE TABLE "_trigger"."TriggerTable" (
  "id" INT4 NOT NULL DEFAULT nextval('"_trigger"."TriggerTable_id_seq"'::regclass),
  "updated_time" TIMESTAMPTZ,
  PRIMARY KEY("id")
);
CREATE OR REPLACE FUNCTION "_trigger"."YT-TriggerTable-update_time-8085b1-af0df2"() RETURNS TRIGGER AS $$ BEGIN
  NEW.updated_time = NOW();
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "update_time"
  BEFORE UPDATE ON "_trigger"."TriggerTable"
  FOR EACH ROW
  WHEN (OLD.* IS DISTINCT FROM NEW.*)
  EXECUTE FUNCTION "_trigger"."YT-TriggerTable-update_time-8085b1-af0df2"();"""

    await conn.execute(result)

    result = await sync(conn, REGISTRY)
    assert not result

    # CHANGE: before -> after
    TriggerTable.__triggers__[0] = PostgreTrigger(
        name="update_time",
        after="UPDATE",
        for_each="ROW",
        when="OLD.* IS DISTINCT FROM NEW.*",
        body="""
            NEW.updated_time = NOW();
            RETURN NEW;
        """,
    )

    result = await sync(conn, REGISTRY)
    assert result == """DROP TRIGGER IF EXISTS "update_time" ON "_trigger"."TriggerTable";
DROP FUNCTION IF EXISTS "_trigger"."YT-TriggerTable-update_time-8085b1-af0df2";
CREATE OR REPLACE FUNCTION "_trigger"."YT-TriggerTable-update_time-8085b1-af0df2"() RETURNS TRIGGER AS $$ BEGIN
  NEW.updated_time = NOW();
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "update_time"
  AFTER UPDATE ON "_trigger"."TriggerTable"
  FOR EACH ROW
  WHEN (OLD.* IS DISTINCT FROM NEW.*)
  EXECUTE FUNCTION "_trigger"."YT-TriggerTable-update_time-8085b1-af0df2"();"""

    await conn.execute(result)

    # CHANGE: when to None
    TriggerTable.__triggers__[0] = PostgreTrigger(
        name="update_time",
        after="UPDATE",
        for_each="ROW",
        body="""
            NEW.updated_time = NOW();
            RETURN NEW;
        """,
    )

    result = await sync(conn, REGISTRY)
    assert result == """DROP TRIGGER IF EXISTS "update_time" ON "_trigger"."TriggerTable";
DROP FUNCTION IF EXISTS "_trigger"."YT-TriggerTable-update_time-8085b1-af0df2";
CREATE OR REPLACE FUNCTION "_trigger"."YT-TriggerTable-update_time-af0df2"() RETURNS TRIGGER AS $$ BEGIN
  NEW.updated_time = NOW();
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "update_time"
  AFTER UPDATE ON "_trigger"."TriggerTable"
  FOR EACH ROW
  EXECUTE FUNCTION "_trigger"."YT-TriggerTable-update_time-af0df2"();"""

    await conn.execute(result)

    # CHANGE: when to original
    # CHANGE body
    TriggerTable.__triggers__[0] = PostgreTrigger(
        name="update_time",
        after="UPDATE",
        for_each="ROW",
        when="OLD.* IS DISTINCT FROM NEW.*",
        body="""
            NEW.updated_time = NOW();
            RETURN OLD;
        """,
    )

    result = await sync(conn, REGISTRY)
    assert result == """DROP TRIGGER IF EXISTS "update_time" ON "_trigger"."TriggerTable";
DROP FUNCTION IF EXISTS "_trigger"."YT-TriggerTable-update_time-af0df2";
CREATE OR REPLACE FUNCTION "_trigger"."YT-TriggerTable-update_time-8085b1-45cbab"() RETURNS TRIGGER AS $$ BEGIN
  NEW.updated_time = NOW();
  RETURN OLD;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "update_time"
  AFTER UPDATE ON "_trigger"."TriggerTable"
  FOR EACH ROW
  WHEN (OLD.* IS DISTINCT FROM NEW.*)
  EXECUTE FUNCTION "_trigger"."YT-TriggerTable-update_time-8085b1-45cbab"();"""

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
    assert result == """CREATE OR REPLACE FUNCTION "_trigger"."YT-TriggerX-update_time-8085b1-af0df2"() RETURNS TRIGGER AS $$ BEGIN
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
