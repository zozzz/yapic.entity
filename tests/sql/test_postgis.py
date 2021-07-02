# flake8: noqa: E501

import pytest
from datetime import datetime, date, time, tzinfo, timedelta
from decimal import Decimal
from yapic.entity.sql import Entity, sync, PostgreDialect
from yapic.entity import (Field, Serial, Int, String, Bytes, Date, DateTime, DateTimeTz, Time, TimeTz, Bool, ForeignKey,
                          PrimaryKey, One, Query, func, EntityDiff, Registry, Json, JsonArray, Composite, Auto, Numeric,
                          Float, Point, UUID, virtual)
from yapic.entity.sql.pgsql import postgis

pytestmark = pytest.mark.asyncio
dialect = PostgreDialect()
ddl = dialect.create_ddl_compiler()
REGISTRY = Registry()


class Point(Entity, schema="postgis", registry=REGISTRY):
    id: Serial
    location: postgis.Point


class LatLng(Entity, schema="postgis", registry=REGISTRY):
    id: Serial
    location: postgis.LatLng


async def test_sync(conn, pgclean):
    result = await sync(conn, Point.__registry__)
    assert result == """CREATE SCHEMA IF NOT EXISTS "postgis";
CREATE SEQUENCE "postgis"."LatLng_id_seq";
CREATE TABLE "postgis"."LatLng" (
  "id" INT4 NOT NULL DEFAULT nextval('"postgis"."LatLng_id_seq"'::regclass),
  "location" geography(POINT, 4326),
  PRIMARY KEY("id")
);
CREATE SEQUENCE "postgis"."Point_id_seq";
CREATE TABLE "postgis"."Point" (
  "id" INT4 NOT NULL DEFAULT nextval('"postgis"."Point_id_seq"'::regclass),
  "location" geometry(POINT, 4326),
  PRIMARY KEY("id")
);"""

    await conn.execute(result)

    result = await sync(conn, Point.__registry__)
    assert result is None


async def test_point_isu(conn):
    p = Point(location=[19.0424536, 47.5135873])
    assert p.location.x == 19.0424536
    assert p.location.y == 47.5135873
    assert await conn.insert(p) is True

    q = Query().select_from(Point).where(Point.id == p.id)
    res = await conn.select(q).first()
    assert res.location.x == 19.0424536
    assert res.location.y == 47.5135873

    res.location.x = 19.0433738
    res.location.y = 47.5187817
    await conn.save(res) is True
    q = Query().select_from(Point).where(Point.id == p.id)
    res = await conn.select(q).first()
    assert res.location.x == 19.0433738
    assert res.location.y == 47.5187817

    p2 = Point(location={"x": 19.0424536, "y": 47.5135873})
    assert p2.location.x == 19.0424536
    assert p2.location.y == 47.5135873
    assert await conn.insert(p2) is True

    q = Query().select_from(Point).where(Point.id == p2.id)
    res = await conn.select(q).first()
    assert res.location.x == 19.0424536
    assert res.location.y == 47.5135873


async def test_latlng_isu(conn):
    p = LatLng(location=[47.5135873, 19.0424536])
    assert p.location.lat == 47.5135873
    assert p.location.lng == 19.0424536
    assert await conn.insert(p) is True

    q = Query().select_from(LatLng).where(LatLng.id == p.id)
    res = await conn.select(q).first()
    assert res.location.lat == 47.5135873
    assert res.location.lng == 19.0424536

    res.location.lat = 47.5187817
    res.location.lng = 19.0433738
    await conn.insert_or_update(res) is True
    q = Query().select_from(LatLng).where(LatLng.id == p.id)
    res = await conn.select(q).first()
    assert res.location.lat == 47.5187817
    assert res.location.lng == 19.0433738

    p2 = LatLng(location={"lat": 47.5135873, "lng": 19.0424536})
    assert p2.location.lat == 47.5135873
    assert p2.location.lng == 19.0424536
    assert await conn.insert(p2) is True

    q = Query().select_from(LatLng).where(LatLng.id == p2.id)
    res = await conn.select(q).first()
    assert res.location.lat == 47.5135873
    assert res.location.lng == 19.0424536
