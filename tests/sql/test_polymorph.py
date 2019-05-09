import pytest
from yapic.entity.sql import wrap_connection, Entity, sync
from yapic.entity import (Serial, Int, String, ForeignKey, PrimaryKey, One, Many, ManyAcross, Registry, DependencyList,
                          Json, Composite, save_operations, Auto, Query)

pytestmark = pytest.mark.asyncio  # type: ignore


@pytest.yield_fixture  # type: ignore
async def conn(pgsql):
    yield wrap_connection(pgsql, "pgsql")


_registry = Registry()


class Employee(Entity, schema="poly", polymorph="variant"):
    id: Serial
    variant: String
    employee_field: String


class Manager(Employee, polymorph_id="manager"):
    manager_field: String


class Worker(Employee, polymorph_id="worker"):
    worker_field: String


async def test_begin(conn):
    await conn.conn.execute("""DROP SCHEMA IF EXISTS "poly" CASCADE""")


async def test_sync(conn):
    result = await sync(conn, Worker.__registry__)
    assert result == """CREATE SCHEMA IF NOT EXISTS "poly";
CREATE SEQUENCE "poly"."Employee_id_seq";
CREATE TABLE "poly"."Employee" (
  "id" INT4 NOT NULL DEFAULT nextval('"poly"."Employee_id_seq"'::regclass),
  "variant" TEXT,
  "employee_field" TEXT,
  PRIMARY KEY("id")
);
CREATE TABLE "poly"."Manager" (
  "id" INT4 NOT NULL,
  "manager_field" TEXT,
  PRIMARY KEY("id"),
  CONSTRAINT "fk_Manager__id-Employee__id" FOREIGN KEY ("id") REFERENCES "poly"."Employee" ("id") ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE "poly"."Worker" (
  "id" INT4 NOT NULL,
  "worker_field" TEXT,
  PRIMARY KEY("id"),
  CONSTRAINT "fk_Worker__id-Employee__id" FOREIGN KEY ("id") REFERENCES "poly"."Employee" ("id") ON UPDATE CASCADE ON DELETE CASCADE
);"""

    await conn.conn.execute(result)


async def test_query_from_worker(conn):
    q = Query().select_from(Worker).where(Worker.employee_field == "Nice")
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t1"."variant", "t1"."employee_field", "t0"."worker_field" FROM "poly"."Worker" "t0" INNER JOIN "poly"."Employee" "t1" ON "t0"."id" = "t1"."id" WHERE "t1"."employee_field" = $1"""
    assert params == ("Nice", )


async def test_query_from_employee(conn):
    q = Query().select_from(Employee) \
        .where(Employee.employee_field == "Nice") \
        .where(Worker.worker_field == "WF")
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert sql == ""
    assert params == ("Nice", "WF")


# async def test_end(conn):
#     await conn.conn.execute("""DROP SCHEMA IF EXISTS "poly" CASCADE""")
