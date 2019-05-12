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


class WorkerX(Worker, polymorph_id="workerx"):
    workerx_field: String


class WorkerY(Worker, polymorph_id="workery"):
    workery_field: String


class Organization(Entity, schema="poly"):
    id: Serial
    employee_id: Auto = ForeignKey(Employee.id)
    employee: One[Employee]


async def test_begin(conn):
    await conn.conn.execute("""DROP SCHEMA IF EXISTS "poly" CASCADE""")


async def test_sync(conn, pgclean):
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
  CONSTRAINT "polymorph_fkey" FOREIGN KEY ("id") REFERENCES "poly"."Employee" ("id") ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE SEQUENCE "poly"."Organization_id_seq";
CREATE TABLE "poly"."Organization" (
  "id" INT4 NOT NULL DEFAULT nextval('"poly"."Organization_id_seq"'::regclass),
  "employee_id" INT4,
  PRIMARY KEY("id"),
  CONSTRAINT "fk_Organization__employee_id-Employee__id" FOREIGN KEY ("employee_id") REFERENCES "poly"."Employee" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE TABLE "poly"."Worker" (
  "id" INT4 NOT NULL,
  "worker_field" TEXT,
  PRIMARY KEY("id"),
  CONSTRAINT "polymorph_fkey" FOREIGN KEY ("id") REFERENCES "poly"."Employee" ("id") ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE "poly"."WorkerX" (
  "id" INT4 NOT NULL,
  "workerx_field" TEXT,
  PRIMARY KEY("id"),
  CONSTRAINT "polymorph_fkey" FOREIGN KEY ("id") REFERENCES "poly"."Worker" ("id") ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE "poly"."WorkerY" (
  "id" INT4 NOT NULL,
  "workery_field" TEXT,
  PRIMARY KEY("id"),
  CONSTRAINT "polymorph_fkey" FOREIGN KEY ("id") REFERENCES "poly"."Worker" ("id") ON UPDATE CASCADE ON DELETE CASCADE
);"""

    await conn.conn.execute(result)


async def test_query_from_worker(conn):
    q = Query().select_from(Worker).where(Worker.employee_field == "Nice")
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t1"."id", "t1"."variant", "t1"."employee_field", "t0"."worker_field", "t2"."workerx_field", "t3"."workery_field" FROM "poly"."Worker" "t0" INNER JOIN "poly"."Employee" "t1" ON "t0"."id" = "t1"."id" LEFT JOIN "poly"."WorkerX" "t2" ON "t2"."id" = "t0"."id" LEFT JOIN "poly"."WorkerY" "t3" ON "t3"."id" = "t0"."id" WHERE "t1"."employee_field" = $1"""
    assert params == ("Nice", )


async def test_query_from_employee(conn):
    q = Query().select_from(Employee) \
        .where(Employee.employee_field == "Nice") \
        .where(Worker.worker_field == "WF")
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."variant", "t0"."employee_field", "t2"."manager_field", "t1"."worker_field", "t3"."workerx_field", "t4"."workery_field" FROM "poly"."Employee" "t0" INNER JOIN "poly"."Worker" "t1" ON "t1"."id" = "t0"."id" LEFT JOIN "poly"."Manager" "t2" ON "t2"."id" = "t0"."id" LEFT JOIN "poly"."WorkerX" "t3" ON "t3"."id" = "t1"."id" LEFT JOIN "poly"."WorkerY" "t4" ON "t4"."id" = "t1"."id" WHERE "t0"."employee_field" = $1 AND "t1"."worker_field" = $2"""
    assert params == ("Nice", "WF")


async def test_query_org(conn):
    q = Query().select_from(Organization) \
        .where(Employee.employee_field == "Nice") \
        .where(Worker.worker_field == "WF")
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."employee_id" FROM "poly"."Organization" "t0" INNER JOIN "poly"."Employee" "t1" ON "t0"."employee_id" = "t1"."id" INNER JOIN "poly"."Worker" "t2" ON "t2"."id" = "t1"."id" WHERE "t1"."employee_field" = $1 AND "t2"."worker_field" = $2"""
    assert params == ("Nice", "WF")


# async def test_end(conn):
#     await conn.conn.execute("""DROP SCHEMA IF EXISTS "poly" CASCADE""")
