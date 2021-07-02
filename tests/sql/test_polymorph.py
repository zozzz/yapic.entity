import pytest
from yapic.entity.sql import sync
from yapic.entity import (Entity, Serial, Int, String, ForeignKey, PrimaryKey, One, Many, ManyAcross, Registry,
                          DependencyList, Json, Composite, save_operations, Auto, Query)

pytestmark = pytest.mark.asyncio  # type: ignore
REGISTRY = Registry()

_registry = Registry()


class Employee(Entity, schema="poly", polymorph="variant", registry=REGISTRY):
    id: Serial
    variant: String
    employee_field: String


class Manager(Employee, polymorph_id="manager", registry=REGISTRY):
    manager_field: String


class Worker(Employee, polymorph_id="worker", registry=REGISTRY):
    worker_field: String


class WorkerX(Worker, polymorph_id="workerx", registry=REGISTRY):
    workerx_field: String


class WorkerY(Worker, polymorph_id="workery", registry=REGISTRY):
    workery_field: String


class Organization(Entity, schema="poly", registry=REGISTRY):
    id: Serial
    employee_id: Auto = ForeignKey(Employee.id)
    employee: One[Employee]


async def test_sync(conn, pgclean):
    result = await sync(conn, REGISTRY)
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
  PRIMARY KEY("id")
);
CREATE SEQUENCE "poly"."Organization_id_seq";
CREATE TABLE "poly"."Organization" (
  "id" INT4 NOT NULL DEFAULT nextval('"poly"."Organization_id_seq"'::regclass),
  "employee_id" INT4,
  PRIMARY KEY("id")
);
CREATE TABLE "poly"."Worker" (
  "id" INT4 NOT NULL,
  "worker_field" TEXT,
  PRIMARY KEY("id")
);
CREATE TABLE "poly"."WorkerX" (
  "id" INT4 NOT NULL,
  "workerx_field" TEXT,
  PRIMARY KEY("id")
);
CREATE TABLE "poly"."WorkerY" (
  "id" INT4 NOT NULL,
  "workery_field" TEXT,
  PRIMARY KEY("id")
);
ALTER TABLE "poly"."Manager"
  ADD CONSTRAINT "fk_Manager__id-Employee__id" FOREIGN KEY ("id") REFERENCES "poly"."Employee" ("id") ON UPDATE CASCADE ON DELETE CASCADE;
CREATE OR REPLACE FUNCTION "poly"."YT-Manager-polyd_Employee"() RETURNS TRIGGER AS $$ BEGIN
  DELETE FROM "poly"."Employee" "parent" WHERE "parent"."id"=OLD."id";
  RETURN OLD;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "polyd_Employee"
  AFTER DELETE ON "poly"."Manager"
  FOR EACH ROW
  EXECUTE FUNCTION "poly"."YT-Manager-polyd_Employee"();
CREATE INDEX "idx_Organization__employee_id" ON "poly"."Organization" USING btree ("employee_id");
ALTER TABLE "poly"."Organization"
  ADD CONSTRAINT "fk_Organization__employee_id-Employee__id" FOREIGN KEY ("employee_id") REFERENCES "poly"."Employee" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE "poly"."Worker"
  ADD CONSTRAINT "fk_Worker__id-Employee__id" FOREIGN KEY ("id") REFERENCES "poly"."Employee" ("id") ON UPDATE CASCADE ON DELETE CASCADE;
CREATE OR REPLACE FUNCTION "poly"."YT-Worker-polyd_Employee"() RETURNS TRIGGER AS $$ BEGIN
  DELETE FROM "poly"."Employee" "parent" WHERE "parent"."id"=OLD."id";
  RETURN OLD;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "polyd_Employee"
  AFTER DELETE ON "poly"."Worker"
  FOR EACH ROW
  EXECUTE FUNCTION "poly"."YT-Worker-polyd_Employee"();
ALTER TABLE "poly"."WorkerX"
  ADD CONSTRAINT "fk_WorkerX__id-Worker__id" FOREIGN KEY ("id") REFERENCES "poly"."Worker" ("id") ON UPDATE CASCADE ON DELETE CASCADE;
CREATE OR REPLACE FUNCTION "poly"."YT-WorkerX-polyd_Worker"() RETURNS TRIGGER AS $$ BEGIN
  DELETE FROM "poly"."Worker" "parent" WHERE "parent"."id"=OLD."id";
  RETURN OLD;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "polyd_Worker"
  AFTER DELETE ON "poly"."WorkerX"
  FOR EACH ROW
  EXECUTE FUNCTION "poly"."YT-WorkerX-polyd_Worker"();
ALTER TABLE "poly"."WorkerY"
  ADD CONSTRAINT "fk_WorkerY__id-Worker__id" FOREIGN KEY ("id") REFERENCES "poly"."Worker" ("id") ON UPDATE CASCADE ON DELETE CASCADE;
CREATE OR REPLACE FUNCTION "poly"."YT-WorkerY-polyd_Worker"() RETURNS TRIGGER AS $$ BEGIN
  DELETE FROM "poly"."Worker" "parent" WHERE "parent"."id"=OLD."id";
  RETURN OLD;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "polyd_Worker"
  AFTER DELETE ON "poly"."WorkerY"
  FOR EACH ROW
  EXECUTE FUNCTION "poly"."YT-WorkerY-polyd_Worker"();"""

    await conn.execute(result)

    result = await sync(conn, Worker.__registry__)
    assert bool(result) is False


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
    assert sql == """SELECT "t0"."id", "t0"."variant", "t0"."employee_field", "t1"."manager_field", "t2"."worker_field", "t3"."workerx_field", "t4"."workery_field" FROM "poly"."Employee" "t0" LEFT JOIN "poly"."Manager" "t1" ON "t1"."id" = "t0"."id" LEFT JOIN "poly"."Worker" "t2" ON "t2"."id" = "t0"."id" LEFT JOIN "poly"."WorkerX" "t3" ON "t3"."id" = "t2"."id" LEFT JOIN "poly"."WorkerY" "t4" ON "t4"."id" = "t2"."id" WHERE "t0"."employee_field" = $1 AND "t2"."worker_field" = $2"""
    assert params == ("Nice", "WF")


async def test_query_org(conn):
    q = Query().select_from(Organization) \
        .where(Employee.employee_field == "Nice") \
        .where(Worker.worker_field == "WF")
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", "t0"."employee_id" FROM "poly"."Organization" "t0" INNER JOIN "poly"."Employee" "t1" ON "t0"."employee_id" = "t1"."id" INNER JOIN "poly"."Worker" "t2" ON "t2"."id" = "t1"."id" WHERE "t1"."employee_field" = $1 AND "t2"."worker_field" = $2"""
    assert params == ("Nice", "WF")


async def test_query_from_workerx(conn):
    q = Query().select_from(WorkerX).where(Employee.employee_field == "OK")
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t2"."id", "t2"."variant", "t2"."employee_field", "t1"."worker_field", "t0"."workerx_field" FROM "poly"."WorkerX" "t0" INNER JOIN "poly"."Worker" "t1" ON "t0"."id" = "t1"."id" INNER JOIN "poly"."Employee" "t2" ON "t1"."id" = "t2"."id" WHERE "t2"."employee_field" = $1"""
    assert params == ("OK", )


async def test_load_one(conn):
    q = Query().select_from(Organization).load(Organization.employee).where(Organization.id == 1)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT (SELECT ROW("t2"."id", "t2"."variant", "t2"."employee_field", "t3"."manager_field", "t4"."worker_field", "t5"."workerx_field", "t6"."workery_field") FROM "poly"."Employee" "t2" LEFT JOIN "poly"."Manager" "t3" ON "t3"."id" = "t2"."id" LEFT JOIN "poly"."Worker" "t4" ON "t4"."id" = "t2"."id" LEFT JOIN "poly"."WorkerX" "t5" ON "t5"."id" = "t4"."id" LEFT JOIN "poly"."WorkerY" "t6" ON "t6"."id" = "t4"."id" WHERE "t1"."employee_id" = "t2"."id") as "t0" FROM "poly"."Organization" "t1" WHERE "t1"."id" = $1"""
    assert params == (1, )


async def test_insert_worker(conn):
    worker = Worker()
    worker.employee_field = "ef"
    worker.worker_field = "wf"
    await conn.save(worker)

    w = await conn.select(Query().select_from(Worker).where(Worker.id == worker.id)).first()
    assert isinstance(w, Worker)
    assert w.id == worker.id
    assert w.variant == "worker"
    assert w.employee_field == "ef"
    assert w.worker_field == "wf"


async def test_insert_workerx(conn):
    worker = WorkerX()
    worker.employee_field = "employee_field: set from workerx"
    worker.worker_field = "worker_field: set from workerx"
    worker.workerx_field = "workerx_field: set from workerx"
    await conn.save(worker)

    org = Organization()
    org.employee_id = worker.id
    await conn.save(org)

    def test_worker_x_fields(w):
        assert isinstance(w, WorkerX)
        assert w.id == worker.id
        assert w.variant == "workerx"
        assert w.employee_field == "employee_field: set from workerx"
        assert w.worker_field == "worker_field: set from workerx"
        assert w.workerx_field == "workerx_field: set from workerx"

    w = await conn.select(Query().select_from(WorkerX).where(WorkerX.id == worker.id)).first()
    test_worker_x_fields(w)

    w = await conn.select(Query().select_from(Employee).where(WorkerX.id == worker.id)).first()
    test_worker_x_fields(w)

    o = await conn.select(Query().select_from(Organization).load(
        Organization.employee).where(Organization.id == org.id)).first()
    test_worker_x_fields(o.employee)


async def test_insert_with_specified_id(conn):
    id = 12345
    worker = WorkerX()
    worker.id = id
    worker.employee_field = "employee_field: set from workerx"
    worker.worker_field = "worker_field: set from workerx"
    worker.workerx_field = "workerx_field: set from workerx"
    await conn.save(worker)

    def test_worker_x_fields(w):
        assert isinstance(w, WorkerX)
        assert w.id == id
        assert w.variant == "workerx"
        assert w.employee_field == "employee_field: set from workerx"
        assert w.worker_field == "worker_field: set from workerx"
        assert w.workerx_field == "workerx_field: set from workerx"

    w = await conn.select(Query().select_from(WorkerX).where(WorkerX.id == id)).first()
    test_worker_x_fields(w)


async def test_delete(conn):
    id = 12345
    worker = WorkerX()
    worker.id = id
    worker.employee_field = "employee_field: set from workerx"
    worker.worker_field = "worker_field: set from workerx"
    worker.workerx_field = "workerx_field: set from workerx"
    await conn.save(worker)

    w = await conn.select(Query().select_from(Worker).where(Worker.id == id)).first()
    assert w.id == id

    w = await conn.select(Query().select_from(Employee).where(Employee.id == id)).first()
    assert w.id == id

    w = await conn.select(Query().select_from(WorkerX).where(WorkerX.id == id)).first()
    assert w.id == id

    await conn.delete(w)

    w = await conn.select(Query().select_from(Worker).where(Worker.id == id)).first()
    assert not w

    w = await conn.select(Query().select_from(Employee).where(Employee.id == id)).first()
    assert not w

    w = await conn.select(Query().select_from(WorkerX).where(WorkerX.id == id)).first()
    assert not w


async def test_ambiguous_load(conn, pgclean):
    registry = Registry()

    class PolyBase(Entity, schema="poly", registry=registry, polymorph="type"):
        id: Serial
        type: String
        base_field: String

    class PolyChild(PolyBase, polymorph_id="child"):
        child_field: String

    class Something(Entity, schema="poly", registry=registry):
        id: Serial
        pid1: Auto = ForeignKey(PolyChild.id)
        p1: One[PolyChild] = "Something.pid1 == PolyChild.id"
        pid2: Auto = ForeignKey(PolyChild.id)
        p2: One[PolyChild] = "Something.pid2 == PolyChild.id"

    result = await sync(conn, registry)
    # print(result)
    await conn.execute(result)

    p1 = PolyChild()
    p1.base_field = "base field 1"
    p1.child_field = "child field 1"
    p2 = PolyChild()
    p2.base_field = "base field 2"
    p2.child_field = "child field 2"
    await conn.save(p1)
    await conn.save(p2)

    s = Something(pid1=p1.id, pid2=p2.id)
    await conn.save(s)

    q = Query().select_from(Something).load(Something, Something.p1, Something.p2).where(Something.id == s.id)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    # print(sql)

    s = await conn.select(q).first()
    assert s.p1.id == p1.id
    assert s.p1.base_field == "base field 1"
    assert s.p1.child_field == "child field 1"

    assert s.p2.id == p2.id
    assert s.p2.base_field == "base field 2"
    assert s.p2.child_field == "child field 2"


# async def test_end(conn):
#     await conn.execute("""DROP SCHEMA IF EXISTS "poly" CASCADE""")
