# flake8: noqa: E501

import pytest
from yapic.entity import (
    Auto,
    Entity,
    ForeignKey,
    Int,
    Many,
    One,
    Query,
    Registry,
    Serial,
    String,
    func,
    virtual,
)
from yapic.entity.sql import sync

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
    ambiguous_field: String


class WorkerY(Worker, polymorph_id="workery", registry=REGISTRY):
    workery_field: String
    ambiguous_field: String


class Organization(Entity, schema="poly", registry=REGISTRY):
    id: Serial
    employee_id: Auto = ForeignKey(Employee.id)
    employee: One[Employee]
    managers: Many[Manager] = "_self_.employee_id == Manager.id"


async def test_sync(conn, pgclean):
    result = await sync(conn, REGISTRY)
    assert (
        result
        == """CREATE SCHEMA IF NOT EXISTS "poly";
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
  "ambiguous_field" TEXT,
  PRIMARY KEY("id")
);
CREATE TABLE "poly"."WorkerY" (
  "id" INT4 NOT NULL,
  "workery_field" TEXT,
  "ambiguous_field" TEXT,
  PRIMARY KEY("id")
);
ALTER TABLE "poly"."Manager"
  ADD CONSTRAINT "fk_Manager__id-Employee__id" FOREIGN KEY ("id") REFERENCES "poly"."Employee" ("id") ON UPDATE CASCADE ON DELETE CASCADE;
CREATE OR REPLACE FUNCTION "poly"."YT-Manager-polyd_Employee"() RETURNS TRIGGER AS $$
BEGIN
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
CREATE OR REPLACE FUNCTION "poly"."YT-Worker-polyd_Employee"() RETURNS TRIGGER AS $$
BEGIN
  DELETE FROM "poly"."Employee" "parent" WHERE "parent"."id"=OLD."id";
  RETURN OLD;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "polyd_Employee"
  AFTER DELETE ON "poly"."Worker"
  FOR EACH ROW
  EXECUTE FUNCTION "poly"."YT-Worker-polyd_Employee"();
ALTER TABLE "poly"."WorkerX"
  ADD CONSTRAINT "fk_WorkerX__id-Worker__id" FOREIGN KEY ("id") REFERENCES "poly"."Worker" ("id") ON UPDATE CASCADE ON DELETE CASCADE;
CREATE OR REPLACE FUNCTION "poly"."YT-WorkerX-polyd_Worker"() RETURNS TRIGGER AS $$
BEGIN
  DELETE FROM "poly"."Worker" "parent" WHERE "parent"."id"=OLD."id";
  RETURN OLD;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "polyd_Worker"
  AFTER DELETE ON "poly"."WorkerX"
  FOR EACH ROW
  EXECUTE FUNCTION "poly"."YT-WorkerX-polyd_Worker"();
ALTER TABLE "poly"."WorkerY"
  ADD CONSTRAINT "fk_WorkerY__id-Worker__id" FOREIGN KEY ("id") REFERENCES "poly"."Worker" ("id") ON UPDATE CASCADE ON DELETE CASCADE;
CREATE OR REPLACE FUNCTION "poly"."YT-WorkerY-polyd_Worker"() RETURNS TRIGGER AS $$
BEGIN
  DELETE FROM "poly"."Worker" "parent" WHERE "parent"."id"=OLD."id";
  RETURN OLD;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "polyd_Worker"
  AFTER DELETE ON "poly"."WorkerY"
  FOR EACH ROW
  EXECUTE FUNCTION "poly"."YT-WorkerY-polyd_Worker"();"""
    )

    await conn.execute(result)

    result = await sync(conn, Worker.__registry__)
    assert bool(result) is False

    assert Employee.__polymorph__["workery"] is WorkerY


async def test_query_from_worker(conn):
    q = Query().select_from(Worker).where(Worker.employee_field == "Nice")
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == """SELECT "t1"."id", "t1"."variant", "t1"."employee_field", "t0"."worker_field", "t2"."workerx_field", "t2"."ambiguous_field", "t3"."workery_field", "t3"."ambiguous_field" FROM "poly"."Worker" "t0" INNER JOIN "poly"."Employee" "t1" ON "t0"."id" = "t1"."id" LEFT JOIN "poly"."WorkerX" "t2" ON "t2"."id" = "t0"."id" LEFT JOIN "poly"."WorkerY" "t3" ON "t3"."id" = "t0"."id" WHERE "t1"."employee_field" = $1"""
    )
    assert params == ("Nice",)


async def test_query_from_employee(conn):
    q = Query().select_from(Employee).where(Employee.employee_field == "Nice").where(Worker.worker_field == "WF")
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == """SELECT "t0"."id", "t0"."variant", "t0"."employee_field", "t1"."manager_field", "t2"."worker_field", "t3"."workerx_field", "t3"."ambiguous_field", "t4"."workery_field", "t4"."ambiguous_field" FROM "poly"."Employee" "t0" LEFT JOIN "poly"."Manager" "t1" ON "t1"."id" = "t0"."id" LEFT JOIN "poly"."Worker" "t2" ON "t2"."id" = "t0"."id" LEFT JOIN "poly"."WorkerX" "t3" ON "t3"."id" = "t2"."id" LEFT JOIN "poly"."WorkerY" "t4" ON "t4"."id" = "t2"."id" WHERE "t0"."employee_field" = $1 AND "t2"."worker_field" = $2"""
    )
    assert params == ("Nice", "WF")


async def test_query_org(conn):
    q = Query().select_from(Organization).where(Employee.employee_field == "Nice").where(Worker.worker_field == "WF")
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == """SELECT "t0"."id", "t0"."employee_id" FROM "poly"."Organization" "t0" INNER JOIN "poly"."Employee" "t1" ON "t0"."employee_id" = "t1"."id" INNER JOIN "poly"."Worker" "t2" ON "t2"."id" = "t1"."id" WHERE "t1"."employee_field" = $1 AND "t2"."worker_field" = $2"""
    )
    assert params == ("Nice", "WF")


async def test_query_from_workerx(conn):
    q = Query().select_from(WorkerX).where(Employee.employee_field == "OK")
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == """SELECT "t2"."id", "t2"."variant", "t2"."employee_field", "t1"."worker_field", "t0"."workerx_field", "t0"."ambiguous_field" FROM "poly"."WorkerX" "t0" INNER JOIN "poly"."Worker" "t1" ON "t0"."id" = "t1"."id" INNER JOIN "poly"."Employee" "t2" ON "t1"."id" = "t2"."id" WHERE "t2"."employee_field" = $1"""
    )
    assert params == ("OK",)


async def test_query_from_workerx_alias(conn):
    POLY = WorkerX.alias("POLY")
    q = Query().select_from(POLY).where(POLY.employee_field == "OK")
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT "t1"."id", "t1"."variant", "t1"."employee_field", "t0"."worker_field", "POLY"."workerx_field", "POLY"."ambiguous_field" FROM "poly"."WorkerX" "POLY" INNER JOIN "poly"."Worker" "t0" ON "POLY"."id" = "t0"."id" INNER JOIN "poly"."Employee" "t1" ON "t0"."id" = "t1"."id" WHERE "t1"."employee_field" = $1'
    )
    assert params == ("OK",)


async def test_load_one(conn):
    q = Query().select_from(Organization).load(Organization.employee).where(Organization.id == 1)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == """SELECT (SELECT ROW("t2"."id", "t2"."variant", "t2"."employee_field", "t3"."manager_field", "t4"."worker_field", "t5"."workerx_field", "t5"."ambiguous_field", "t6"."workery_field", "t6"."ambiguous_field") FROM "poly"."Employee" "t2" LEFT JOIN "poly"."Manager" "t3" ON "t3"."id" = "t2"."id" LEFT JOIN "poly"."Worker" "t4" ON "t4"."id" = "t2"."id" LEFT JOIN "poly"."WorkerX" "t5" ON "t5"."id" = "t4"."id" LEFT JOIN "poly"."WorkerY" "t6" ON "t6"."id" = "t4"."id" WHERE "t1"."employee_id" = "t2"."id") as "t0" FROM "poly"."Organization" "t1" WHERE "t1"."id" = $1"""
    )
    assert params == (1,)


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

    o = await conn.select(
        Query().select_from(Organization).load(Organization.employee).where(Organization.id == org.id)
    ).first()
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


async def test_ambiguous(conn, pgclean):
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
    assert (
        sql
        == 'SELECT "t2"."id", "t2"."pid1", "t2"."pid2", (SELECT ROW("t4"."id", "t4"."type", "t4"."base_field", "t3"."child_field") FROM "poly"."PolyChild" "t3" INNER JOIN "poly"."PolyBase" "t4" ON "t3"."id" = "t4"."id" WHERE "t2"."pid1" = "t3"."id") as "t0", (SELECT ROW("t6"."id", "t6"."type", "t6"."base_field", "t5"."child_field") FROM "poly"."PolyChild" "t5" INNER JOIN "poly"."PolyBase" "t6" ON "t5"."id" = "t6"."id" WHERE "t2"."pid2" = "t5"."id") as "t1" FROM "poly"."Something" "t2" WHERE "t2"."id" = $1'
    )

    s = await conn.select(q).first()
    assert s.p1.id == p1.id
    assert s.p1.base_field == "base field 1"
    assert s.p1.child_field == "child field 1"

    assert s.p2.id == p2.id
    assert s.p2.base_field == "base field 2"
    assert s.p2.child_field == "child field 2"

    # TODO: dont join PolyChild when not required (Something.p2.base_field == 2)
    q = Query().select_from(Something).where(Something.p1.child_field == 1, Something.p2.base_field == 2)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT "t0"."id", "t0"."pid1", "t0"."pid2" FROM "poly"."Something" "t0" INNER JOIN "poly"."PolyChild" "t1" ON "t0"."pid1" = "t1"."id" INNER JOIN "poly"."PolyChild" "t2" ON "t0"."pid2" = "t2"."id" INNER JOIN "poly"."PolyBase" "t3" ON "t2"."id" = "t3"."id" WHERE "t1"."child_field" = $1 AND "t3"."base_field" = $2'
    )
    assert params == (1, 2)


async def test_order_by(conn):
    q = Query(Worker).order(Worker.employee_field.asc())
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT "t1"."id", "t1"."variant", "t1"."employee_field", "t0"."worker_field", "t2"."workerx_field", "t2"."ambiguous_field", "t3"."workery_field", "t3"."ambiguous_field" FROM "poly"."Worker" "t0" INNER JOIN "poly"."Employee" "t1" ON "t0"."id" = "t1"."id" LEFT JOIN "poly"."WorkerX" "t2" ON "t2"."id" = "t0"."id" LEFT JOIN "poly"."WorkerY" "t3" ON "t3"."id" = "t0"."id" ORDER BY "t1"."employee_field" ASC'
    )

    q = Query(Worker).load(Worker.worker_field).order(Worker.employee_field.asc())
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT "t1"."id", "t1"."variant", "t1"."employee_field", "t0"."worker_field", "t2"."workerx_field", "t2"."ambiguous_field", "t3"."workery_field", "t3"."ambiguous_field" FROM "poly"."Worker" "t0" INNER JOIN "poly"."Employee" "t1" ON "t0"."id" = "t1"."id" LEFT JOIN "poly"."WorkerX" "t2" ON "t2"."id" = "t0"."id" LEFT JOIN "poly"."WorkerY" "t3" ON "t3"."id" = "t0"."id" ORDER BY "t1"."employee_field" ASC'
    )

    wa = Worker.alias("wa")
    q = Query(wa).order(wa.employee_field.asc())
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT "t0"."id", "t0"."variant", "t0"."employee_field", "wa"."worker_field", "t1"."workerx_field", "t1"."ambiguous_field", "t2"."workery_field", "t2"."ambiguous_field" FROM "poly"."Worker" "wa" INNER JOIN "poly"."Employee" "t0" ON "wa"."id" = "t0"."id" LEFT JOIN "poly"."WorkerX" "t1" ON "t1"."id" = "wa"."id" LEFT JOIN "poly"."WorkerY" "t2" ON "t2"."id" = "wa"."id" ORDER BY "t0"."employee_field" ASC'
    )

    q = Query(wa).load(wa.worker_field).order(wa.employee_field.asc())
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT "t0"."id", "t0"."variant", "t0"."employee_field", "wa"."worker_field", "t1"."workerx_field", "t1"."ambiguous_field", "t2"."workery_field", "t2"."ambiguous_field" FROM "poly"."Worker" "wa" INNER JOIN "poly"."Employee" "t0" ON "wa"."id" = "t0"."id" LEFT JOIN "poly"."WorkerX" "t1" ON "t1"."id" = "wa"."id" LEFT JOIN "poly"."WorkerY" "t2" ON "t2"."id" = "wa"."id" ORDER BY "t0"."employee_field" ASC'
    )


@pytest.mark.skip("TODO")
async def test_child_field_from_parent(conn):
    # print(Employee.worker_field)
    # print(Employee.workerx_field)
    # print(Employee.workery_field)
    # print("***", WorkerX.ambiguous_field)
    # print("***", WorkerY.ambiguous_field)
    # print(Employee.ambiguous_field)

    q = Query(Employee).where(Employee.worker_field == 1)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT "t0"."id", "t0"."variant", "t0"."employee_field", "t1"."manager_field", "t2"."worker_field", "t3"."workerx_field", "t3"."ambiguous_field", "t4"."workery_field", "t4"."ambiguous_field" FROM "poly"."Employee" "t0" LEFT JOIN "poly"."Manager" "t1" ON "t1"."id" = "t0"."id" LEFT JOIN "poly"."Worker" "t2" ON "t2"."id" = "t0"."id" LEFT JOIN "poly"."WorkerX" "t3" ON "t3"."id" = "t2"."id" LEFT JOIN "poly"."WorkerY" "t4" ON "t4"."id" = "t2"."id" WHERE "t2"."worker_field" = $1'
    )
    assert params == (1,)

    q = Query(Organization).where(Organization.employee.worker_field == 2)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT "t0"."id", "t0"."employee_id" FROM "poly"."Organization" "t0" INNER JOIN "poly"."Employee" "t1" ON "t0"."employee_id" = "t1"."id" INNER JOIN "poly"."Worker" "t2" ON "t2"."id" = "t1"."id" WHERE "t2"."worker_field" = $1'
    )

    q = Query(Organization).where(Organization.employee.ambiguous_field == 3)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    print(sql)
    assert sql == ""

    q = Query(Organization).order(Organization.employee.ambiguous_field.desc())
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    print(sql)
    assert sql == ""

    q = Query(Organization).load(Organization.employee.ambiguous_field)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    print(sql)
    assert sql == ""

    q = Query(Worker).where(Worker.ambiguous_field == 3)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    print(sql)
    assert sql == ""

    q = Query(Worker).order(Worker.ambiguous_field.desc())
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    print(sql)
    assert sql == ""

    q = Query(Worker).load(Worker.ambiguous_field)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    print(sql)
    assert sql == ""


async def test_load_related(conn):
    q = Query(Organization).columns(Organization.employee.employee_field).group(Organization.employee.employee_field)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT "t1"."employee_field" FROM "poly"."Organization" "t0" INNER JOIN "poly"."Employee" "t1" ON "t0"."employee_id" = "t1"."id" GROUP BY "t1"."employee_field"'
    )

    q = Query(Organization).columns(Organization.managers.employee_field).group(Organization.managers.employee_field)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT "t2"."employee_field" FROM "poly"."Organization" "t0" INNER JOIN "poly"."Manager" "t1" ON "t0"."employee_id" = "t1"."id" INNER JOIN "poly"."Employee" "t2" ON "t1"."id" = "t2"."id" GROUP BY "t2"."employee_field"'
    )


# @pytest.mark.skip("TODO")
async def test_virtual(conn, pgclean):
    registry = Registry()

    class Node(Entity, registry=registry, schema="poly", polymorph="type"):
        id: Serial
        parent_id: Auto = ForeignKey("Node.id")
        type: String
        name: String
        parent: One["Node"] = "_joined_.id == _self_.parent_id"

        @virtual
        def path(cls):
            return None

        # TODO:
        # @path.value
        # def path(cls, q):
        #     max_depth = 5
        #     parts = [cls.name]
        #     parent = cls.parent

        #     for i in range(max_depth):
        #         parts.append(parent.name)
        #         parent = parent.parent

        #     # # TODO: recursive query
        #     # dir = Node.alias("_dir_")
        #     # q.join(dir, dir.id == cls.parent_id, "INNER")
        #     return func.CONCAT_WS("/", *reversed(parts))

        @path.value
        def path(cls, q):
            dir = Node.alias("_dir_")
            q.join(dir, dir.id == cls.parent_id, "INNER")
            return func.CONCAT_WS("/", dir.name, cls.name)

        @virtual
        def full_path(cls):
            return None

        # TODO: find a way to define join type, in this case LEFT is required
        @full_path.value
        def full_path(cls, q):
            max_depth = 3
            parts = [cls.name]
            parent = cls.parent

            for i in range(max_depth):
                parts.append(parent.name)
                parent = parent.parent

            return func.CONCAT_WS("/", *reversed(parts))

    class File(Node, polymorph_id="file"):
        size: Int

        @virtual
        def with_dir(cls):
            return None

        @with_dir.value
        def with_dir(cls, q):
            return func.CONCAT_WS("/", cls.parent.name, cls.name)

    class Dir(Node, polymorph_id="dir"):
        count: Int

        files: Many[File] = "poly.Dir.id == poly.File.parent_id"

    result = await sync(conn, registry)
    await conn.execute(result)

    async def _create_node(t, name, parent_id):
        node = t(name=name, parent_id=parent_id)
        await conn.save(node)
        return node

    root = await _create_node(Dir, "root", None)
    home = await _create_node(Dir, "home", root.id)
    zozzz = await _create_node(Dir, "zozzz", home.id)
    some_file = await _create_node(File, "test.py", zozzz.id)
    var = await _create_node(Dir, "var", root.id)
    log = await _create_node(Dir, "log", var.id)

    q = Query().select_from(Node).columns(Node.path).where(Node.id == some_file.id)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT CONCAT_WS($1, "_dir_"."name", "t0"."name") FROM "poly"."Node" "t0" INNER JOIN "poly"."Node" "_dir_" ON "_dir_"."id" = "t0"."parent_id" WHERE "t0"."id" = $2'
    )
    assert params == ("/", some_file.id)
    result = await conn.select(q).first()
    assert result == "zozzz/test.py"

    q = Query().select_from(File).columns(File.with_dir).where(Node.id == some_file.id)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT CONCAT_WS($1, "t2"."name", "t1"."name") FROM "poly"."File" "t0" INNER JOIN "poly"."Node" "t1" ON "t0"."id" = "t1"."id" INNER JOIN "poly"."Node" "t2" ON "t2"."id" = "t1"."parent_id" WHERE "t1"."id" = $2'
    )
    assert params == ("/", some_file.id)
    result = await conn.select(q).first()
    assert result == "zozzz/test.py"

    q = Query().select_from(Node).load(Node.id, Node.path)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT "t0"."id", "t0"."type", CONCAT_WS($1, "_dir_"."name", "t0"."name"), "t1"."size", "t2"."count" FROM "poly"."Node" "t0" INNER JOIN "poly"."Node" "_dir_" ON "_dir_"."id" = "t0"."parent_id" LEFT JOIN "poly"."File" "t1" ON "t1"."id" = "t0"."id" LEFT JOIN "poly"."Dir" "t2" ON "t2"."id" = "t0"."id"'
    )
    assert params == ("/",)
    result = await conn.select(q)
    expected = {
        home.id: "root/home",
        zozzz.id: "home/zozzz",
        some_file.id: "zozzz/test.py",
        var.id: "root/var",
        log.id: "var/log",
    }
    for x in result:
        assert x.path == expected[x.id]

    q = Query().select_from(Node).columns(Node.full_path).where(Node.id == some_file.id)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT CONCAT_WS($1, "t3"."name", "t2"."name", "t1"."name", "t0"."name") FROM "poly"."Node" "t0" INNER JOIN "poly"."Node" "t1" ON "t1"."id" = "t0"."parent_id" INNER JOIN "poly"."Node" "t2" ON "t2"."id" = "t1"."parent_id" INNER JOIN "poly"."Node" "t3" ON "t3"."id" = "t2"."parent_id" WHERE "t0"."id" = $2'
    )
    assert params == ("/", some_file.id)
    result = await conn.select(q).first()
    assert result == "root/home/zozzz/test.py"

    q = Query().select_from(Dir).load(Dir, Dir.files, Dir.files.with_dir).where(Dir.id == zozzz.id)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert (
        sql
        == 'SELECT "t3"."id", "t3"."parent_id", "t3"."type", "t3"."name", "t2"."count", (SELECT ARRAY_AGG("t1") FROM (SELECT "t5"."id", "t5"."parent_id", "t5"."type", "t5"."name", "t4"."size", CONCAT_WS($1, "t6"."name", "t5"."name") FROM "poly"."File" "t4" INNER JOIN "poly"."Node" "t5" ON "t4"."id" = "t5"."id" INNER JOIN "poly"."Node" "t6" ON "t6"."id" = "t5"."parent_id" WHERE "t2"."id" = "t5"."parent_id") as "t1") as "t0" FROM "poly"."Dir" "t2" INNER JOIN "poly"."Node" "t3" ON "t2"."id" = "t3"."id" WHERE "t2"."id" = $2'
    )
    assert params == ("/", zozzz.id)
    result = await conn.select(q).first()
    assert result.name == "zozzz"
    assert result.files[0].id == some_file.id
    assert result.files[0].with_dir == "zozzz/test.py"


async def test_reduce_children(conn, pgclean):
    R = Registry()

    class Base(Entity, registry=R, schema="reduce_children", polymorph="type"):
        id: Serial
        type: String

    class A(Base, polymorph_id="A"):
        a_field: Int

    class B(A, polymorph_id="B"):
        b_field: Int

    class C(B, polymorph_id="C"):
        c_field: Int

    class C2(B, polymorph_id="C2"):
        c2_field: Int

    class D(C, polymorph_id="D"):
        d_field: Int

    class D2(C2, polymorph_id="D2"):
        d2_field: Int

    class E(D, polymorph_id="E"):
        e_field: Int

    class E2(D2, polymorph_id="E2"):
        e2_field: Int

    class E3(D, polymorph_id="E3"):
        e3_field: Int

    diff = await sync(conn, R)
    await conn.execute(diff)

    q = Query().select_from(B)
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert sql == '''SELECT "t2"."id", "t2"."type", "t1"."a_field", "t0"."b_field", "t3"."c_field", "t4"."d_field", "t5"."e_field", "t6"."e3_field", "t7"."c2_field", "t8"."d2_field", "t9"."e2_field" FROM "reduce_children"."B" "t0" INNER JOIN "reduce_children"."A" "t1" ON "t0"."id" = "t1"."id" INNER JOIN "reduce_children"."Base" "t2" ON "t1"."id" = "t2"."id" LEFT JOIN "reduce_children"."C" "t3" ON "t3"."id" = "t0"."id" LEFT JOIN "reduce_children"."D" "t4" ON "t4"."id" = "t3"."id" LEFT JOIN "reduce_children"."E" "t5" ON "t5"."id" = "t4"."id" LEFT JOIN "reduce_children"."E3" "t6" ON "t6"."id" = "t4"."id" LEFT JOIN "reduce_children"."C2" "t7" ON "t7"."id" = "t0"."id" LEFT JOIN "reduce_children"."D2" "t8" ON "t8"."id" = "t7"."id" LEFT JOIN "reduce_children"."E2" "t9" ON "t9"."id" = "t8"."id"'''

    q = Query().select_from(B).reduce_children({E3})
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert sql == '''SELECT "t2"."id", "t2"."type", "t1"."a_field", "t0"."b_field", "t3"."c_field", "t4"."d_field", "t5"."e3_field" FROM "reduce_children"."B" "t0" INNER JOIN "reduce_children"."A" "t1" ON "t0"."id" = "t1"."id" INNER JOIN "reduce_children"."Base" "t2" ON "t1"."id" = "t2"."id" LEFT JOIN "reduce_children"."C" "t3" ON "t3"."id" = "t0"."id" LEFT JOIN "reduce_children"."D" "t4" ON "t4"."id" = "t3"."id" LEFT JOIN "reduce_children"."E3" "t5" ON "t5"."id" = "t4"."id"'''

    q = Query().select_from(Base).reduce_children({E2, E3})
    sql, params = conn.dialect.create_query_compiler().compile_select(q)
    assert sql == '''SELECT "t0"."id", "t0"."type", "t1"."a_field", "t2"."b_field", "t3"."c_field", "t4"."d_field", "t5"."e3_field", "t6"."c2_field", "t7"."d2_field", "t8"."e2_field" FROM "reduce_children"."Base" "t0" LEFT JOIN "reduce_children"."A" "t1" ON "t1"."id" = "t0"."id" LEFT JOIN "reduce_children"."B" "t2" ON "t2"."id" = "t1"."id" LEFT JOIN "reduce_children"."C" "t3" ON "t3"."id" = "t2"."id" LEFT JOIN "reduce_children"."D" "t4" ON "t4"."id" = "t3"."id" LEFT JOIN "reduce_children"."E3" "t5" ON "t5"."id" = "t4"."id" LEFT JOIN "reduce_children"."C2" "t6" ON "t6"."id" = "t2"."id" LEFT JOIN "reduce_children"."D2" "t7" ON "t7"."id" = "t6"."id" LEFT JOIN "reduce_children"."E2" "t8" ON "t8"."id" = "t7"."id"'''

