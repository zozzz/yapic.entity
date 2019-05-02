import pytest
from yapic.entity.sql import wrap_connection, Entity, sync
from yapic.entity import (Serial, Int, String, ForeignKey, PrimaryKey, One, Many, ManyAcross, Registry, DependencyList,
                          Json, Composite, save_operations, Auto)

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


async def test_basics(conn):
    # print(Worker.__fields__)
    print(Employee.__meta__)
    print(Manager.__meta__)
    print(Worker.__meta__)
    print(Worker.__fields__, Worker.id, Worker.worker_field)


async def test_end(conn):
    await conn.conn.execute("""DROP SCHEMA IF EXISTS "poly" CASCADE""")
