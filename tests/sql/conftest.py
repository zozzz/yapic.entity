import asyncio
import json
import os
import sys
import time
from contextlib import contextmanager

import asyncpg
import docker
import pytest
import pytest_asyncio
from asyncpg.exceptions import OperatorInterventionError, PostgresConnectionError
from docker import APIClient
from docker.errors import ImageNotFound, NotFound
from yapic.entity.sql.pgsql import PostgreConnection

PG_DOCKER_TAG = "yapic_entity:pgsql_test"
SELF_PATH = os.path.realpath(os.path.dirname(__file__))
IN_DOCKER = int(os.getenv("IN_DOCKER", "0")) == 1

if False:
    import uvloop
    uvloop.install()


@contextmanager
def docker_client():
    client = docker.from_env()
    try:
        yield client
    finally:
        client.close()


def build_docker(path, tag, force=False):
    with docker_client() as client:
        if force is False:
            try:
                return client.images.get(tag)
            except ImageNotFound:
                pass

        cli = APIClient()
        try:
            for out in cli.build(path=path, rm=True, tag=tag):
                for line in out.decode("utf-8").split("\r\n"):
                    if line:
                        line = json.loads(line)
                        sys.stdout.write(line.get("stream", ""))
        finally:
            cli.close()

        return client.images.get(tag)


def start_container(image, name, ports):
    with docker_client() as client:
        try:
            return client.containers.get(name)
        except NotFound:
            # docker run -d --name yapic_entity_pgsql_docker -p 5432:5432 --rm yapic_entity:pgsql_test
            res = client.containers.run(
                image,
                name=name,
                detach=True,
                stream=False,
                remove=True,
                ports={f"{port}/tcp": port
                       for port in ports},
                cap_add=["SYS_PTRACE"],
            )
            time.sleep(5)
            return res


@pytest.fixture()
def pgsql_docker():
    if not IN_DOCKER:
        build_docker(SELF_PATH, PG_DOCKER_TAG)
        return start_container(PG_DOCKER_TAG, "yapic_entity_pgsql_docker", ports=[5432])


@pytest_asyncio.fixture
async def pgsql(pgsql_docker):
    host = "postgre" if IN_DOCKER else "127.0.0.1"
    for i in range(30):
        try:
            connection = await asyncpg.connect(user="postgres",
                                               password="root",
                                               database="root",
                                               host=host,
                                               connection_class=PostgreConnection)
            yield connection
        except (OperatorInterventionError, PostgresConnectionError):
            await asyncio.sleep(1)
        else:
            await connection.close()
            return


@pytest_asyncio.fixture
async def conn(pgsql):
    await pgsql.execute('CREATE EXTENSION IF NOT EXISTS "postgis"')
    yield pgsql


@pytest_asyncio.fixture
async def pgclean(pgsql):
    q = """SELECT 'DROP SCHEMA "' || nspname || '" CASCADE;'
        FROM pg_namespace
        WHERE nspname != 'information_schema'
            AND nspname != 'public'
            AND nspname NOT LIKE 'pg_%';"""
    for r in await pgsql.fetch(q):
        await pgsql.execute(r[0])
