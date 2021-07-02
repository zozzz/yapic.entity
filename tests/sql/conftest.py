import os, sys
import pytest
import docker
import json
import asyncpg
import asyncio
from asyncpg.exceptions import OperatorInterventionError, PostgresConnectionError
from docker import APIClient
from docker.errors import ImageNotFound, NotFound
from yapic.entity.sql.pgsql import PostgreConnection

cli = APIClient()
client = docker.from_env()

PG_DOCKER_TAG = "yapic_entity:pgsql_test"
SELF_PATH = os.path.realpath(os.path.dirname(__file__))


def build_docker(path, tag, force=False):
    if force is False:
        try:
            return client.images.get(tag)
        except ImageNotFound:
            pass

    for out in cli.build(path=path, rm=True, tag=tag):
        for line in out.decode("utf-8").split("\r\n"):
            if line:
                line = json.loads(line)
                sys.stdout.write(line.get("stream", ""))

    return client.images.get(tag)


def start_container(image, name, ports):
    try:
        return client.containers.get(name)
    except NotFound:
        # docker run -d --name yapic_entity_pgsql_docker -p 5432:5432 --rm yapic_entity:pgsql_test
        return client.containers.run(
            image,
            name=name,
            detach=True,
            stream=False,
            remove=True,
            ports={f"{port}/tcp": port
                   for port in ports},
            cap_add=["SYS_PTRACE"],
        )


@pytest.fixture
def pgsql_docker():
    build_docker(SELF_PATH, PG_DOCKER_TAG)
    return start_container(PG_DOCKER_TAG, "yapic_entity_pgsql_docker", ports=[5432])


@pytest.fixture
async def pgsql(pgsql_docker):
    for i in range(30):
        try:
            connection = await asyncpg.connect(user="root",
                                               password="root",
                                               database="root",
                                               host="127.0.0.1",
                                               connection_class=PostgreConnection)
            yield connection
        except (OperatorInterventionError, PostgresConnectionError):
            await asyncio.sleep(1)
        else:
            await connection.close()
            return


# TODO: remove, csak a visszafele kompatibilitás miatt van
@pytest.fixture
async def conn(pgsql):
    await pgsql.execute('CREATE EXTENSION IF NOT EXISTS "postgis"')
    yield pgsql


@pytest.fixture
async def pgclean(pgsql):
    q = """SELECT 'DROP SCHEMA "' || nspname || '" CASCADE;'
        FROM pg_namespace
        WHERE nspname != 'information_schema'
            AND nspname != 'public'
            AND nspname NOT LIKE 'pg_%';"""
    for r in await pgsql.fetch(q):
        await pgsql.execute(r[0])
