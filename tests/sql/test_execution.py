import pytest

pytestmark = pytest.mark.asyncio


async def test_basic(pgsql):
    print(await pgsql.fetch("SELECT 1"))


async def test_basic2(pgsql):
    print(await pgsql.fetch("SELECT 'fuck yeah'"))
