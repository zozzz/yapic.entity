# flake8: noqa: E501

from datetime import date, datetime, timedelta

import pytest
from yapic.entity import (
    Date,
    DateTime,
    Entity,
    Int,
    PrimaryKey,
    Query,
    Registry,
    Serial,
    Time,
)
from yapic.entity.sql import PostgreDialect, sync

pytestmark = pytest.mark.asyncio
REGISTRY = Registry()
dialect = PostgreDialect()




async def test_datetime(pgclean, conn):
    REGISTRY = Registry()
    class Something(Entity, schema="date_time", registry=REGISTRY):
        id: Serial
        day: Date
        time: Time
        updated: DateTime
    diff = await sync(conn, REGISTRY)
    await conn.execute(diff)

    today = date.today()
    time_ = datetime.now().time()
    updated = datetime.now()
    inserted = Something(day=today, time=time_, updated=updated)
    await conn.save(inserted)

    queried = await conn.select(Query(Something).where(Something.id == inserted.id)).first()
    assert queried.day == today
    assert queried.time == time_
    assert queried.updated == updated

    today += timedelta(days=1)
    time_ = (datetime.now() + timedelta(seconds=100)).time()
    updated = datetime.now() + timedelta(days=2)
    queried.day = today
    queried.time = time_
    queried.updated = updated

    await conn.save(queried)
    assert queried.day == today
    assert queried.time == time_
    assert queried.updated == updated


async def test_day_as_pk(pgclean, conn):
    REGISTRY = Registry()
    class DayAsPK(Entity, schema="date_time", registry=REGISTRY):
        day: Date = PrimaryKey()
        value: Int
    diff = await sync(conn, REGISTRY)
    await conn.execute(diff)

    today = date.today()
    inserted = DayAsPK(day=today, value=1)
    await conn.save(inserted)

    queried = await conn.select(Query(DayAsPK).where(DayAsPK.day == inserted.day)).first()
    assert queried.day == today
    assert queried.value == 1

    queried.value = 2
    await conn.save(queried)

    queried = await conn.select(Query(DayAsPK).where(DayAsPK.day == inserted.day)).first()
    assert queried.value == 2
