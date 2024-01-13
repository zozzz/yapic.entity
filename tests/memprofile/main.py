import asyncio

from config import REGISTRY, Connection, connect, profile

DO_NOT_REORDER_IMPORT = ""

from models import User
from yapic.entity.sql import sync

USER_COUNT = 100


@profile
async def migrate(conn: Connection):
    diff = await sync(conn, REGISTRY)
    if diff:
        print(diff)
        await conn.execute(diff)


@profile
async def insert_user(conn: Connection):
    for i in range(USER_COUNT):
        await User.insert(conn, id=i+1, name={"surname": "Teszt", "forename": "Elek"}, email="example@c.com")

@profile
async def query_users_all(conn: Connection):
    users = await conn.select(User.query())
    for user in users:
        pass

@profile
async def query_users_one_by_one(conn: Connection):
    for i in range(USER_COUNT):
        user = await conn.select(User.query().where(User.id == i+1)).first()

@profile
async def update_users(conn: Connection):
    for i in range(USER_COUNT):
        user = await conn.select(User.query().where(User.id == i+1)).first()
        user.email = f"email{i}@example.com"
        await conn.save(user)


async def cleanup(conn: Connection):
    await User.clear(conn)

@profile
async def main():
    async with connect() as conn:
        await migrate(conn)
        await insert_user(conn)
        await query_users_all(conn)
        await query_users_one_by_one(conn)
        await update_users(conn)
        await cleanup(conn)

if __name__ == "__main__":
    asyncio.run(main())
