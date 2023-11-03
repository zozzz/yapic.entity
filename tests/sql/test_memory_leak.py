# flake8: noqa: E501

import asyncio
import functools
import gc
import sys
import tracemalloc

import asyncpg
from yapic.entity import (
    Auto,
    Composite,
    Entity,
    ForeignKey,
    Index,
    Int,
    Many,
    ManyAcross,
    One,
    PrimaryKey,
    Query,
    Registry,
    Serial,
    String,
    UpdatedTime,
)
from yapic.entity.sql import PostgreDialect, sync

dialect = PostgreDialect()
REGISTRY = Registry()


class GlobalEntity(Entity, registry=REGISTRY, schema="memleak", _root=True):
    pass


class Name(GlobalEntity):
    family: String
    given: String


class Address(GlobalEntity):
    id: Serial
    value: String


class Tag(GlobalEntity):
    id: String = PrimaryKey()
    label: String


class User(GlobalEntity):
    id: Serial
    name: Composite[Name]
    address_id: Auto = ForeignKey(Address.id)
    updated_time: UpdatedTime
    tags: ManyAcross["UserTags", "Tag"]


class UserTags(GlobalEntity):
    user_id: Auto = ForeignKey(User.id) // PrimaryKey()
    tag_id: Auto = ForeignKey(Tag.id) // PrimaryKey()


class Article(GlobalEntity):
    id: Serial
    title: String
    author_id: Auto = ForeignKey(User.id)
    author: One[User]


# nem a legjobb, de ez van
async def test_setup(conn, pgclean):
    result = await sync(conn, REGISTRY)
    await conn.execute(result)


def test_entity():
    # TODO: polymorph
    registry = Registry()

    class LeakName(Entity, registry=registry):
        family: String
        given: String

    class LeakUser(Entity, registry=registry):
        id: Serial
        name: Composite[LeakName]
        email: String = Index(name="unique_email", unique=True)
        articles: Many["LeakArticle"]

    class LeakArticle(Entity, registry=registry):
        id: Serial
        author_id: Auto = ForeignKey(LeakUser.id)
        author: One["LeakUser"]

    class LeakNode(Entity, registry=registry):
        id: Serial
        parent_id: Auto = ForeignKey("LeakNode.id")
        parent: One["LeakNode"]

    user_alias = User.alias()


def test_poly_entity():
    # TODO: megoldani
    registry = Registry()

    class LeakNode(Entity, registry=registry, polymorph="type"):
        id: Serial
        type: String

    class LeakArticle(LeakNode, registry=registry, polymorph_id="article"):
        title: String


def test_query():
    q = Query(User).where(User.tags.label == "nice")
    cloned1 = q.clone()
    cloned2 = cloned1.clone()
    cloned3 = cloned2.clone()


def test_entity_state():
    registry = Registry()

    class User(Entity, registry=registry):
        id: Serial
        name: String

    user = User(id=1, name="Almafa")
    user.__state__.changes()
    user.__state__.changes_with_previous()
    user.__state__.changed_realtions()

    if user.__state__.is_dirty:
        pass

    if user.__state__.is_empty:
        pass

    user2 = User(id=2, name="Almafa")

    if user.__state__ == user2.__state__:
        pass


def test_polymorph():
    registry = Registry()

    class Node(Entity, registry=registry, polymorph="type"):
        id: Serial
        type: String
        title: String

    class File(Node, polymorph_id="file"):
        size: Int

    class Pdf(File, polymorph_id="pdf"):
        page_count: Int

    pdf_alias = Pdf.alias("pdf")
    q = Query(pdf_alias).where(pdf_alias.title == "file.pdf")
    dialect.create_query_compiler().compile_select(q)


# region: memleak kiderítés
def gc_objects():
    for obj in gc.get_objects():
        yield obj, sys.getrefcount(obj)


def memleak(fn):

    @functools.wraps(fn)
    async def wrapped(*args, **kwargs):
        # warm up
        await fn(*args, **kwargs)

        only_asyncpg = tracemalloc.Filter(inclusive=True, filename_pattern="*asyncpg*")
        only_yapic = tracemalloc.Filter(inclusive=True, filename_pattern="*yapic.entity*")
        skip_tests = tracemalloc.Filter(inclusive=False, filename_pattern="*yapic.entity/tests*")
        filters = [only_asyncpg, only_yapic]

        gc.collect()
        # seen = {id(obj): obj for obj in objgraph.get_leaking_objects()}
        seen = {id(obj): (obj, refcnt) for obj, refcnt in gc_objects()}

        tracemalloc.start(5)
        snapshot1 = tracemalloc.take_snapshot().filter_traces(filters)

        totref = sys.gettotalrefcount()

        await fn(*args, **kwargs)
        await fn(*args, **kwargs)
        await fn(*args, **kwargs)

        await asyncio.sleep(2)

        gc.collect()

        print("Leaked refcount", sys.gettotalrefcount() - totref)

        snapshot2 = tracemalloc.take_snapshot().filter_traces(filters)
        result = snapshot2.compare_to(snapshot1, "traceback")
        for diff in result:
            if diff.size_diff > 0:
                print(f"New: {diff.size_diff} B ({diff.count_diff} blk) "
                      f"Total: {diff.size} B ({diff.count} blk)")
                for line in diff.traceback.format():
                    print(f"    {line}")

        # leaked = [obj for obj in objgraph.get_leaking_objects() if id(obj) not in seen]
        leaked = [(obj, refcnt) for obj, refcnt in gc_objects() if seen.get(id(obj), (obj, refcnt))[1] > refcnt]
        for value in leaked:
            print(value)

    return wrapped


# regionend


# XXX: valami leak van az asyncpg-ben
async def test_asyncpg_connection():
    conn = await asyncpg.connect(
        user="postgres",
        password="root",
        database="postgres",
        host="127.0.0.1",
    )
    await conn.close()


async def test_sync(conn):
    diff = await sync(conn, REGISTRY)
    assert bool(diff) is False
