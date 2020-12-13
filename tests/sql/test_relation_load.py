import pytest
from yapic.entity.sql import wrap_connection, Entity, sync, raw, PostgreDialect
from yapic.entity import (Serial, Int, String, ForeignKey, PrimaryKey, One, Many, ManyAcross, Registry, DependencyList,
                          Json, Composite, save_operations, Auto, Query, Loading, Relation)
from yapic import json

pytestmark = pytest.mark.asyncio  # type: ignore


@pytest.yield_fixture  # type: ignore
async def conn(pgsql):
    yield wrap_connection(pgsql, "pgsql")


_registry = Registry()


class Address(Entity, registry=_registry, schema="ent_load"):
    id: Serial
    addr: String


class FullName(Entity, registry=_registry, schema="ent_load"):
    title: String
    family: String
    given: String


class User(Entity, registry=_registry, schema="ent_load"):
    id: Serial
    name: Composite[FullName]

    address_id: Auto = ForeignKey(Address.id)
    address: One[Address]

    children: Many["UserChild"]

    tags: ManyAcross["UserTags", "Tag"]


class Tag(Entity, registry=_registry, schema="ent_load"):
    id: Serial
    value: String


class UserTags(Entity, registry=_registry, schema="ent_load"):
    user_id: Auto = ForeignKey(User.id) // PrimaryKey()
    tag_id: Auto = ForeignKey(Tag.id) // PrimaryKey()


class UserChild(Entity, registry=_registry, schema="ent_load"):
    id: Serial
    parent_id: Auto = ForeignKey(User.id)
    name: String


class Article(Entity, registry=_registry, schema="ent_load"):
    id: Serial
    creator_id: Auto = ForeignKey(User.id)
    creator: One[User] = "User.id == Article.creator_id"
    updater_id: Auto = ForeignKey(User.id)
    updater: One[User] = "User.id == Article.updater_id"


class Something(Entity, registry=_registry, schema="ent_load"):
    id: Serial
    article_id: Auto = ForeignKey(Article.id)
    article: One[Article] = Relation(join="Article.id == Something.article_id") // Loading(always=True)


class Something2(Entity, registry=_registry, schema="ent_load"):
    id: Serial
    something_id: Auto = ForeignKey(Something.id)
    something: One[Something]


async def test_sync(conn, pgclean):
    result = await sync(conn, _registry)
    assert result == """CREATE SCHEMA IF NOT EXISTS "ent_load";
CREATE SEQUENCE "ent_load"."Address_id_seq";
CREATE TABLE "ent_load"."Address" (
  "id" INT4 NOT NULL DEFAULT nextval('"ent_load"."Address_id_seq"'::regclass),
  "addr" TEXT,
  PRIMARY KEY("id")
);
CREATE SEQUENCE "ent_load"."Article_id_seq";
CREATE TYPE "ent_load"."FullName" AS (
  "title" TEXT,
  "family" TEXT,
  "given" TEXT
);
CREATE SEQUENCE "ent_load"."User_id_seq";
CREATE TABLE "ent_load"."User" (
  "id" INT4 NOT NULL DEFAULT nextval('"ent_load"."User_id_seq"'::regclass),
  "name" "ent_load"."FullName",
  "address_id" INT4,
  PRIMARY KEY("id")
);
CREATE TABLE "ent_load"."Article" (
  "id" INT4 NOT NULL DEFAULT nextval('"ent_load"."Article_id_seq"'::regclass),
  "creator_id" INT4,
  "updater_id" INT4,
  PRIMARY KEY("id")
);
CREATE SEQUENCE "ent_load"."Something_id_seq";
CREATE TABLE "ent_load"."Something" (
  "id" INT4 NOT NULL DEFAULT nextval('"ent_load"."Something_id_seq"'::regclass),
  "article_id" INT4,
  PRIMARY KEY("id")
);
CREATE SEQUENCE "ent_load"."Something2_id_seq";
CREATE TABLE "ent_load"."Something2" (
  "id" INT4 NOT NULL DEFAULT nextval('"ent_load"."Something2_id_seq"'::regclass),
  "something_id" INT4,
  PRIMARY KEY("id")
);
CREATE SEQUENCE "ent_load"."Tag_id_seq";
CREATE TABLE "ent_load"."Tag" (
  "id" INT4 NOT NULL DEFAULT nextval('"ent_load"."Tag_id_seq"'::regclass),
  "value" TEXT,
  PRIMARY KEY("id")
);
CREATE SEQUENCE "ent_load"."UserChild_id_seq";
CREATE TABLE "ent_load"."UserChild" (
  "id" INT4 NOT NULL DEFAULT nextval('"ent_load"."UserChild_id_seq"'::regclass),
  "parent_id" INT4,
  "name" TEXT,
  PRIMARY KEY("id")
);
CREATE TABLE "ent_load"."UserTags" (
  "user_id" INT4 NOT NULL,
  "tag_id" INT4 NOT NULL,
  PRIMARY KEY("user_id", "tag_id")
);
CREATE INDEX "idx_User__address_id" ON "ent_load"."User" USING btree ("address_id");
ALTER TABLE "ent_load"."User"
  ADD CONSTRAINT "fk_User__address_id-Address__id" FOREIGN KEY ("address_id") REFERENCES "ent_load"."Address" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
CREATE INDEX "idx_Article__creator_id" ON "ent_load"."Article" USING btree ("creator_id");
CREATE INDEX "idx_Article__updater_id" ON "ent_load"."Article" USING btree ("updater_id");
ALTER TABLE "ent_load"."Article"
  ADD CONSTRAINT "fk_Article__creator_id-User__id" FOREIGN KEY ("creator_id") REFERENCES "ent_load"."User" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  ADD CONSTRAINT "fk_Article__updater_id-User__id" FOREIGN KEY ("updater_id") REFERENCES "ent_load"."User" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
CREATE INDEX "idx_Something__article_id" ON "ent_load"."Something" USING btree ("article_id");
ALTER TABLE "ent_load"."Something"
  ADD CONSTRAINT "fk_Something__article_id-Article__id" FOREIGN KEY ("article_id") REFERENCES "ent_load"."Article" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
CREATE INDEX "idx_Something2__something_id" ON "ent_load"."Something2" USING btree ("something_id");
ALTER TABLE "ent_load"."Something2"
  ADD CONSTRAINT "fk_Something2__something_id-Something__id" FOREIGN KEY ("something_id") REFERENCES "ent_load"."Something" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
CREATE INDEX "idx_UserChild__parent_id" ON "ent_load"."UserChild" USING btree ("parent_id");
ALTER TABLE "ent_load"."UserChild"
  ADD CONSTRAINT "fk_UserChild__parent_id-User__id" FOREIGN KEY ("parent_id") REFERENCES "ent_load"."User" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE "ent_load"."UserTags"
  ADD CONSTRAINT "fk_UserTags__user_id-User__id" FOREIGN KEY ("user_id") REFERENCES "ent_load"."User" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  ADD CONSTRAINT "fk_UserTags__tag_id-Tag__id" FOREIGN KEY ("tag_id") REFERENCES "ent_load"."Tag" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;"""
    await conn.conn.execute(result)


async def test_load(conn):
    addr = Address(addr="Address1")
    await conn.save(addr)
    await conn.save(Address(addr="Address2"))

    tag1 = Tag(value="tag1")
    await conn.save(tag1)

    tag2 = Tag(value="tag2")
    await conn.save(tag2)

    await conn.save(Tag(value="tag3"))

    user = User(name="User", address=addr, tags=[tag1, tag2])
    user.children.append(UserChild(name="Child1"))
    user.children.append(UserChild(name="Child2"))
    await conn.save(user)

    q = Query(User).where(User.id == 1).load(User, User.address, User.children, User.tags)
    dialect = PostgreDialect()
    sql, params = dialect.create_query_compiler().compile_select(q)
    assert sql == """SELECT "t0"."id", ("t0"."name")."title", ("t0"."name")."family", ("t0"."name")."given", "t0"."address_id", "t1"."id", "t1"."addr" FROM "ent_load"."User" "t0" LEFT JOIN "ent_load"."Address" "t1" ON "t0"."address_id" = "t1"."id" WHERE "t0"."id" = $1"""
    assert params == (1, )

    user = await conn.select(q).first()
    assert user.id == 1
    assert user.address.id == user.address_id
    assert user.address.addr == "Address1"
    assert len(user.children) == 2
    assert user.children[0].name in ("Child1", "Child2")
    assert user.children[1].name in ("Child1", "Child2")
    assert user.children[0].name != user.children[1].name
    assert len(user.tags) == 2
    assert user.tags[0].value in ("tag1", "tag2")
    assert user.tags[1].value in ("tag1", "tag2")
    assert user.tags[0].value != user.tags[1].value


async def test_always_load(conn):
    article = Article(
        creator=User(
            name={
                "family": "Article",
                "given": "Creator"
            },
            address=Address(addr="Creator Addr"),
        ),
        updater=User(
            name={
                "family": "Article",
                "given": "Updater"
            },
            address=Address(addr="Updater Addr"),
        ),
    )

    something = Something()
    something.article = article
    await conn.save(something)

    something = await conn.select(Query(Something)).first()
    assert something.id == 1
    assert something.article is not None
    assert something.article.id == 1


async def test_load_empty_across(conn):
    user = User(name={"family": "User"})
    await conn.save(user)

    user = await conn.select(Query(User).load(User, User.tags).where(User.id == user.id)).first()
    assert user.name.family == "User"
    assert user.tags == []


async def test_mixin(conn):
    class Tracking:
        creator_id: Auto = ForeignKey(User.id)
        creator: One[User] = lambda cls: cls.creator_id == User.id

    class Tracked(Entity, Tracking, registry=_registry, schema="ent_load"):
        id: Serial

    result = await sync(conn, _registry)
    await conn.conn.execute(result)

    t = Tracked(creator_id=1)
    await conn.save(t)

    t = await conn.select(Query(Tracked).load(Tracked.creator).where(Tracked.id == t.id)).first()
    assert t.creator.id == 1


async def test_load_only_relation_field(conn):
    article_o = Article(
        id=42,
        creator=User(
            name={
                "family": "Article",
                "given": "Creator"
            },
            tags=[
                Tag(value="ctag1"),
                Tag(value="ctag2"),
                Tag(value="ctag3"),
            ],
            address=Address(addr="Creator Addr"),
        ),
        updater=User(
            name={
                "family": "Article",
                "given": "Updater"
            },
            address=Address(addr="Updater Addr"),
        ),
    )

    await conn.save(article_o)

    # TODO: remove empty relations from result
    q = Query(Article).where(Article.id == 42).load(Article.creator.name)
    article = await conn.select(q).first()
    data = json.dumps(article)
    assert data == """{"creator":{"name":{"family":"Article","given":"Creator"},"children":[],"tags":[]}}"""

    # TODO: ...
    # q = Query(Article).where(Article.id == 42).load(Article.creator.name.family)
    # article = await conn.select(q).first()
    # data = json.dumps(article)
    # assert data == """{"creator":{"name":{"family":"Article"},"children":[],"tags":[]}}"""

    # TODO: remove empty name from result
    q = Query(Article).where(Article.id == 42).load(Article.creator.address.addr)
    article = await conn.select(q).first()
    data = json.dumps(article)
    assert data == """{"creator":{"name":{},"address":{"addr":"Creator Addr"},"children":[],"tags":[]}}"""

    something2 = Something2(something=Something(article=article_o))
    await conn.save(something2)

    q = Query(Something2).where(Something2.id == something2.id).load(Something2.id, Something2.something)
    s = await conn.select(q).first()
    data = json.dumps(s)
    # TODO: itt lehet nem kéne betöltenie az something.article értékét
    assert data == """{"id":1,"something":{"id":2,"article_id":42,"article":{"id":42,"creator_id":5,"updater_id":6}}}"""

    # TODO: ...
    # q = Query(Article).where(Article.id == 42).load(Article.creator.tags.value)
    # s = await conn.select(q).first()
    # data = json.dumps(s)
    # assert data == """ """


async def test_deep_relation_where(conn):
    q = Query(Something2).where(Something2.something.article.creator.name.family == "Article")
    something2 = await conn.select(q).first()

    assert bool(something2) is True
    assert something2.id == 1


async def test_load_multi_entity(conn):
    user = User(
        name={"family": "User"},
        address=Address(addr="XYZ Addr"),
    )
    await conn.save(user)

    other_field = raw(f"1").alias("something")

    q = Query(User) \
        .columns(User, other_field) \
        .load(User.id, User.address.addr) \
        .where(User.id == user.id)

    user = await conn.select(q).first()
    data = json.dumps(user)
    # TODO: üres nem kívánt értékek törlése
    assert data == """[{"id":7,"name":{},"address":{"addr":"XYZ Addr"},"children":[],"tags":[]},1]"""
