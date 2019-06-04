import pytest
from yapic.entity.sql import wrap_connection, Entity, sync
from yapic.entity import (Serial, Int, String, ForeignKey, PrimaryKey, One, Many, ManyAcross, Registry, DependencyList,
                          Json, Composite, save_operations, Auto, Query, Loading)

pytestmark = pytest.mark.asyncio  # type: ignore


@pytest.yield_fixture  # type: ignore
async def conn(pgsql):
    yield wrap_connection(pgsql, "pgsql")


_registry = Registry()


class Address(Entity, registry=_registry, schema="ent_load"):
    id: Serial
    addr: String


class User(Entity, registry=_registry, schema="ent_load"):
    id: Serial
    name: String

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
    article: One[Article] = Loading(always=True)


async def test_sync(conn, pgclean):
    result = await sync(conn, _registry)
    assert result == """CREATE SCHEMA IF NOT EXISTS "ent_load";
CREATE SEQUENCE "ent_load"."Address_id_seq";
CREATE TABLE "ent_load"."Address" (
  "id" INT4 NOT NULL DEFAULT nextval('"ent_load"."Address_id_seq"'::regclass),
  "addr" TEXT,
  PRIMARY KEY("id")
);
CREATE SEQUENCE "ent_load"."User_id_seq";
CREATE TABLE "ent_load"."User" (
  "id" INT4 NOT NULL DEFAULT nextval('"ent_load"."User_id_seq"'::regclass),
  "name" TEXT,
  "address_id" INT4,
  PRIMARY KEY("id"),
  CONSTRAINT "fk_User__address_id-Address__id" FOREIGN KEY ("address_id") REFERENCES "ent_load"."Address" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE SEQUENCE "ent_load"."Article_id_seq";
CREATE TABLE "ent_load"."Article" (
  "id" INT4 NOT NULL DEFAULT nextval('"ent_load"."Article_id_seq"'::regclass),
  "creator_id" INT4,
  "updater_id" INT4,
  PRIMARY KEY("id"),
  CONSTRAINT "fk_Article__creator_id-User__id" FOREIGN KEY ("creator_id") REFERENCES "ent_load"."User" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_Article__updater_id-User__id" FOREIGN KEY ("updater_id") REFERENCES "ent_load"."User" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE SEQUENCE "ent_load"."Something_id_seq";
CREATE TABLE "ent_load"."Something" (
  "id" INT4 NOT NULL DEFAULT nextval('"ent_load"."Something_id_seq"'::regclass),
  "article_id" INT4,
  PRIMARY KEY("id"),
  CONSTRAINT "fk_Something__article_id-Article__id" FOREIGN KEY ("article_id") REFERENCES "ent_load"."Article" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
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
  PRIMARY KEY("id"),
  CONSTRAINT "fk_UserChild__parent_id-User__id" FOREIGN KEY ("parent_id") REFERENCES "ent_load"."User" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
CREATE TABLE "ent_load"."UserTags" (
  "user_id" INT4 NOT NULL,
  "tag_id" INT4 NOT NULL,
  PRIMARY KEY("user_id", "tag_id"),
  CONSTRAINT "fk_UserTags__user_id-User__id" FOREIGN KEY ("user_id") REFERENCES "ent_load"."User" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_UserTags__tag_id-Tag__id" FOREIGN KEY ("tag_id") REFERENCES "ent_load"."Tag" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);"""
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

    user = await conn.select(Query(User).where(User.id == 1).load(User, User.address, User.children, User.tags)).first()
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
        creator=User(name="Article Creator", address=Address(addr="Creator Addr")),
        updater=User(name="Article Updater", address=Address(addr="Updater Addr")),
    )

    something = Something()
    something.article = article
    await conn.save(something)

    something = await conn.select(Query(Something)).first()
    assert something.id == 1
    assert something.article is not None
    assert something.article.id == 1
