# flake8: noqa: E501

import pytest
from asyncpg.exceptions import ForeignKeyViolationError, UniqueViolationError
from yapic.entity import Auto, Entity, ForeignKeyList, Query, Registry, Serial, String
from yapic.entity.sql import sync

pytestmark = pytest.mark.asyncio


async def test_ddl(conn, pgclean):
    reg = Registry()

    class A(Entity, registry=reg, schema="fkl"):
        id: Serial
        name: String

    class XDelete(Entity, registry=reg, schema="fkl"):
        id: Serial
        restrict_ids: Auto = ForeignKeyList(A.id, on_delete="RESTRICT", on_update="NO ACTION")
        cascade_ids: Auto = ForeignKeyList(A.id, on_delete="CASCADE", on_update="NO ACTION")
        null_ids: Auto = ForeignKeyList(A.id, on_delete="SET NULL", on_update="NO ACTION")
        # a_items: Many[A] # Maybe good

    class XUpdate(Entity, registry=reg, schema="fkl"):
        id: Serial
        restrict_ids: Auto = ForeignKeyList(A.id, on_update="RESTRICT", on_delete="NO ACTION")
        cascade_ids: Auto = ForeignKeyList(A.id, on_update="CASCADE", on_delete="NO ACTION")

    result = await sync(conn, reg)
    assert result == """CREATE SCHEMA IF NOT EXISTS "fkl";
CREATE SEQUENCE "fkl"."A_id_seq";
CREATE TABLE "fkl"."A" (
  "id" INT4 NOT NULL DEFAULT nextval('"fkl"."A_id_seq"'::regclass),
  "name" TEXT,
  PRIMARY KEY("id")
);
CREATE SEQUENCE "fkl"."XDelete_id_seq";
CREATE TABLE "fkl"."XDelete" (
  "id" INT4 NOT NULL DEFAULT nextval('"fkl"."XDelete_id_seq"'::regclass),
  "restrict_ids" INT4[],
  "cascade_ids" INT4[],
  "null_ids" INT4[],
  PRIMARY KEY("id")
);
CREATE SEQUENCE "fkl"."XUpdate_id_seq";
CREATE TABLE "fkl"."XUpdate" (
  "id" INT4 NOT NULL DEFAULT nextval('"fkl"."XUpdate_id_seq"'::regclass),
  "restrict_ids" INT4[],
  "cascade_ids" INT4[],
  PRIMARY KEY("id")
);
CREATE OR REPLACE FUNCTION "fkl"."YT-A-fk_XDelete__restrict_ids-A__id-RD-a15090"() RETURNS TRIGGER AS $$
BEGIN
  IF EXISTS(SELECT 1 FROM "fkl"."XDelete" WHERE "restrict_ids" @> ARRAY[OLD."id"]) THEN
      RAISE EXCEPTION 'ForeignKeyList prevent of record delete, because has references %', OLD
          USING ERRCODE = 'foreign_key_violation';
  END IF;
  RETURN OLD;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XDelete__restrict_ids-A__id-RD"
  BEFORE DELETE ON "fkl"."A"
  FOR EACH ROW
  EXECUTE FUNCTION "fkl"."YT-A-fk_XDelete__restrict_ids-A__id-RD-a15090"();
CREATE OR REPLACE FUNCTION "fkl"."YT-A-fk_XDelete__cascade_ids-A__id-RD-947544"() RETURNS TRIGGER AS $$
BEGIN
  UPDATE "fkl"."XDelete"
      SET "cascade_ids" = array_remove("cascade_ids", OLD."id")
  WHERE "cascade_ids" @> ARRAY[OLD."id"];
  RETURN OLD;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XDelete__cascade_ids-A__id-RD"
  AFTER DELETE ON "fkl"."A"
  FOR EACH ROW
  EXECUTE FUNCTION "fkl"."YT-A-fk_XDelete__cascade_ids-A__id-RD-947544"();
CREATE OR REPLACE FUNCTION "fkl"."YT-A-fk_XDelete__null_ids-A__id-RD-1b4d00"() RETURNS TRIGGER AS $$
BEGIN
  UPDATE "fkl"."XDelete"
      SET "null_ids" = array_replace("null_ids", OLD."id", NULL)
  WHERE "null_ids" @> ARRAY[OLD."id"];
  RETURN OLD;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XDelete__null_ids-A__id-RD"
  AFTER DELETE ON "fkl"."A"
  FOR EACH ROW
  EXECUTE FUNCTION "fkl"."YT-A-fk_XDelete__null_ids-A__id-RD-1b4d00"();
CREATE OR REPLACE FUNCTION "fkl"."YT-A-fk_XUpdate__restrict_ids-A__id-RU-b66f6e-dee42b"() RETURNS TRIGGER AS $$
BEGIN
  IF EXISTS(SELECT 1 FROM "fkl"."XUpdate" WHERE "restrict_ids" @> ARRAY[OLD."id"]) THEN
      RAISE EXCEPTION 'ForeignKeyList prevent of record update, because has references %', NEW
          USING ERRCODE = 'foreign_key_violation';
  END IF;
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XUpdate__restrict_ids-A__id-RU"
  BEFORE UPDATE ON "fkl"."A"
  FOR EACH ROW
  WHEN (OLD."id" IS DISTINCT FROM NEW."id")
  EXECUTE FUNCTION "fkl"."YT-A-fk_XUpdate__restrict_ids-A__id-RU-b66f6e-dee42b"();
CREATE OR REPLACE FUNCTION "fkl"."YT-A-fk_XUpdate__cascade_ids-A__id-RU-b66f6e-f29b6c"() RETURNS TRIGGER AS $$
BEGIN
  UPDATE "fkl"."XUpdate"
      SET "cascade_ids" = array_replace("cascade_ids", OLD."id", NEW."id")
  WHERE "cascade_ids" @> ARRAY[OLD."id"];
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XUpdate__cascade_ids-A__id-RU"
  AFTER UPDATE ON "fkl"."A"
  FOR EACH ROW
  WHEN (OLD."id" IS DISTINCT FROM NEW."id")
  EXECUTE FUNCTION "fkl"."YT-A-fk_XUpdate__cascade_ids-A__id-RU-b66f6e-f29b6c"();
CREATE INDEX "idx_XDelete__restrict_ids" ON "fkl"."XDelete" USING gin ("restrict_ids");
CREATE INDEX "idx_XDelete__cascade_ids" ON "fkl"."XDelete" USING gin ("cascade_ids");
CREATE INDEX "idx_XDelete__null_ids" ON "fkl"."XDelete" USING gin ("null_ids");
CREATE OR REPLACE FUNCTION "fkl"."YT-XDelete-fk_XDelete__restrict_ids-A__id-CI-687eaa"() RETURNS TRIGGER AS $$
DECLARE missing RECORD;
BEGIN
  IF array_length(array_remove(NEW."restrict_ids", NULL), 1)
      <> array_length((SELECT array_agg(DISTINCT ref_id) FROM unnest(array_remove(NEW."restrict_ids", NULL)) fkl(ref_id)), 1) THEN
      RAISE EXCEPTION 'ForeignKeyList items is not unique %', NEW
          USING ERRCODE = 'unique_violation';
  END IF;
  FOR missing IN SELECT ref_id
      FROM unnest(array_remove(NEW."restrict_ids", NULL)) fkl(ref_id)
      WHERE NOT EXISTS(SELECT 1 FROM "fkl"."A" WHERE "id" = fkl.ref_id)
  LOOP
      RAISE EXCEPTION 'ForeignKeyList missing entry "fkl"."A"."id"=%" %', missing.ref_id, NEW
          USING ERRCODE = 'foreign_key_violation';
  END LOOP;
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XDelete__restrict_ids-A__id-CI"
  BEFORE INSERT ON "fkl"."XDelete"
  FOR EACH ROW
  EXECUTE FUNCTION "fkl"."YT-XDelete-fk_XDelete__restrict_ids-A__id-CI-687eaa"();
CREATE OR REPLACE FUNCTION "fkl"."YT-XDelete-fk_XDelete__restrict_ids-A__id-CU-3828be-687eaa"() RETURNS TRIGGER AS $$
DECLARE missing RECORD;
BEGIN
  IF array_length(array_remove(NEW."restrict_ids", NULL), 1)
      <> array_length((SELECT array_agg(DISTINCT ref_id) FROM unnest(array_remove(NEW."restrict_ids", NULL)) fkl(ref_id)), 1) THEN
      RAISE EXCEPTION 'ForeignKeyList items is not unique %', NEW
          USING ERRCODE = 'unique_violation';
  END IF;
  FOR missing IN SELECT ref_id
      FROM unnest(array_remove(NEW."restrict_ids", NULL)) fkl(ref_id)
      WHERE NOT EXISTS(SELECT 1 FROM "fkl"."A" WHERE "id" = fkl.ref_id)
  LOOP
      RAISE EXCEPTION 'ForeignKeyList missing entry "fkl"."A"."id"=%" %', missing.ref_id, NEW
          USING ERRCODE = 'foreign_key_violation';
  END LOOP;
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XDelete__restrict_ids-A__id-CU"
  BEFORE UPDATE ON "fkl"."XDelete"
  FOR EACH ROW
  WHEN (OLD."restrict_ids" IS DISTINCT FROM NEW."restrict_ids")
  EXECUTE FUNCTION "fkl"."YT-XDelete-fk_XDelete__restrict_ids-A__id-CU-3828be-687eaa"();
CREATE OR REPLACE FUNCTION "fkl"."YT-XDelete-fk_XDelete__cascade_ids-A__id-CI-50a446"() RETURNS TRIGGER AS $$
DECLARE missing RECORD;
BEGIN
  IF array_length(array_remove(NEW."cascade_ids", NULL), 1)
      <> array_length((SELECT array_agg(DISTINCT ref_id) FROM unnest(array_remove(NEW."cascade_ids", NULL)) fkl(ref_id)), 1) THEN
      RAISE EXCEPTION 'ForeignKeyList items is not unique %', NEW
          USING ERRCODE = 'unique_violation';
  END IF;
  FOR missing IN SELECT ref_id
      FROM unnest(array_remove(NEW."cascade_ids", NULL)) fkl(ref_id)
      WHERE NOT EXISTS(SELECT 1 FROM "fkl"."A" WHERE "id" = fkl.ref_id)
  LOOP
      RAISE EXCEPTION 'ForeignKeyList missing entry "fkl"."A"."id"=%" %', missing.ref_id, NEW
          USING ERRCODE = 'foreign_key_violation';
  END LOOP;
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XDelete__cascade_ids-A__id-CI"
  BEFORE INSERT ON "fkl"."XDelete"
  FOR EACH ROW
  EXECUTE FUNCTION "fkl"."YT-XDelete-fk_XDelete__cascade_ids-A__id-CI-50a446"();
CREATE OR REPLACE FUNCTION "fkl"."YT-XDelete-fk_XDelete__cascade_ids-A__id-CU-f3fd34-50a446"() RETURNS TRIGGER AS $$
DECLARE missing RECORD;
BEGIN
  IF array_length(array_remove(NEW."cascade_ids", NULL), 1)
      <> array_length((SELECT array_agg(DISTINCT ref_id) FROM unnest(array_remove(NEW."cascade_ids", NULL)) fkl(ref_id)), 1) THEN
      RAISE EXCEPTION 'ForeignKeyList items is not unique %', NEW
          USING ERRCODE = 'unique_violation';
  END IF;
  FOR missing IN SELECT ref_id
      FROM unnest(array_remove(NEW."cascade_ids", NULL)) fkl(ref_id)
      WHERE NOT EXISTS(SELECT 1 FROM "fkl"."A" WHERE "id" = fkl.ref_id)
  LOOP
      RAISE EXCEPTION 'ForeignKeyList missing entry "fkl"."A"."id"=%" %', missing.ref_id, NEW
          USING ERRCODE = 'foreign_key_violation';
  END LOOP;
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XDelete__cascade_ids-A__id-CU"
  BEFORE UPDATE ON "fkl"."XDelete"
  FOR EACH ROW
  WHEN (OLD."cascade_ids" IS DISTINCT FROM NEW."cascade_ids")
  EXECUTE FUNCTION "fkl"."YT-XDelete-fk_XDelete__cascade_ids-A__id-CU-f3fd34-50a446"();
CREATE OR REPLACE FUNCTION "fkl"."YT-XDelete-fk_XDelete__null_ids-A__id-CI-d37b86"() RETURNS TRIGGER AS $$
DECLARE missing RECORD;
BEGIN
  IF array_length(array_remove(NEW."null_ids", NULL), 1)
      <> array_length((SELECT array_agg(DISTINCT ref_id) FROM unnest(array_remove(NEW."null_ids", NULL)) fkl(ref_id)), 1) THEN
      RAISE EXCEPTION 'ForeignKeyList items is not unique %', NEW
          USING ERRCODE = 'unique_violation';
  END IF;
  FOR missing IN SELECT ref_id
      FROM unnest(array_remove(NEW."null_ids", NULL)) fkl(ref_id)
      WHERE NOT EXISTS(SELECT 1 FROM "fkl"."A" WHERE "id" = fkl.ref_id)
  LOOP
      RAISE EXCEPTION 'ForeignKeyList missing entry "fkl"."A"."id"=%" %', missing.ref_id, NEW
          USING ERRCODE = 'foreign_key_violation';
  END LOOP;
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XDelete__null_ids-A__id-CI"
  BEFORE INSERT ON "fkl"."XDelete"
  FOR EACH ROW
  EXECUTE FUNCTION "fkl"."YT-XDelete-fk_XDelete__null_ids-A__id-CI-d37b86"();
CREATE OR REPLACE FUNCTION "fkl"."YT-XDelete-fk_XDelete__null_ids-A__id-CU-15562c-d37b86"() RETURNS TRIGGER AS $$
DECLARE missing RECORD;
BEGIN
  IF array_length(array_remove(NEW."null_ids", NULL), 1)
      <> array_length((SELECT array_agg(DISTINCT ref_id) FROM unnest(array_remove(NEW."null_ids", NULL)) fkl(ref_id)), 1) THEN
      RAISE EXCEPTION 'ForeignKeyList items is not unique %', NEW
          USING ERRCODE = 'unique_violation';
  END IF;
  FOR missing IN SELECT ref_id
      FROM unnest(array_remove(NEW."null_ids", NULL)) fkl(ref_id)
      WHERE NOT EXISTS(SELECT 1 FROM "fkl"."A" WHERE "id" = fkl.ref_id)
  LOOP
      RAISE EXCEPTION 'ForeignKeyList missing entry "fkl"."A"."id"=%" %', missing.ref_id, NEW
          USING ERRCODE = 'foreign_key_violation';
  END LOOP;
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XDelete__null_ids-A__id-CU"
  BEFORE UPDATE ON "fkl"."XDelete"
  FOR EACH ROW
  WHEN (OLD."null_ids" IS DISTINCT FROM NEW."null_ids")
  EXECUTE FUNCTION "fkl"."YT-XDelete-fk_XDelete__null_ids-A__id-CU-15562c-d37b86"();
CREATE INDEX "idx_XUpdate__restrict_ids" ON "fkl"."XUpdate" USING gin ("restrict_ids");
CREATE INDEX "idx_XUpdate__cascade_ids" ON "fkl"."XUpdate" USING gin ("cascade_ids");
CREATE OR REPLACE FUNCTION "fkl"."YT-XUpdate-fk_XUpdate__restrict_ids-A__id-CI-687eaa"() RETURNS TRIGGER AS $$
DECLARE missing RECORD;
BEGIN
  IF array_length(array_remove(NEW."restrict_ids", NULL), 1)
      <> array_length((SELECT array_agg(DISTINCT ref_id) FROM unnest(array_remove(NEW."restrict_ids", NULL)) fkl(ref_id)), 1) THEN
      RAISE EXCEPTION 'ForeignKeyList items is not unique %', NEW
          USING ERRCODE = 'unique_violation';
  END IF;
  FOR missing IN SELECT ref_id
      FROM unnest(array_remove(NEW."restrict_ids", NULL)) fkl(ref_id)
      WHERE NOT EXISTS(SELECT 1 FROM "fkl"."A" WHERE "id" = fkl.ref_id)
  LOOP
      RAISE EXCEPTION 'ForeignKeyList missing entry "fkl"."A"."id"=%" %', missing.ref_id, NEW
          USING ERRCODE = 'foreign_key_violation';
  END LOOP;
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XUpdate__restrict_ids-A__id-CI"
  BEFORE INSERT ON "fkl"."XUpdate"
  FOR EACH ROW
  EXECUTE FUNCTION "fkl"."YT-XUpdate-fk_XUpdate__restrict_ids-A__id-CI-687eaa"();
CREATE OR REPLACE FUNCTION "fkl"."YT-XUpdate-fk_XUpdate__restrict_ids-A__id-CU-3828be-687eaa"() RETURNS TRIGGER AS $$
DECLARE missing RECORD;
BEGIN
  IF array_length(array_remove(NEW."restrict_ids", NULL), 1)
      <> array_length((SELECT array_agg(DISTINCT ref_id) FROM unnest(array_remove(NEW."restrict_ids", NULL)) fkl(ref_id)), 1) THEN
      RAISE EXCEPTION 'ForeignKeyList items is not unique %', NEW
          USING ERRCODE = 'unique_violation';
  END IF;
  FOR missing IN SELECT ref_id
      FROM unnest(array_remove(NEW."restrict_ids", NULL)) fkl(ref_id)
      WHERE NOT EXISTS(SELECT 1 FROM "fkl"."A" WHERE "id" = fkl.ref_id)
  LOOP
      RAISE EXCEPTION 'ForeignKeyList missing entry "fkl"."A"."id"=%" %', missing.ref_id, NEW
          USING ERRCODE = 'foreign_key_violation';
  END LOOP;
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XUpdate__restrict_ids-A__id-CU"
  BEFORE UPDATE ON "fkl"."XUpdate"
  FOR EACH ROW
  WHEN (OLD."restrict_ids" IS DISTINCT FROM NEW."restrict_ids")
  EXECUTE FUNCTION "fkl"."YT-XUpdate-fk_XUpdate__restrict_ids-A__id-CU-3828be-687eaa"();
CREATE OR REPLACE FUNCTION "fkl"."YT-XUpdate-fk_XUpdate__cascade_ids-A__id-CI-50a446"() RETURNS TRIGGER AS $$
DECLARE missing RECORD;
BEGIN
  IF array_length(array_remove(NEW."cascade_ids", NULL), 1)
      <> array_length((SELECT array_agg(DISTINCT ref_id) FROM unnest(array_remove(NEW."cascade_ids", NULL)) fkl(ref_id)), 1) THEN
      RAISE EXCEPTION 'ForeignKeyList items is not unique %', NEW
          USING ERRCODE = 'unique_violation';
  END IF;
  FOR missing IN SELECT ref_id
      FROM unnest(array_remove(NEW."cascade_ids", NULL)) fkl(ref_id)
      WHERE NOT EXISTS(SELECT 1 FROM "fkl"."A" WHERE "id" = fkl.ref_id)
  LOOP
      RAISE EXCEPTION 'ForeignKeyList missing entry "fkl"."A"."id"=%" %', missing.ref_id, NEW
          USING ERRCODE = 'foreign_key_violation';
  END LOOP;
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XUpdate__cascade_ids-A__id-CI"
  BEFORE INSERT ON "fkl"."XUpdate"
  FOR EACH ROW
  EXECUTE FUNCTION "fkl"."YT-XUpdate-fk_XUpdate__cascade_ids-A__id-CI-50a446"();
CREATE OR REPLACE FUNCTION "fkl"."YT-XUpdate-fk_XUpdate__cascade_ids-A__id-CU-f3fd34-50a446"() RETURNS TRIGGER AS $$
DECLARE missing RECORD;
BEGIN
  IF array_length(array_remove(NEW."cascade_ids", NULL), 1)
      <> array_length((SELECT array_agg(DISTINCT ref_id) FROM unnest(array_remove(NEW."cascade_ids", NULL)) fkl(ref_id)), 1) THEN
      RAISE EXCEPTION 'ForeignKeyList items is not unique %', NEW
          USING ERRCODE = 'unique_violation';
  END IF;
  FOR missing IN SELECT ref_id
      FROM unnest(array_remove(NEW."cascade_ids", NULL)) fkl(ref_id)
      WHERE NOT EXISTS(SELECT 1 FROM "fkl"."A" WHERE "id" = fkl.ref_id)
  LOOP
      RAISE EXCEPTION 'ForeignKeyList missing entry "fkl"."A"."id"=%" %', missing.ref_id, NEW
          USING ERRCODE = 'foreign_key_violation';
  END LOOP;
  RETURN NEW;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "fk_XUpdate__cascade_ids-A__id-CU"
  BEFORE UPDATE ON "fkl"."XUpdate"
  FOR EACH ROW
  WHEN (OLD."cascade_ids" IS DISTINCT FROM NEW."cascade_ids")
  EXECUTE FUNCTION "fkl"."YT-XUpdate-fk_XUpdate__cascade_ids-A__id-CU-f3fd34-50a446"();"""
    await conn.execute(result)
    assert not await sync(conn, reg)


async def test_checks(conn, pgclean):
    reg = Registry()

    class A(Entity, registry=reg, schema="fkl"):
        id: Serial
        name: String

    class B(Entity, registry=reg, schema="fkl"):
        id: Serial
        a_ids: Auto = ForeignKeyList(A.id)
        # a_items: Many[A] # Maybe good

    result = await sync(conn, reg)
    await conn.execute(result)
    assert not await sync(conn, reg)

    a1 = A(id=1, name="Item 1")
    a2 = A(id=2, name="Item 2")
    a3 = A(id=3, name="Item 3")
    await conn.save(a1)
    await conn.save(a2)
    await conn.save(a3)

    b = B(a_ids=[1, 2, 3])
    await conn.save(b)

    b.a_ids = [1, 1]
    with pytest.raises(UniqueViolationError, match="ForeignKeyList items is not unique"):
        await conn.save(b)  # UPDATE

    b_not_unique = B(a_ids=[1, 2, 1])
    with pytest.raises(UniqueViolationError, match="ForeignKeyList items is not unique"):
        await conn.save(b_not_unique)  # INSERT

    b_missing = B(a_ids=[23])
    with pytest.raises(ForeignKeyViolationError, match='ForeignKeyList missing entry "fkl"."A"."id"=23"'):
        await conn.save(b_missing)  # INSERT

    b_missing = B(a_ids=[1])
    await conn.save(b_missing)
    b_missing.a_ids = [1, 32]
    with pytest.raises(ForeignKeyViolationError, match='ForeignKeyList missing entry "fkl"."A"."id"=32"'):
        await conn.save(b_missing)  # UPDATE

    b_empty = B()
    await conn.save(b_empty)


async def test_update_trigger(conn, pgclean):
    reg = Registry()

    class A(Entity, registry=reg, schema="fkl"):
        id: Serial
        name: String

    class B(Entity, registry=reg, schema="fkl"):
        id: Serial
        restrict_ids: Auto = ForeignKeyList(A.id, on_update="RESTRICT")
        cascade_ids: Auto = ForeignKeyList(A.id, on_update="CASCADE")
        # a_items: Many[A] # Maybe good

    result = await sync(conn, reg)
    await conn.execute(result)
    assert not await sync(conn, reg)

    a1 = A(id=1, name="Item 1")
    a2 = A(id=2, name="Item 2")
    a3 = A(id=3, name="Item 3")
    await conn.save(a1)
    await conn.save(a2)
    await conn.save(a3)

    b = B(restrict_ids=[1], cascade_ids=[2])
    await conn.save(b)

    with pytest.raises(ForeignKeyViolationError, match="ForeignKeyList prevent of record update, because has references"):
        await conn.execute('UPDATE "fkl"."A" SET "id" = 10 WHERE "id" = 1')

    await conn.execute('UPDATE "fkl"."A" SET "id" = 20 WHERE "id" = 2')
    b = await conn.select(Query(B).where(B.id == b.id)).first()
    assert b.restrict_ids == [1]
    assert b.cascade_ids == [20]


async def test_delete_trigger(conn, pgclean):
    reg = Registry()

    class A(Entity, registry=reg, schema="fkl"):
        id: Serial
        name: String

    class B(Entity, registry=reg, schema="fkl"):
        id: Serial
        restrict_ids: Auto = ForeignKeyList(A.id, on_delete="RESTRICT")
        cascade_ids: Auto = ForeignKeyList(A.id, on_delete="CASCADE")
        null_ids: Auto = ForeignKeyList(A.id, on_delete="SET NULL")
        # a_items: Many[A] # Maybe good

    result = await sync(conn, reg)
    await conn.execute(result)
    assert not await sync(conn, reg)

    a1 = A(id=1, name="Item 1")
    a2 = A(id=2, name="Item 2")
    a3 = A(id=3, name="Item 3")
    a4 = A(id=4, name="Item 3")
    await conn.save(a1)
    await conn.save(a2)
    await conn.save(a3)
    await conn.save(a4)

    b = B(restrict_ids=[1], cascade_ids=[2], null_ids=[3, 4])
    await conn.save(b)

    with pytest.raises(ForeignKeyViolationError, match="ForeignKeyList prevent of record delete, because has references"):
        await conn.delete(a1)

    await conn.delete(a2)
    b = await conn.select(Query(B).where(B.id == b.id)).first()
    assert b.restrict_ids == [1]
    assert b.cascade_ids == []
    assert b.null_ids == [3, 4]

    await conn.delete(a3)
    b = await conn.select(Query(B).where(B.id == b.id)).first()
    assert b.restrict_ids == [1]
    assert b.cascade_ids == []
    assert b.null_ids == [None, 4]

    await conn.delete(a4)
    b = await conn.select(Query(B).where(B.id == b.id)).first()
    assert b.restrict_ids == [1]
    assert b.cascade_ids == []
    assert b.null_ids == [None, None]
