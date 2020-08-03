import operator
import pytest
from typing import Any

from yapic.entity.sql import wrap_connection, Entity, sync
from yapic.entity import (Field, Serial, Int, String, Bytes, Date, DateTime, DateTimeTz, Time, TimeTz, Bool, ForeignKey,
                          PrimaryKey, One, Query, func, EntityDiff, Registry, Json, JsonArray, Composite, Auto, Numeric,
                          Float, Point, UUID, virtual)

pytestmark = pytest.mark.asyncio

REGISTRY = Registry()


class Document(Entity, schema="circular_deps", registry=REGISTRY):
    id: Serial
    group_id: Auto = Field(nullable=False) // ForeignKey("DocumentGroup.id")
    group: One["DocumentGroup"]
    author_id: Auto = ForeignKey("circular_deps.User.id")
    author: One["circular_deps.User"]


class DocumentGroup(Entity, schema="circular_deps", registry=REGISTRY):
    id: Serial
    primary_document_id: Int = ForeignKey(Document.id)
    primary_document: One[Document] = "DocumentGroup.primary_document_id == Document.id"


class __User(Entity, name="User", schema="circular_deps", registry=REGISTRY):
    id: Serial
    name: String
    contract_id: Auto = ForeignKey(Document.id)
    contract: One[Document]
    profile_image_id: Auto = ForeignKey("circular_deps.File.id", on_delete="SET NULL")
    profile_image: One["circular_deps.File"]


class A(Entity, schema="circular_deps", registry=REGISTRY):
    id: Serial
    b_id: Int = ForeignKey("circular_deps.B.id")
    b: One["B"] = "B.id == A.b_id"


class B(Entity, schema="circular_deps", registry=REGISTRY):
    id: Serial
    c_id: Int = ForeignKey("circular_deps.C.id")


class C(Entity, schema="circular_deps", registry=REGISTRY):
    id: Serial
    a_id: Int = ForeignKey("circular_deps.A.id")
    a: One[A] = "A.id == C.a_id"


class Node(Entity, schema="circular_deps", registry=REGISTRY, polymorph="type"):
    id: Serial
    type: String = Field(nullable=False)
    parent_id: Auto = ForeignKey("Node.id")
    author_id: Auto = ForeignKey("circular_deps.User.id")
    author: One["circular_deps.User"]

    # TODO: Windows fatal exception: access violation
    # author: One["User"]


class File(Node, polymorph_id="file"):
    file_name: String


class Dir(Node, polymorph_id="dir"):
    dir_name: String


@pytest.yield_fixture
async def conn(pgsql):
    yield wrap_connection(pgsql, "pgsql")


async def test_sync(conn, pgclean):
    result = await sync(conn, REGISTRY)

    assert result == """CREATE SCHEMA IF NOT EXISTS "circular_deps";
CREATE SEQUENCE "circular_deps"."A_id_seq";
CREATE SEQUENCE "circular_deps"."B_id_seq";
CREATE SEQUENCE "circular_deps"."C_id_seq";
CREATE TABLE "circular_deps"."C" (
  "id" INT4 NOT NULL DEFAULT nextval('"circular_deps"."C_id_seq"'::regclass),
  "a_id" INT4,
  PRIMARY KEY("id")
);
CREATE TABLE "circular_deps"."B" (
  "id" INT4 NOT NULL DEFAULT nextval('"circular_deps"."B_id_seq"'::regclass),
  "c_id" INT4,
  PRIMARY KEY("id")
);
CREATE TABLE "circular_deps"."A" (
  "id" INT4 NOT NULL DEFAULT nextval('"circular_deps"."A_id_seq"'::regclass),
  "b_id" INT4,
  PRIMARY KEY("id")
);
CREATE SEQUENCE "circular_deps"."Node_id_seq";
CREATE SEQUENCE "circular_deps"."DocumentGroup_id_seq";
CREATE TABLE "circular_deps"."DocumentGroup" (
  "id" INT4 NOT NULL DEFAULT nextval('"circular_deps"."DocumentGroup_id_seq"'::regclass),
  "primary_document_id" INT4,
  PRIMARY KEY("id")
);
CREATE SEQUENCE "circular_deps"."Document_id_seq";
CREATE TABLE "circular_deps"."Document" (
  "id" INT4 NOT NULL DEFAULT nextval('"circular_deps"."Document_id_seq"'::regclass),
  "group_id" INT4 NOT NULL,
  "author_id" INT4,
  PRIMARY KEY("id")
);
CREATE TABLE "circular_deps"."Node" (
  "id" INT4 NOT NULL DEFAULT nextval('"circular_deps"."Node_id_seq"'::regclass),
  "type" TEXT NOT NULL,
  "parent_id" INT4,
  "author_id" INT4,
  PRIMARY KEY("id")
);
CREATE TABLE "circular_deps"."File" (
  "id" INT4 NOT NULL,
  "file_name" TEXT,
  PRIMARY KEY("id")
);
CREATE SEQUENCE "circular_deps"."User_id_seq";
CREATE TABLE "circular_deps"."User" (
  "id" INT4 NOT NULL DEFAULT nextval('"circular_deps"."User_id_seq"'::regclass),
  "name" TEXT,
  "contract_id" INT4,
  "profile_image_id" INT4,
  PRIMARY KEY("id")
);
CREATE TABLE "circular_deps"."Dir" (
  "id" INT4 NOT NULL,
  "dir_name" TEXT,
  PRIMARY KEY("id")
);
CREATE INDEX "idx_C__a_id" ON "circular_deps"."C" USING btree ("a_id");
ALTER TABLE "circular_deps"."C"
  ADD CONSTRAINT "fk_C__a_id-A__id" FOREIGN KEY ("a_id") REFERENCES "circular_deps"."A" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
CREATE INDEX "idx_B__c_id" ON "circular_deps"."B" USING btree ("c_id");
ALTER TABLE "circular_deps"."B"
  ADD CONSTRAINT "fk_B__c_id-C__id" FOREIGN KEY ("c_id") REFERENCES "circular_deps"."C" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
CREATE INDEX "idx_A__b_id" ON "circular_deps"."A" USING btree ("b_id");
ALTER TABLE "circular_deps"."A"
  ADD CONSTRAINT "fk_A__b_id-B__id" FOREIGN KEY ("b_id") REFERENCES "circular_deps"."B" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
CREATE INDEX "idx_DocumentGroup__primary_document_id" ON "circular_deps"."DocumentGroup" USING btree ("primary_document_id");
ALTER TABLE "circular_deps"."DocumentGroup"
  ADD CONSTRAINT "fk_DocumentGroup__primary_document_id-Document__id" FOREIGN KEY ("primary_document_id") REFERENCES "circular_deps"."Document" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
CREATE INDEX "idx_Document__group_id" ON "circular_deps"."Document" USING btree ("group_id");
CREATE INDEX "idx_Document__author_id" ON "circular_deps"."Document" USING btree ("author_id");
ALTER TABLE "circular_deps"."Document"
  ADD CONSTRAINT "fk_Document__group_id-DocumentGroup__id" FOREIGN KEY ("group_id") REFERENCES "circular_deps"."DocumentGroup" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  ADD CONSTRAINT "fk_Document__author_id-User__id" FOREIGN KEY ("author_id") REFERENCES "circular_deps"."User" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
CREATE INDEX "idx_Node__parent_id" ON "circular_deps"."Node" USING btree ("parent_id");
CREATE INDEX "idx_Node__author_id" ON "circular_deps"."Node" USING btree ("author_id");
ALTER TABLE "circular_deps"."Node"
  ADD CONSTRAINT "fk_Node__parent_id-Node__id" FOREIGN KEY ("parent_id") REFERENCES "circular_deps"."Node" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  ADD CONSTRAINT "fk_Node__author_id-User__id" FOREIGN KEY ("author_id") REFERENCES "circular_deps"."User" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE "circular_deps"."File"
  ADD CONSTRAINT "fk_File__id-Node__id" FOREIGN KEY ("id") REFERENCES "circular_deps"."Node" ("id") ON UPDATE CASCADE ON DELETE CASCADE;
CREATE OR REPLACE FUNCTION "circular_deps"."YT-File-polyd_Node"() RETURNS TRIGGER AS $$ BEGIN
  DELETE FROM "circular_deps"."Node" "parent" WHERE "parent"."id"=OLD."id";
  RETURN OLD;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "polyd_Node"
  AFTER DELETE ON "circular_deps"."File"
  FOR EACH ROW
  EXECUTE FUNCTION "circular_deps"."YT-File-polyd_Node"();
CREATE INDEX "idx_User__contract_id" ON "circular_deps"."User" USING btree ("contract_id");
CREATE INDEX "idx_User__profile_image_id" ON "circular_deps"."User" USING btree ("profile_image_id");
ALTER TABLE "circular_deps"."User"
  ADD CONSTRAINT "fk_User__contract_id-Document__id" FOREIGN KEY ("contract_id") REFERENCES "circular_deps"."Document" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  ADD CONSTRAINT "fk_User__profile_image_id-File__id" FOREIGN KEY ("profile_image_id") REFERENCES "circular_deps"."File" ("id") ON UPDATE RESTRICT ON DELETE SET NULL;
ALTER TABLE "circular_deps"."Dir"
  ADD CONSTRAINT "fk_Dir__id-Node__id" FOREIGN KEY ("id") REFERENCES "circular_deps"."Node" ("id") ON UPDATE CASCADE ON DELETE CASCADE;
CREATE OR REPLACE FUNCTION "circular_deps"."YT-Dir-polyd_Node"() RETURNS TRIGGER AS $$ BEGIN
  DELETE FROM "circular_deps"."Node" "parent" WHERE "parent"."id"=OLD."id";
  RETURN OLD;
END; $$ language 'plpgsql' ;
CREATE TRIGGER "polyd_Node"
  AFTER DELETE ON "circular_deps"."Dir"
  FOR EACH ROW
  EXECUTE FUNCTION "circular_deps"."YT-Dir-polyd_Node"();"""

    await conn.conn.execute(result)


async def test_exec(conn):
    user = __User(name="Test User")
    await conn.save(user)

    doc_group = DocumentGroup()
    await conn.save(doc_group)

    doc = Document(author=user, group=doc_group)
    await conn.save(doc)

    user.contract_id = doc.id
    await conn.save(user)

    user = await conn.select(Query(__User).where(__User.id == user.id).load(__User, __User.contract)).first()

    assert user.name == "Test User"
    assert user.contract.id == doc.id

    b = A(id=3)
    await conn.save(b)

    a = A(id=1, b=b)
    await conn.save(a)

    c = C(id=2, a=a)
    await conn.save(c)

    cq = await conn.select(Query(C).load(C, C.a).where(C.id == 2)).first()

    assert cq.id == 2
    assert cq.a.id == 1


async def test_poly_circular(conn):
    file = File()
    file.file_name = "Test"
    await conn.save(file)

    fq = await conn.select(Query(File).where(File.id == file.id)).first()
    assert fq.id == file.id
    assert fq.type == "file"
    assert fq.file_name == "Test"
