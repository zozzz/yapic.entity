from enum import Enum, Flag

from yapic.entity import Entity, Int, Serial, String, Choice, Field, PrimaryKey, ForeignKey, Date, DateTime, DateTimeTz, Bool, func
from yapic.sql import PostgreDialect

dialect = PostgreDialect()
ddl = dialect.create_ddl_compiler()


def test_basic():
    class User(Entity):
        pass

    class User2(Entity, name="__user__"):
        pass

    class User3(Entity, schema="auth"):
        pass

    class User4(Entity, schema="auth", name="userX"):
        pass

    result = ddl.compile_entity(User)
    assert result == """CREATE TABLE "User" (
);"""

    result = ddl.compile_entity(User2)
    assert result == """CREATE TABLE "__user__" (
);"""

    result = ddl.compile_entity(User3)
    assert result == """CREATE TABLE "auth"."User3" (
);"""

    result = ddl.compile_entity(User4)
    assert result == """CREATE TABLE "auth"."userX" (
);"""


def test_int():
    class Ints(Entity):
        int_small: Int = Field(size=2)
        int_medium: Int = Field(size=4)
        int_large: Int = Field(size=8)

    result = ddl.compile_entity(Ints)
    assert result == """CREATE TABLE "Ints" (
  "int_small" INT2,
  "int_medium" INT4,
  "int_large" INT8
);"""

    class Pk_small(Entity):
        id: Int = Field(size=2) // PrimaryKey(auto_increment=True)
        id2: Serial = Field(size=2)

    class Pk_medium(Entity):
        id: Int = Field(size=4) // PrimaryKey(auto_increment=True)
        id2: Serial = Field(size=4)

    class Pk_large(Entity):
        id: Int = Field(size=8) // PrimaryKey(auto_increment=True)
        id2: Serial = Field(size=8)

    result = ddl.compile_entity(Pk_small)
    assert result == """CREATE TABLE "Pk_small" (
  "id" SERIAL2 PRIMARY KEY NOT NULL,
  "id2" SERIAL2 PRIMARY KEY NOT NULL
);"""

    result = ddl.compile_entity(Pk_medium)
    assert result == """CREATE TABLE "Pk_medium" (
  "id" SERIAL4 PRIMARY KEY NOT NULL,
  "id2" SERIAL4 PRIMARY KEY NOT NULL
);"""

    result = ddl.compile_entity(Pk_large)
    assert result == """CREATE TABLE "Pk_large" (
  "id" SERIAL8 PRIMARY KEY NOT NULL,
  "id2" SERIAL8 PRIMARY KEY NOT NULL
);"""


def test_string():
    class A(Entity):
        id: String = Field(size=[10, 10]) // PrimaryKey()
        name: String = Field(size=50)
        email: String = Field(size=50)
        description: String

    result = ddl.compile_entity(A)
    assert result == """CREATE TABLE "A" (
  "id" CHAR(10) PRIMARY KEY NOT NULL,
  "name" VARCHAR(50),
  "email" VARCHAR(50),
  "description" TEXT
);"""


def test_enum():
    class Mood(Enum):
        SAD = "sad"
        OK = "ok"
        HAPPY = "happy"

    class Color(Flag):
        RED = 1
        GREEN = 2
        BLUE = 4

    class A(Entity):
        mood: Choice[Mood]
        colors: Choice[Color]

    result = ddl.compile_entity(A)
    assert result == """CREATE TABLE "A" (
  "mood" VARCHAR(5) CHECK("mood" IN ('sad', 'ok', 'happy')),
  "colors" BIT(3)
);"""


def test_fk():
    class A(Entity):
        id: Serial

    class X(Entity, schema="x_schema", name="y"):
        id: Serial

    class B(Entity):
        id: Serial
        id_a: Int = ForeignKey(A.id)
        id_x: Int = ForeignKey(X.id)

    result = ddl.compile_entity(B)
    assert result == """CREATE TABLE "B" (
  "id" SERIAL PRIMARY KEY NOT NULL,
  "id_a" INT,
  "id_x" INT,
  CONSTRAINT "fk_B__id_a-A__id" FOREIGN KEY ("id_a") REFERENCES "A" ("id"),
  CONSTRAINT "fk_B__id_x-y__id" FOREIGN KEY ("id_x") REFERENCES "x_schema"."y" ("id")
);"""


def test_date():
    class A(Entity):
        date: Date
        date_time: DateTime
        date_time_tz: DateTimeTz = func.now()

    result = ddl.compile_entity(A)
    assert result == """CREATE TABLE "A" (
  "date" DATE,
  "date_time" TIMESTAMP,
  "date_time_tz" TIMESTAMPTZ NOT NULL DEFAULT now()
);"""


def test_bool():
    class A(Entity):
        is_active: Bool = True

    result = ddl.compile_entity(A)
    assert result == """CREATE TABLE "A" (
  "is_active" BOOLEAN NOT NULL DEFAULT 1
);"""
