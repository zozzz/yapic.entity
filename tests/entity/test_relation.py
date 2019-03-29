import pytest

from yapic.entity import (
    Entity,
    String,
    Int,
    One,
    Many,
    ManyAcross,
    Relation,
    Index,
    ForeignKey,
)
from yapic.entity._relation import RelatedList


class GlobalA(Entity):
    id: Int
    name: String
    b_list: ManyAcross["AcrossAB", "GlobalB"]


class GlobalB(Entity):
    id: Int
    name: String
    a_list: ManyAcross["AcrossAB", "GlobalA"]


class AcrossAB(Entity):
    id_a: Int = ForeignKey(GlobalA.id)
    id_b: Int = ForeignKey(GlobalB.id)


def test_relation_one():
    class B(Entity):
        id_a: Int = ForeignKey(GlobalA.id)
        n_one: One[GlobalA]
        fw_one: One["GlobalA"]

    assert isinstance(B.n_one, Relation)
    assert B.n_one.__impl__.joined is GlobalA
    assert isinstance(B.fw_one, Relation)
    assert B.fw_one.__impl__.joined is GlobalA

    binst = B()
    assert binst.n_one is None
    binst.n_one = "Something wrong"
    assert binst.n_one == "Something wrong"

    assert binst.fw_one is None
    binst.fw_one = "VALUE"
    assert binst.fw_one == "VALUE"
    binst.fw_one = "VALUE2"
    assert binst.fw_one == "VALUE2"


def test_relation_many():
    class B(Entity):
        id_a: Int = ForeignKey(GlobalA.id)
        n_many: Many[GlobalA]
        fw_many: Many["GlobalA"]

    assert isinstance(B.n_many, Relation)
    assert B.n_many.__impl__.joined is GlobalA
    assert isinstance(B.fw_many, Relation)
    assert B.fw_many.__impl__.joined is GlobalA

    binst = B()
    assert isinstance(binst.n_many, RelatedList)
    assert isinstance(binst.n_many.__removed__, list)
    assert isinstance(binst.n_many.__added__, list)

    binst.n_many.append(42)
    assert binst.n_many.__added__[0] == 42
    assert len(binst.n_many.__removed__) == 0
    del binst.n_many[0]
    assert len(binst.n_many.__added__) == 0
    assert len(binst.n_many.__removed__) == 1
    assert binst.n_many.__removed__[0] == 42

    binst.n_many.reset()
    assert len(binst.n_many.__added__) == 0
    assert len(binst.n_many.__removed__) == 0

    binst.n_many = [4, 6]
    assert len(binst.n_many.__added__) == 2
    assert len(binst.n_many.__removed__) == 0

    assert list(binst.n_many) == [4, 6]

    binst.n_many = [10, 11, 12]
    assert len(binst.n_many.__added__) == 3
    assert len(binst.n_many.__removed__) == 2

    assert binst.n_many.__added__ == [10, 11, 12]
    assert binst.n_many.__removed__ == [4, 6]

    binst.n_many.append(13)
    binst.n_many.append(14)
    binst.n_many.append(15)

    assert binst.n_many.__added__ == [10, 11, 12, 13, 14, 15]
    assert binst.n_many.__removed__ == [4, 6]

    binst.n_many[2:5] = [100, 101, 102, 103]

    assert list(binst.n_many) == [10, 11, 100, 101, 102, 103, 15]
    assert binst.n_many.__added__ == [10, 11, 15, 100, 101, 102, 103]
    assert binst.n_many.__removed__ == [4, 6, 12, 13, 14]

    binst.fw_many = (1, 2, 3)
    assert list(binst.fw_many) == [1, 2, 3]
    del binst.fw_many
    assert list(binst.fw_many) == []
    assert binst.fw_many.__removed__ == [1, 2, 3]

    b2 = B()
    assert list(b2.n_many) == []
    assert list(b2.n_many.__added__) == []
    assert list(b2.n_many.__removed__) == []
    assert list(b2.fw_many) == []
    assert list(b2.fw_many.__added__) == []
    assert list(b2.fw_many.__removed__) == []

    assert (
        repr(B.n_many.__impl__.join_expr)
        == "<Expr <Field id_a: Int of <Entity B>> <built-in function eq> <Field id: Int of <Entity GlobalA>>>"
    )


def test_relation_many_many():
    rel = GlobalA.b_list
    across_join_expr = rel.__impl__.across_join_expr
    join_expr = rel.__impl__.join_expr

    assert (
        repr(across_join_expr)
        == "<Expr <Field id_a: Int of <Entity AcrossAB>> <built-in function eq> <Field id: Int of <Entity GlobalA>>>"
    )
    assert (
        repr(join_expr)
        == "<Expr <Field id_b: Int of <Entity AcrossAB>> <built-in function eq> <Field id: Int of <Entity GlobalB>>>"
    )


def test_fk_field():
    class B(Entity):
        id: Int
        fk_1: Int = ForeignKey(GlobalA.id)
        fk_2: Int = ForeignKey("GlobalA.id")

    fk1 = B.fk_1.extensions[0]
    fk2 = B.fk_2.extensions[0]

    assert isinstance(fk1, ForeignKey)
    assert isinstance(fk2, ForeignKey)

    assert fk1.ref is GlobalA.id
    assert fk2.ref is GlobalA.id
