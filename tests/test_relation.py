import pytest

from yapic.entity import Entity, String, Int, One, Many, ManyAcross, Relation


class GlobalA(Entity):
    id: Int
    name: String


def test_relation_one():
    class B(Entity):
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
