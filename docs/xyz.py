from typing import Generic, TypeVar

T = TypeVar("T")


class XYZ(Generic[T]):
    vaa: T

    def __init__(self, v: T):
        pass


class BAKTER:
    pass


CCC = XYZ["BAKTER"]
