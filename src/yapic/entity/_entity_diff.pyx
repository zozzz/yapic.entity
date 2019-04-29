import cython
from enum import Enum

from ._entity cimport EntityType
from ._field cimport Field
from ._expression cimport Expression


class EntityDiffKind(Enum):
    REMOVED = 1
    CREATED = 2
    CHANGED = 3
    RENAMED = 4


@cython.final
cdef class EntityDiff:
    def __cinit__(self, EntityType a, EntityType b, object expression_eq=None):
        self.a = a
        self.b = b
        self.changes = []

        a_fields = {f._name_: f for f in a.__fields__}
        b_fields = {f._name_: f for f in b.__fields__}
        a_field_names = set(a_fields.keys())
        b_field_names = set(b_fields.keys())

        removed = a_field_names - b_field_names
        for r in sorted([a_fields[n] for n in removed], key=lambda f: f._index_):
            self.changes.append((EntityDiffKind.REMOVED, r))

        created = b_field_names - a_field_names
        for r in sorted([b_fields[n] for n in created], key=lambda f: f._index_):
            self.changes.append((EntityDiffKind.CREATED, r))

        cdef Field a_field
        cdef Field b_field

        maybe_changed = b_field_names & a_field_names
        for r in sorted(maybe_changed, key=lambda n: b_fields[n]._index_):
            a_field = a_fields[r]
            b_field = b_fields[r]

            changed = field_eq(a_field, b_field, expression_eq)
            if changed:
                self.changes.append((EntityDiffKind.CHANGED, (a_field, b_field, changed)))

    def __bool__(self):
        return len(self.changes) > 0

    def __len__(self):
        return len(self.changes)

    def __iter__(self):
        return iter(self.changes)


cdef inline dict field_eq(Field a, Field b, object expression_eq):
    result = {}

    # XXX: implement field order change in pgsql
    # if a._index_ != b._index_:
    #     result["_index_"] = b._index_

    if a._name_ != b._name_:
        result["_name_"] = b._name_

    if a._impl_ != b._impl_:
        result["_impl_"] = b._impl_

    if a.min_size != b.min_size or a.max_size != b.max_size:
        result["size"] = [b.min_size, b.max_size]

    if a.nullable is not b.nullable:
        result["nullable"] = b.nullable

    if not isinstance(a._default_, Expression) and not isinstance(b._default_, Expression):
        if not callable(a._default_) and not callable(b._default_):
            if a._default_ != b._default_:
                result["_default_"] = b._default_
        elif callable(a._default_) or callable(b._default_):
            result["_default_"] = b._default_
    elif not isinstance(a._default_, Expression) or not isinstance(b._default_, Expression):
        result["_default_"] = b._default_
    elif expression_eq is not None and not expression_eq(a._default_, b._default_):
        result["_default_"] = b._default_

    exts = compare_exts(a._exts_, b._exts_)
    if exts:
        result["_exts_"] = exts

    return result


cdef inline list compare_exts(list a, list b):
    return []

