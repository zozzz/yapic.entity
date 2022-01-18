import cython
from enum import Enum

from ._entity cimport EntityType
from ._field cimport Field, AutoIncrement, ForeignKey
from ._expression cimport Expression


class EntityDiffKind(Enum):
    REMOVED = 1
    CREATED = 2
    CHANGED = 3
    RENAMED = 4
    REMOVE_PK = 5
    CREATE_PK = 6
    REMOVE_EXTGROUP = 7
    CREATE_EXTGROUP = 8
    CREATE_TRIGGER = 9
    REMOVE_TRIGGER = 10


@cython.final
cdef class EntityDiff:
    def __cinit__(self, EntityType a, EntityType b, object expression_eq=None):
        cdef Field field
        self.a = a
        self.b = b
        self.changes = []

        extgroup_changes = compare_extgroups(a, b)
        if extgroup_changes:
            self.changes.extend(extgroup_changes[0])

        a_pk_names = {field._name_ for field in a.__pk__}
        b_pk_names = {field._name_ for field in b.__pk__}

        if a_pk_names != b_pk_names:
            recreate_pk = True
            if a.__pk__:
                self.changes.append((EntityDiffKind.REMOVE_PK, a))
        else:
            recreate_pk = False

        a_fields = {field._name_: field for field in a.__fields__ if not field._virtual_}
        b_fields = {field._name_: field for field in b.__fields__ if not field._virtual_}
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

        if recreate_pk:
            if b.__pk__:
                self.changes.append((EntityDiffKind.CREATE_PK, b))

        if extgroup_changes:
            self.changes.extend(extgroup_changes[1])

        a_triggers = {trigger.name: trigger for trigger in a.__triggers__}
        b_triggers = {trigger.name: trigger for trigger in b.__triggers__}
        a_trigger_names = set(a_triggers.keys())
        b_trigger_names = set(b_triggers.keys())

        t_removed = a_trigger_names - b_trigger_names
        for trigger in sorted([a_triggers[k] for k in t_removed], key=lambda t: t.name):
            self.changes.append((EntityDiffKind.REMOVE_TRIGGER, (a, trigger)))

        t_created = b_trigger_names - a_trigger_names
        for trigger in sorted([b_triggers[k] for k in t_created], key=lambda t: t.name):
            self.changes.append((EntityDiffKind.CREATE_TRIGGER, (b, trigger)))

        t_maybe_changed = a_trigger_names & b_trigger_names
        for trigger_a, trigger_b in sorted([(a_triggers[k], b_triggers[k]) for k in t_maybe_changed], key=lambda t: t[0].name):
            if not trigger_a.is_eq(b, trigger_b):
                self.changes.append((EntityDiffKind.REMOVE_TRIGGER, (a, trigger_a)))
                self.changes.append((EntityDiffKind.CREATE_TRIGGER, (b, trigger_b)))

        # print("\n".join(map(repr, self.changes)))

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

    a_ac = a.get_ext(AutoIncrement)
    b_ac = b.get_ext(AutoIncrement)

    if a_ac is not None or b_ac is not None:
        if a_ac is not None and b_ac is not None:
            if a_ac != b_ac:
                result["_default_"] = b_ac
        elif a_ac is not None:
            result["_default_"] = None
        else:
            result["_default_"] = b_ac
    elif not isinstance(a._default_, Expression) and not isinstance(b._default_, Expression):
        if not callable(a._default_) and not callable(b._default_):
            if a._default_ != b._default_:
                result["_default_"] = b._default_
        elif (a._default_ is None and callable(b._default_)) \
                or (callable(a._default_) and b._default_ is None) \
                or (callable(a._default_) and callable(b._default_)):
            # default is not changed on db level
            pass
        else:
            result["_default_"] = b._default_
    elif not isinstance(a._default_, Expression) or not isinstance(b._default_, Expression):
        result["_default_"] = b._default_
    elif expression_eq is not None and not expression_eq(a._default_, b._default_):
        result["_default_"] = b._default_



    # exts = compare_exts(a._exts_, b._exts_)
    # if exts:
    #     result["_exts_"] = exts

    return result


cdef inline list compare_exts(list a, list b):
    # exts_a = set(a)
    # exts_b = set(b)

    # removed = [() for x in exts_a - exts_b]
    # created = [() for x in exts_b - exts_a]

    # if removed or created:
    #     return (removed, created)
    # else:
    #     return None
    return None



# cdef inline tuple compare_fks(EntityType a, EntityType b):
#     removed = []
#     created = []

#     a_fks = set(filter(lambda v: v.type is ForeignKey, a.__extgroups__))
#     b_fks = set(filter(lambda v: v.type is ForeignKey, b.__extgroups__))

#     removed = [(EntityDiffKind.REMOVE_EXTGROUP, (x[0].name, x)) for x in a_fks - b_fks]
#     created = [(EntityDiffKind.CREATE_EXTGROUP, (x[0].name, x)) for x in b_fks - a_fks]

#     if removed or created:
#         return (removed, created)
#     else:
#         return None


cdef inline tuple compare_extgroups(EntityType a, EntityType b):
    a_groups = set(a.__extgroups__)
    b_groups = set(b.__extgroups__)

    removed = [(EntityDiffKind.REMOVE_EXTGROUP, x) for x in a_groups - b_groups]
    created = [(EntityDiffKind.CREATE_EXTGROUP, x) for x in b_groups - a_groups]

    if removed or created:
        return (removed, created)
    else:
        return None
