import cython
from enum import Enum
from weakref import WeakValueDictionary

from ._entity cimport DependencyList, EntityBase, EntityType, EntityAttribute, EntityAttributeExt, EntityAttributeImpl, NOTSET
from ._entity_diff cimport EntityDiff
from ._field cimport ForeignKey


@cython.final
cdef class Registry:
    def __cinit__(self):
        # self.entities = WeakValueDictionary()
        self.entities = {}
        self.locals = {}
        self.deferred = []

    cpdef object register(self, str name, EntityType entity):
        if name in self.entities:
            raise ValueError("entity already registered: %r" % entity)
        else:
            insert_local_ref(self.locals, name, entity)

            self.entities[name] = entity
            self.deferred.append(entity)

            self.resolve_deferred()

    def __getitem__(self, str name):
        return self.entities[name]

    def __iter__(self):
        return iter(self.entities)

    cpdef keys(self):
        return self.entities.keys()

    cpdef values(self):
        return self.entities.values()

    cpdef items(self):
        return self.entities.items()

    cpdef filter(self, fn):
        cdef dict res = {}
        for k, v in self.entities.items():
            if fn(v):
                res[k] = v
        reg = Registry()
        reg.entities = res
        return reg

    cpdef list get_foreign_key_refs(self, EntityAttribute column):
        cdef list result = []
        cdef list per_entity
        cdef EntityType entity
        cdef EntityAttribute field
        cdef EntityAttributeExt ext
        cdef ForeignKey fk

        for entity in self.entities.values():
            per_entity = []

            for field in entity.__fields__:
                for ext in field._exts_:
                    if isinstance(ext, ForeignKey):
                        fk = <ForeignKey>ext
                        if fk.ref._entity_ is column._entity_ and fk.ref._name_ == column._name_:
                            per_entity.append(field._key_)

            if len(per_entity) != 0:
                result.append((entity, per_entity))

        return result


    # cdef resolve_deferred(self):
    #     cdef EntityType entity
    #     cdef list deferred = self.deferred
    #     cdef int index = len(deferred) - 1

    #     while index >= 0:
    #         entity = deferred[index]
    #         if entity.resolve_deferred():
    #             deferred.pop(index)

    #         index -= 1
    cdef resolve_deferred(self):
        cdef EntityType entity
        cdef list deferred = self.deferred
        cdef int lastLen = -1
        cdef int length
        cdef int i

        while True:
            length = len(deferred)
            if length == 0 or length == lastLen:
                break
            lastLen = length

            length -= 1
            while length >= 0:
                entity = deferred[length]
                if entity.resolve_deferred():
                    deferred.pop(length)

                length -= 1



class RegistryDiffKind(Enum):
    REMOVED = 1
    CREATED = 2
    CHANGED = 3
    INSERT_ENTITY = 4
    UPDATE_ENTITY = 5
    REMOVE_ENTITY = 6
    COMPARE_DATA = 7


@cython.final
cdef class RegistryDiff:
    def __cinit__(self, Registry a, Registry b, object entity_diff):
        a = a.filter(skip_virtual)
        b = b.filter(skip_virtual)
        self.a = a
        self.b = b
        self.changes = []
        cdef DependencyList order = DependencyList()
        cdef EntityBase fix

        a_names = set(a.keys())
        b_names = set(b.keys())

        for removed in sorted(a_names - b_names):
            val = a[removed]
            self.changes.append((RegistryDiffKind.REMOVED, val))
            order.add(val)

        for created in sorted(b_names - a_names):
            val = b[created]
            self.changes.append((RegistryDiffKind.CREATED, val))
            order.add(val)

            if val.__fix_entries__:
                for fix in val.__fix_entries__:
                    self.changes.append((RegistryDiffKind.INSERT_ENTITY, fix))

        for maybe_changed in sorted(a_names & b_names):
            val = b[maybe_changed]
            diff = entity_diff(a[maybe_changed], val)
            if diff:
                self.changes.append((RegistryDiffKind.CHANGED, diff))
                order.add(val)

            if val.__fix_entries__:
                self.changes.append((RegistryDiffKind.COMPARE_DATA, (a[maybe_changed], val)))
                order.add(val)

        def key(item):
            kind, val = item
            if isinstance(val, EntityDiff):
                return order.index((<EntityDiff>val).b)
            elif kind is RegistryDiffKind.INSERT_ENTITY:
                return order.index(type(val))
            elif kind is RegistryDiffKind.COMPARE_DATA:
                return order.index(val[1])
            else:
                return order.index(val)

        self.changes.sort(key=key)


    def __bool__(self):
        return len(self.changes) > 0

    def __len__(self):
        return len(self.changes)

    def __iter__(self):
        return iter(self.changes)

    cpdef list compare_data(self, list a_ents, list b_ents):
        cdef list result = []
        a_values = set(a_ents)
        b_values = set(b_ents)

        for removed in a_values - b_values:
            result.append((RegistryDiffKind.REMOVE_ENTITY, removed))

        for created in b_values - a_values:
            result.append((RegistryDiffKind.INSERT_ENTITY, created))

        for i in sorted(map(b_ents.index, a_values & b_values)):
            b_ent = b_ents[i]
            a_ent = a_ents[a_ents.index(b_ent)]
            if not entity_data_is_eq(a_ent, b_ent):
                result.append((RegistryDiffKind.UPDATE_ENTITY, b_ent))

        return result


def skip_virtual(EntityType ent):
    return ent.__meta__.get("is_virtual", False) is not True


cdef object entity_data_is_eq(EntityBase a, EntityBase b):
    cdef EntityType entity_a = type(a)
    cdef EntityType entity_b = type(b)
    cdef EntityAttribute attr
    cdef EntityAttribute attr2

    field_names_a = {attr._name_ for attr in entity_a.__fields__}
    field_names_b = {attr._name_ for attr in entity_b.__fields__}

    if field_names_a.symmetric_difference(field_names_b):
        return False

    cdef int length = len(entity_b.__fields__)

    for i in range(length):
        attr = <EntityAttribute>entity_b.__fields__[i]
        attr2 = getattr(entity_a, attr._key_, None)
        if attr2 is None:
            return False

        iv = a.__state__.get_value(attr2)
        cv = b.__state__.get_value(attr)
        nv = (<EntityAttributeImpl>attr._impl_).state_get_dirty(iv, cv)

        if nv is NOTSET:
            continue
        else:
            return False

    return True


cdef insert_local_ref(dict locals, str name, EntityType entity):
    parts = name.split(".")
    last_part = parts.pop()

    container = locals
    for p in parts:
        try:
            container = container[p]
        except KeyError:
            new_container = _localdict()
            container[p] = new_container
            container = new_container
    container[last_part] = entity


class _localdict(dict):
    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise NameError("No such attribute: " + name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name in self:
            del self[name]
        else:
            raise NameError("No such attribute: " + name)
