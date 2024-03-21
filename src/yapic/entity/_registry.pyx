import cython
from enum import Enum
from collections import deque

from ._entity cimport DependencyList, EntityBase, EntityType, EntityAttribute, EntityAttributeExt, EntityAttributeExtGroup, EntityAttributeImpl, NOTSET, entity_is_builtin, entity_is_virtual
from ._entity_diff cimport EntityDiff
from ._field cimport ForeignKey, Field


# TODO: inherit from dict
@cython.final
cdef class Registry:
    def __cinit__(self):
        self.entities = {}
        self.locals = ScopeDict()
        self.deferred = deque()
        self.resolved = []
        # self.resolving = set()
        self.is_draft = False
        self.in_resolving = False

    cdef object register(self, EntityType entity):
        cdef str name = entity.__qname__
        if name in self.entities:
            raise ValueError("entity already registered: %r" % entity)
        else:
            # TODO: ha az in_resolving != 0 akkor egy új resolving contextet kezdjen

            if entity.is_empty():
                self.entities[name] = entity
                if entity._stage_resolving() is False:
                    raise RuntimeError("Empty entity resolving failed")
                entity._stage_resolved()
            else:
                self.entities[name] = entity


                if self.is_draft is False:
                    self.locals.set_path(name, entity)
                    for d in self.deferred:
                        if (<EntityType>d).resolve_ctx is not None:
                            (<EntityType>d).resolve_ctx.add_forward(entity)

                    self.deferred.append(entity)
                    self._finalize_entities()
                else:
                    self.deferred.append(entity)


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

    # TODO: remove, and replace with get_referenced_foreign_keys
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
                        if fk.ref.get_entity() is column.get_entity() and fk.ref._name_ == column._name_:
                            per_entity.append(field._key_)

            if len(per_entity) != 0:
                result.append((entity, per_entity))

        return result

    cpdef list get_referenced_foreign_keys(self, EntityAttribute column):
        cdef list result = []
        cdef list per_entity
        cdef EntityType entity
        cdef EntityAttribute field
        cdef EntityAttributeExt ext
        cdef EntityAttributeExtGroup group
        cdef ForeignKey fk

        for entity in self.entities.values():
            per_entity = []

            for group in entity.__extgroups__.values():
                if group in per_entity:
                    continue
                elif group.type is ForeignKey:
                    for fk in group.items:
                        if fk.ref.get_entity() is column.get_entity() and fk.ref._name_ == column._name_:
                            per_entity.append(group)

            if len(per_entity) != 0:
                result.append((entity, per_entity))

        return result

    # TODO: optimize
    cdef _finalize_entities(self):
        if self.in_resolving is True:
            return
        self.in_resolving = True

        cdef EntityType entity
        cdef object deferred = self.deferred
        cdef object nextq = deque()
        cdef int length = len(deferred)
        cdef int last_length = 0

        while last_length != length:
            last_length = length

            while True:
                entity = <EntityType>deferred.pop()
                if entity._stage_resolving() is True:
                    self.resolved.append(entity)
                else:
                    nextq.append(entity)
                if len(deferred) == 0:
                    break

            deferred = nextq
            length = len(deferred)
            if length == 0:
                break
            else:
                nextq = deque()

        self.deferred = deferred
        if len(deferred) == 0:
            for entity in self.resolved:
                entity._stage_resolved()
            self.resolved = []

        self.in_resolving = False


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
    def __cinit__(self, Registry a, Registry b, object entity_diff, bint compare_field_position):
        self.a = a
        self.b = b
        self.changes = []
        cdef DependencyList order = DependencyList()
        cdef EntityBase fix

        a_names = {k for k, v in a.items() if need_to_compare(v)}
        b_names = {k for k, v in b.items() if need_to_compare(v)}

        for removed in sorted(a_names - b_names):
            val = a[removed]
            self.changes.append((RegistryDiffKind.REMOVED, val))
            order.add(val)

        for created in sorted(b_names - a_names):
            val = b[created]
            self.changes.append((RegistryDiffKind.CREATED, val))
            order.add(val)

            if val.__fix_entries__:
                self.changes.append((RegistryDiffKind.INSERT_ENTITY, val.__fix_entries__))

        for maybe_changed in sorted(a_names & b_names):
            val = b[maybe_changed]
            diff = entity_diff(a[maybe_changed], val, compare_field_position)
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
                return order.index(type(val[0]))
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
        cdef Field field
        cdef list result = []
        a_values = set(a_ents)
        b_values = set(b_ents)

        removed = a_values - b_values
        if removed:
            removed = list(sorted(removed, key=a_ents.index, reverse=True))
            result.append((RegistryDiffKind.REMOVE_ENTITY, removed))

        created = b_values - a_values
        if created:
            created = list(sorted(created, key=b_ents.index))
            result.append((RegistryDiffKind.INSERT_ENTITY, created))

        changed = []
        for i in sorted(map(b_ents.index, a_values & b_values)):
            b_ent = b_ents[i]
            a_ent = a_ents[a_ents.index(b_ent)]
            if not entity_data_is_eq(a_ent, b_ent):
                if a_ent.__state__.exists:
                    target = b_ent
                else:
                    target = a_ent
                changed.append(target)

        if changed:
            result.append((RegistryDiffKind.UPDATE_ENTITY, changed))

        return result


cdef inline need_to_compare(EntityType entity):
    return not entity_is_builtin(entity) and not entity_is_virtual(entity)


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
        if attr2 is None and attr is not None:
            return False

        iv = a.__state__.get_value(attr2)
        cv = b.__state__.get_value(attr)
        nv = (<EntityAttributeImpl>attr._impl_).state_get_dirty(iv, cv)

        if nv is NOTSET:
            continue
        else:
            return False

    return True


@cython.final
cdef class ScopeDict(dict):
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise NameError(f"No such attribute: {key}")

    cdef set_path(self, str path, object value):
        cdef list parts = path.split(".")
        cdef str last_part = parts.pop()
        cdef dict container = <dict>self

        for p in parts:
            try:
                container = <dict>container[p]
            except KeyError:
                new_container = ScopeDict()
                container[p] = new_container
                container = <dict>new_container
            else:
                if not isinstance(container, dict):
                    raise ValueError(f"Can't set '{path}' on {self}")

        container[last_part] = value
