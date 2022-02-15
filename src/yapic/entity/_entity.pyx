import sys
import cython
import random
import string
from weakref import ReferenceType

from collections.abc import ItemsView
from operator import attrgetter

from cpython.object cimport PyObject
from cpython.ref cimport Py_DECREF, Py_INCREF, Py_XDECREF, Py_XINCREF
from cpython.tuple cimport PyTuple_SetItem, PyTuple_GetItem, PyTuple_New, PyTuple_GET_SIZE, PyTuple_SET_ITEM, PyTuple_GET_ITEM, PyTuple_Pack
from cpython.list cimport PyList_New, PyList_SET_ITEM
from cpython.module cimport PyImport_Import, PyModule_GetDict
from cpython.weakref cimport PyWeakref_NewRef, PyWeakref_GetObject

from ._field cimport Field, PrimaryKey, ForeignKey
from ._field_impl cimport AutoImpl
from ._relation cimport Relation, ManyToOne, RelatedItem, RelatedAttribute, RelationImpl
from ._factory cimport Factory, get_type_hints, new_instance_from_forward, is_forward_decl
from ._expression cimport Visitor, Expression
from ._registry cimport Registry
from ._entity_serializer import EntitySerializer, SerializerCtx
from ._virtual_attr cimport VirtualAttribute
from ._trigger cimport PolymorphParentDeleteTrigger


cdef class NOTSET:
    pass

REGISTRY = Registry()


cdef class EntityType(type):
    @staticmethod
    def __prepare__(*args, **kwargs):
        scope = type.__prepare__(*args, **kwargs)
        scope["__slots__"] = ()
        return scope

    def __cinit__(self, *args, **kwargs):
        self.__fix_entries__ = None
        self.__deferred__ = []
        self.__triggers__ = []
        self.__extgroups__ = {}

    def __init__(self, *args, _root=False, **kwargs):
        super().__init__(*args)
        (name, bases, attrs) = args

        self.__deps__ = EntityDependency(<object>self.registry_ref)

        cdef EntityType base_entity = self.get_base_entity()

        # determine polymorph
        cdef object poly_meta = self.get_meta("polymorph", None)
        cdef PolymorphMeta polymorph = None
        if poly_meta:
            if isinstance(poly_meta, PolymorphMeta):
                polymorph = <PolymorphMeta>poly_meta
            else:
                polymorph = PolymorphMeta(poly_meta)
                self.set_meta("polymorph", polymorph)

        # determine attributes
        cdef EntityAttribute attr
        cdef list __attrs__ = None
        cdef list __fields__ = []
        cdef list non_fields = []
        cdef list __pk__ = []
        cdef object self_ref

        cdef list fields = kwargs.get("__fields__", [])
        if fields:
            for field in fields:
                if not isinstance(field, Field):
                    raise ValueError(f"__fields__ contains invalid value: {field}")
            __attrs__ = fields
        else:
            __attrs__ = self._compute_attrs(base_entity, polymorph, attrs)

        if len(__attrs__) > 0:
            self_ref = <object>PyWeakref_NewRef(self, None)
            for attr in __attrs__:
                attr._bind(self_ref, <object>self.registry_ref)
                if isinstance(attr, VirtualAttribute):
                    non_fields.append(attr)
                else:
                    if attr._key_ is not None:
                        setattr(self, attr._key_, attr)
                    elif attr._name_ is not None:
                        attr._key_ = attr._name_
                        setattr(self, attr._key_, attr)

                    if isinstance(attr, Field):
                        __fields__.append(attr)
                        if attr.get_ext(PrimaryKey):
                            __pk__.append(attr)
                    else:
                        non_fields.append(attr)

            # finalizing
            self.__pk__ = tuple(__pk__)
            self.__fields__ = tuple(__fields__)
            self.__attrs__ = tuple(__fields__ + non_fields)

            for index, attr in enumerate(self.__attrs__):
                attr._index_ = index
                self.__deferred__.append(attr)
        else:
            self.__pk__ = tuple()
            self.__fields__ = tuple()
            self.__attrs__ = tuple()

        if _root is False:
            if is_entity_alias(self) is False:
                self.resolve_ctx = ResolveContext(self, sys._getframe(0))
                self.get_registry().register(self)
            else:
                if self._stage_resolving() is False:
                    raise RuntimeError(f"Can't resolve entity alias: {self} ({self.__deferred__})")
                self._stage_resolved()

    cdef EntityType get_base_entity(self):
        # determine base entity
        for base in self.__mro__:
            if base is self:
                continue

            if isinstance(base, EntityType):
                return <EntityType>base
        return None

    cdef list _compute_triggers(self):
        cdef EntityType base_entity = self.get_base_entity()
        cdef PolymorphMeta polymorph = self.get_meta("polymorph", None)
        cdef list result = []

        if polymorph and base_entity and base_entity.__pk__:
            found = None
            for i, t in enumerate(result):
                if isinstance(t, PolymorphParentDeleteTrigger) and (<PolymorphParentDeleteTrigger>t).parent_entity is base_entity:
                    found = i
                    break

            if found is not None:
                result[found] = PolymorphParentDeleteTrigger(base_entity)
            else:
                result.append(PolymorphParentDeleteTrigger(base_entity))

        return result


    cdef list _compute_attrs(self, EntityType base_entity, PolymorphMeta polymorph, object cls_dict):
        if not isinstance(cls_dict, dict):
            raise ValueError("cls_dict must be dict")

        cdef tuple hints = get_type_hints(self)
        cdef Factory factory
        cdef EntityAttribute attr
        cdef list result = []

        if polymorph and base_entity.__fields__:
            polymoph_id = self.get_meta("polymorph_id")
            poly_join = None
            poly_relation = Relation(ManyToOne(base_entity, RelatedItem()))

            result.append(poly_relation)
            polymorph.add(polymoph_id, self, poly_relation)

            for attr in base_entity.__attrs__:
                if attr.get_ext(PrimaryKey):
                    self_pk = Field(AutoImpl(), name=attr._name_) \
                        // ForeignKey(attr, on_delete="CASCADE", on_update="CASCADE") \
                        // PrimaryKey()
                    (<EntityAttribute>self_pk)._key_ = attr._key_
                    result.append(self_pk)

                    if poly_join is None:
                        poly_join = self_pk == attr
                    else:
                        poly_join &= self_pk == attr
                elif attr._key_:
                    # TODO: stateless
                    parent_attr = RelatedAttribute(poly_relation, name=attr._key_)
                    result.append(parent_attr)
                    (<EntityAttribute>parent_attr)._key_ = attr._key_

            (<Relation>poly_relation)._default_ = poly_join

        # TODO: hints in mro order

        if hints[1] is not None:
            for name, type in (<dict>hints[1]).items():
                if polymorph and hasattr(base_entity, name):
                    continue

                factory = Factory.create(type)
                if factory is None:
                    continue

                try:
                    value = (<dict>cls_dict)[name]
                except KeyError:
                    value = getattr(self, name, NOTSET)

                # az öröklődés miatt van itt
                # if isinstance(value, EntityAttribute):
                #     value = (<EntityAttribute>value).clone()

                attr_type = factory.hints[0]

                if issubclass(attr_type, EntityAttribute):
                    attr = create_attribute(factory(), value)
                    attr._key_ = name
                    if not attr._name_:
                        attr._name_ = name

                    result.append(attr)


        for k, v in (<dict>cls_dict).items():
            if isinstance(v, VirtualAttribute):
                if not (<VirtualAttribute>v)._key_:
                    (<VirtualAttribute>v)._key_ = k
                    # if no key, they have not initialized, but if have key parent is initialized
                    result.append(v)

        return result

    @property
    def __meta__(self):
        return <object>self.meta

    cpdef object get_meta(self, str key=None, default=NOTSET):
        if isinstance(<object>self.meta, dict):
            if key is None:
                return <object>self.meta
            else:
                try:
                    return (<dict>self.meta)[key]
                except KeyError:
                    if default is NOTSET:
                        raise KeyError(f"Missing '{key}' from <Entity {self.__module__}::{self.__name__}>.__meta__ properties")
                    else:
                        return default
        else:
            raise RuntimeError(f"<Entity {self.__module__}::{self.__name__}>.__meta__ is not initilazied")

    cpdef object set_meta(self, str key, object value):
        (<object>self.meta)[key] = value

    cpdef bint has_meta(self, str key):
        if isinstance(<object>self.meta, dict):
            return key in (<dict>self.meta)
        else:
            return False

    @property
    def __registry__(self):
        return self.get_registry()

    cdef Registry get_registry(self):
        if self.registry_ref is not <PyObject*>None:
            return <object>PyWeakref_GetObject(<object>self.registry_ref)
        else:
            return None

    @property
    def __qname__(self):
        try:
            schema = self.get_meta("schema")
        except KeyError:
            return self.__name__
        else:
            if not schema:
                return self.__name__
            else:
                return f"{schema}.{self.__name__}"


    def alias(self, str alias = None):
        cdef EntityType original = get_alias_target(self)
        cdef dict clsdict = {"__new__": _no_alias_instance, "__init__": _no_alias_instance}
        return EntityAlias(alias or "", (EntityBase,), clsdict, alias_target=original, registry=<object>original.registry_ref)

    cdef object _stage_resolving(self):
        cdef EntityAttribute attr
        cdef list deferred = self.__deferred__
        cdef list unresolved

        while True:
            unresolved = []
            for attr in deferred:
                if attr._resolve_deferred(self.resolve_ctx) is False:
                    unresolved.append(attr)

            # cant resolve any new attr
            if len(deferred) == len(unresolved):
                break
            deferred = unresolved

        self.__deferred__ = deferred
        return len(deferred) == 0

    cdef object _stage_resolved(self):
        cdef EntityAttributeExtGroup group
        cdef PolymorphMeta polymorph
        cdef EntityAttribute attr

        self.resolve_ctx = None

        for attr in self.__attrs__:
            attr.init()
            self.__deps__ = self.__deps__.merge(attr._deps_)

        for group in self.__extgroups__.values():
            group.type.validate_group(group)
            # TODO: maybe group.seal()

        self.__triggers__.extend(self._compute_triggers())
        self.__entity_ready__()

    cdef bint is_deferred(self):
        return len(self.__deferred__) != 0

    cdef bint is_resolved(self):
        return len(self.__deferred__) == 0

    cdef bint is_empty(self):
        return len(self.__attrs__) == 0

    cpdef object __entity_ready__(self):
        pass

    def __repr__(self):
        return f"<Entity {self.__qname__}>"

    def __dealloc__(self):
        Py_XDECREF(self.meta)
        Py_XDECREF(self.registry_ref)


@cython.final
cdef class EntityAlias(EntityType):
    def __init__(self, *args, EntityType alias_target, **kwargs):
        if alias_target.is_deferred() is True:
            raise RuntimeError(f"Can't alias deferred entity: {alias_target} ({alias_target.__deferred__})")

        self.set_entity(alias_target)
        super().__init__(*args, **kwargs)

    @property
    def __origin__(self):
        return self.get_entity()

    cdef EntityType get_entity(self):
        if self.entity_ref is not None:
            return <EntityType>PyWeakref_GetObject(self.entity_ref)

    cdef EntityType set_entity(self, EntityType entity):
        if self.entity_ref is not None:
            raise ValueError("Can't set entity on alias")
        self.entity_ref = <object>PyWeakref_NewRef(entity, None)

    cdef list _compute_attrs(self, EntityType base_entity, PolymorphMeta polymorph, object attrs):
        cdef EntityType aliased = self.get_entity()
        cdef EntityAttribute attr
        cdef list result = []
        cdef dict relations = {}
        cdef list relatad_attrs = []

        for v in aliased.__attrs__:
            if isinstance(v, RelatedAttribute):
                relatad_attrs.append((len(result), v))
                result.append(None)
            elif isinstance(v, EntityAttribute):
                attr = (<EntityAttribute>v).clone()
                attr._key_ = (<EntityAttribute>v)._key_
                result.append(attr)

                if isinstance(attr, Relation):
                    relations[id(v)] = attr

        for i, relatad_attr in relatad_attrs:
            attr = RelatedAttribute(relations[id((<RelatedAttribute>relatad_attr).__relation__)], name=(<RelatedAttribute>relatad_attr)._name_)
            attr._key_ = (<RelatedAttribute>relatad_attr)._key_
            result[i] = attr

        # TODO: clone PolymorphMeta
        # self.set_meta("polymorph", self.get_meta("polymorph", None))
        self._copy_meta(("polymorph",))

        return result

    cdef list _compute_triggers(self):
        return []

    cdef void _copy_meta(self, keys):
        cdef dict entity_meta = <dict>self.get_entity().__meta__
        cdef dict self_meta = <dict>self.__meta__
        for k in keys:
            try:
                v = entity_meta[k]
            except KeyError:
                pass
            else:
                self_meta[k] = v

    # def __instancecheck__(self, instance):
    #     return isinstance(instance, self.get_entity())

    # def __subclasscheck__(self, subclass):
    #     return issubclass(subclass, self.get_entity())

    def __repr__(self):
        entity = self.get_entity()
        if self.__name__:
            return f"<Alias({id(self)}|{self.__name__}) of {entity.__qname__}>"
        else:
            return f"<Alias({id(self)}) of {entity.__qname__}>"


def _no_alias_instance(self, *args, **kwargs):
    raise RuntimeError(f"{self} is only alias, and cannot be instantiated")


cpdef bint is_entity_alias(object o):
    return type(o) is EntityAlias


cpdef EntityType get_alias_target(EntityType o):
    if is_entity_alias(o):
        return (<EntityAlias>o).get_entity()
    else:
        return o


cdef EntityAttribute create_attribute(EntityAttribute by_type, object value):
    if isinstance(value, EntityAttribute):
        (<EntityAttribute>value).copy_into(by_type)
        return by_type
    elif isinstance(value, EntityAttributeExt):
        by_type._exts_.append(value)
        return by_type
    elif isinstance(value, EntityAttributeExtList):
        by_type._exts_.extend(<list>value)
        return by_type
    else:
        if value is not NOTSET:
            by_type._default_ = value
        return by_type


cdef int ENTITY_ATTRIBUTE_UID_COUNTER = 1


cdef class EntityAttribute(Expression):
    def __cinit__(self, *args, **kwargs):
        global ENTITY_ATTRIBUTE_UID_COUNTER
        self._uid_ = ENTITY_ATTRIBUTE_UID_COUNTER
        ENTITY_ATTRIBUTE_UID_COUNTER += 1

        if args:
            impl = args[0]
            if isinstance(impl, EntityAttributeImpl):
                self._impl_ = impl
                self._impl = None
            else:
                self._impl_ = None
                self._impl = impl
        else:
            self._impl = None

        self._exts_ = []

    @property
    def _entity_(self):
        return self.get_entity()

    cdef EntityType get_entity(self):
        if self.entity_ref is None:
            return None
        return <EntityType>PyWeakref_GetObject(self.entity_ref)

    def __floordiv__(EntityAttribute self, EntityAttributeExt other):
        self._exts_.append(other)

        # TODO: maybe move to another function
        # this need for databaes reflect
        if self.entity_ref is not None:
            attr_ref = <object>PyWeakref_NewRef(self, None)
            other._bind(attr_ref)
        return self

    def __get__(self, instance, owner):
        if instance is None:
            return self
        elif isinstance(instance, EntityBase):
            res = (<EntityBase>instance).__state__.get_value(self)
            if res is NOTSET:
                return None
            else:
                return res
        else:
            raise TypeError("Instance must be 'None' or 'EntityBase'")

    def __set__(self, EntityBase instance, value):
        instance.__state__.set_value(self, value)

    def __delete__(self, EntityBase instance):
        instance.__state__.del_value(self)

    cdef object _bind(self, object entity_ref, object registry_ref):
        cdef EntityType current
        cdef EntityType new
        cdef EntityAttributeExt ext
        cdef object attr_ref

        if self.entity_ref is None:
            self.entity_ref = entity_ref
            self._deps_ = EntityDependency(registry_ref)
        else:
            current = <object>PyWeakref_GetObject(self.entity_ref)
            new = <object>PyWeakref_GetObject(entity_ref)
            if current is not new:
                raise RuntimeError(f"Can't rebind entity attribute {current} -> {new}")

        if self._exts_:
            attr_ref = <object>PyWeakref_NewRef(self, None)

            for ext in self._exts_:
                ext._bind(attr_ref)

    cdef object _resolve_deferred(self, ResolveContext ctx):
        cdef EntityAttributeExt ext

        if self._impl_ is None:
            if self._impl is None:
                raise TypeError("Missing attribute implementation: %r" % self)

            if is_forward_decl(self._impl):
                try:
                    self._impl_ = ctx.forward_ref(self._impl)
                except NameError as e:
                    return False
            else:
                raise ValueError(f"Unexpected attribute implementation: {self._impl}")
            self._impl = None

        if self._impl_._resolve_deferred(ctx, self) is False:
            return False

        for ext in self._exts_:
            if not ext._resolve_deferred(ctx):
                return False

        return True


    cpdef object init(self):
        cdef EntityAttributeExt ext

        if not self._impl_.inited:
            if self._impl_.init(self) is False:
                return False
            else:
                self._impl_.inited = True

        for ext in self._exts_:
            ext.init()

    cpdef clone(self):
        raise NotImplementedError()

    # TODO: rework
    cpdef clone_exts(self, EntityAttribute attr):
        cdef EntityAttributeExt ext
        cdef object attr_ref = <object>PyWeakref_NewRef(attr, None)
        cdef int length = len(self._exts_)
        cdef list res = PyList_New(length)

        for i, ext in enumerate(self._exts_):
            ext = ext.clone()
            ext._bind(attr_ref)
            Py_INCREF(ext)
            PyList_SET_ITEM(res, i, ext)
        return res

    cpdef object get_ext(self, ext_type):
        for ext in self._exts_:
            if isinstance(ext, ext_type):
                return ext

    cpdef copy_into(self, EntityAttribute other):
        other._exts_.extend(self.clone_exts(other))
        other._default_ = self._default_

    cpdef _entity_repr(self):
        if self.entity_ref is None:
            return "(unbound)"
        else:
            return self.get_entity() or "(dead entity)"


cdef class EntityAttributeExt:
    @classmethod
    def validate_group(self, EntityAttributeExtGroup group):
        pass

    @property
    def attr(self):
        return self.get_attr()

    cdef EntityAttribute get_attr(self):
        if self.attr_ref is not None:
            return <EntityAttribute>PyWeakref_GetObject(self.attr_ref)
        else:
            return None

    cdef EntityType get_entity(self):
        return self.get_attr().get_entity()

    cdef object _bind(self, object attr_ref):
        if self.attr_ref is None:
            self.attr_ref = attr_ref
        else:
            current = <object>PyWeakref_GetObject(self.attr_ref)
            new = <object>PyWeakref_GetObject(attr_ref)
            if current is not new:
                raise RuntimeError(f"Can't rebind attribute extension {current} -> {new}")
        return True

    cdef object _resolve_deferred(self, ResolveContext ctx):
        return True

    def __floordiv__(EntityAttributeExt self, EntityAttributeExt other):
        return EntityAttributeExtList((self, other))

    cpdef object init(self):
        pass

    cpdef object add_to_group(self, str key):
        cdef EntityType entity = self.get_entity()
        cdef EntityAttributeExtGroup group

        # if is_entity_alias(entity):
        #     return

        try:
            group = entity.__extgroups__[key]
        except KeyError:
            group = EntityAttributeExtGroup(key, type(self))
            entity.__extgroups__[key] = group
            group.items.append(self)
        else:
            if not isinstance(self, group.type):
                raise ValueError("Can't mix extension types in group")
            for item in group.items:
                # already existing in this group
                if id(item) == id(self):
                    return
            group.items.append(self)

    cpdef object clone(self):
        return type(self)()

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return repr(self) == repr(other)

    def __ne__(self, other):
        return repr(self) != repr(other)

    def __repr__(self):
        return f"@{type(self).__name__}()"


cdef class EntityAttributeExtList(list):
    def __floordiv__(self, EntityAttributeExt other):
        self.append(other)
        return self


cdef class EntityAttributeExtGroup:
    def __cinit__(self, str name, object type):
        self.name = name
        self.type = type
        self.items = []

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return repr(self) == repr(other)

    def __ne__(self, other):
        return repr(self) != repr(other)

    def __repr__(self):
        return "@%s:%s[%s]" % (self.type.__name__, self.name, ", ".join(map(repr, self.items)))


cdef class EntityAttributeImpl:
    def __cinit__(self, *args, **kwargs):
        self.inited = False

    cdef object _resolve_deferred(self, ResolveContext ctx, EntityAttribute attr):
        return True

    cpdef object init(self, EntityAttribute attr):
        return True

    cpdef object clone(self):
        raise NotImplementedError()

    cpdef object getattr(self, EntityAttribute attr, object key):
        raise NotImplementedError()

    cpdef object getitem(self, EntityAttribute attr, object index):
        raise NotImplementedError()

    cdef object state_init(self, object initial):
        return NOTSET

    cdef object state_set(self, object initial, object current, object value):
        return value

    cdef object state_get_dirty(self, object initial, object current):
        if initial != current:
            return current
        else:
            return NOTSET

    def __eq__(self, other):
        return self._is_eq(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    cdef bint _is_eq(self, object other):
        return isinstance(self, type(other)) or isinstance(other, type(self))


cdef inline state_set_value(PyObject* initial, PyObject* current, EntityAttribute attr, object value):
    cdef int idx = attr._index_
    cdef PyObject* iv = PyTuple_GET_ITEM(<object>initial, idx)
    cdef PyObject* cv = PyTuple_GET_ITEM(<object>current, idx)

    nv = (<EntityAttributeImpl>attr._impl_).state_set(<object>iv, <object>cv, value)
    Py_INCREF(<object>nv)
    Py_XDECREF(cv)
    PyTuple_SET_ITEM(<object>current, idx, <object>nv)


@cython.final
@cython.freelist(1000)
cdef class EntityState:

    def __cinit__(self, EntityType entity):
        cdef int length = len(entity.__attrs__)
        self.entity = entity
        self.initial = PyTuple_New(length)
        self.current = PyTuple_New(length)
        self.field_count = len(entity.__fields__)

    cdef object init(self):
        cdef int idx
        cdef EntityAttribute attr
        cdef PyObject* initial = <PyObject*>self.initial
        cdef PyObject* current = <PyObject*>self.current
        cdef PyObject* cv

        for attr in self.entity.__attrs__:
            idx = attr._index_

            cv = PyTuple_GET_ITEM(<object>initial, idx)
            if cv is NULL:
                cv = <PyObject*>NOTSET
                Py_INCREF(<object>cv)
                PyTuple_SET_ITEM(<object>initial, idx, <object>cv)

            iv = (<EntityAttributeImpl>attr._impl_).state_init(<object>cv)

            Py_INCREF(<object>iv)
            cv = PyTuple_GET_ITEM(<object>current, idx)
            Py_XDECREF(cv)
            PyTuple_SET_ITEM(<object>current, idx, <object>iv)


    cpdef object update(self, dict data, bint is_initial = False):
        cdef EntityAttribute attr
        cdef EntityType entity = self.entity

        if is_initial:
            for k, v in data.items():
                attr = getattr(entity, k)
                self.set_initial_value(attr, v)
        else:
            for k, v in data.items():
                attr = getattr(entity, k)
                self.set_value(attr, v)

    cdef object set_value(self, EntityAttribute attr, object value):
        state_set_value(<PyObject*>self.initial, <PyObject*>self.current, attr, value)

    cdef object set_initial_value(self, EntityAttribute attr, object value):
        cdef int idx = attr._index_
        cdef PyObject* initial = <PyObject*>self.initial
        cdef PyObject* iv = PyTuple_GET_ITEM(<object>initial, idx)
        cdef EntityAttributeImpl impl = <EntityAttributeImpl>attr._impl_

        if iv is NULL:
            iv = <PyObject*>NOTSET
            Py_INCREF(<object>iv)

        nv = impl.state_set(<object>iv, <object>iv, value)
        Py_INCREF(<object>nv)
        Py_XDECREF(iv)
        PyTuple_SET_ITEM(<object>initial, idx, <object>nv)

    cdef object get_value(self, EntityAttribute attr):
        cdef PyObject* initial = <PyObject*>self.initial
        cdef PyObject* current = <PyObject*>self.current
        cdef PyObject* cv = PyTuple_GET_ITEM(<object>current, attr._index_)

        if cv is <PyObject*>NOTSET:
            cv = PyTuple_GET_ITEM(<object>initial, attr._index_)

        return <object>cv

    cdef object del_value(self, EntityAttribute attr):
        cdef PyObject* current = <PyObject*>self.current
        cdef PyObject* cv = PyTuple_GET_ITEM(<object>current, attr._index_)
        cdef PyObject* nv = <PyObject*>NOTSET;
        Py_INCREF(<object>nv)
        Py_XDECREF(cv)
        PyTuple_SET_ITEM(<object>current, attr._index_, <object>nv)

    cdef list data_for_insert(self):
        cdef int idx
        cdef list res = []
        cdef EntityType entity = self.entity
        cdef PyObject* initial = <PyObject*>self.initial
        cdef PyObject* current = <PyObject*>self.current
        cdef PyObject* cv
        cdef EntityAttribute attr

        for attr in entity.__attrs__:
            idx = attr._index_
            cv = PyTuple_GET_ITEM(<object>current, idx)
            if cv is <PyObject*>NOTSET:
                cv = PyTuple_GET_ITEM(<object>initial, idx)
            if cv is <PyObject*>NOTSET:
                if not attr.get_ext(PrimaryKey):
                    cv = <PyObject*>(attr._default_)
                    if not isinstance(<object>cv, Expression) and callable(<object>cv):
                        res.append((attr, (<object>cv)()))
                continue

            res.append((attr, <object>cv))

        return res

    cdef list data_for_update(self):
        cdef int idx
        cdef list res = []
        cdef EntityType entity = self.entity
        cdef PyObject* initial = <PyObject*>self.initial
        cdef PyObject* current = <PyObject*>self.current
        cdef PyObject* iv
        cdef PyObject* cv
        cdef EntityAttribute attr

        for attr in entity.__attrs__:
            idx = attr._index_

            iv = PyTuple_GET_ITEM(<object>initial, idx)
            cv = PyTuple_GET_ITEM(<object>current, idx)
            nv = (<EntityAttributeImpl>attr._impl_).state_get_dirty(<object>iv, <object>cv)

            if nv is NOTSET:
                continue

            res.append((attr, <object>nv))

        return res

    def changes(self, EntityAttribute attr=None):
        if attr is not None:
            return self.attr_changes(attr)

        cdef dict res = {}
        for attr, value in self.data_for_update():
            res[attr._name_] = value
        return res

    def changes_with_previous(self):
        cdef dict res = {}
        cdef int idx
        cdef tuple initial = self.initial

        for attr, value in self.data_for_update():
            idx = attr._index_
            res[attr._name_] = (initial[idx], value)

        return res

    cdef object attr_changes(self, EntityAttribute attr):
        cdef int idx = attr._index_
        cdef PyObject* iv
        cdef PyObject* cv
        cdef PyObject* initial = <PyObject*>self.initial
        cdef PyObject* current = <PyObject*>self.current

        iv = PyTuple_GET_ITEM(<object>initial, idx)
        cv = PyTuple_GET_ITEM(<object>current, idx)
        return (<EntityAttributeImpl>attr._impl_).state_get_dirty(<object>iv, <object>cv)

    @property
    def is_dirty(self):
        for attr in self.entity.__attrs__:
            if self.attr_changes(attr) is not NOTSET:
                return True
        return False

    @property
    def is_empty(self):
        return self._is_empty()

    cdef bint _is_empty(self):
        cdef PyObject* initial = <PyObject*>self.initial
        cdef PyObject* current = <PyObject*>self.current
        # cdef PyObject* cv = PyTuple_GET_ITEM(<object>current, attr._index_)

        # if cv is <PyObject*>NOTSET:
        #     cv = PyTuple_GET_ITEM(<object>initial, attr._index_)


        cdef PyObject* val
        for attr in self.entity.__attrs__:
            val = PyTuple_GET_ITEM(<object>current, attr._index_)

            if val is NULL or val is <PyObject*>NOTSET:
                val = PyTuple_GET_ITEM(<object>initial, attr._index_)

            if val is not NULL and val is not <PyObject*>NOTSET and val is not <PyObject*>None:
                return False
        return True

    def reset(self, EntityAttribute attr=None):
        if attr is None:
            return self.reset_all()
        else:
            return self.reset_attr(attr)

    cdef reset_all(self):
        cdef int length = len(self.current)
        cdef int idx
        cdef PyObject* iv
        cdef PyObject* cv
        cdef PyObject* initial = <PyObject*>self.initial
        cdef PyObject* current = <PyObject*>self.current

        for idx in range(length):
            cv = PyTuple_GET_ITEM(<object>current, idx)
            if cv is <PyObject*>NOTSET:
                iv = PyTuple_GET_ITEM(<object>initial, idx)

                if iv is not <PyObject*>NOTSET:
                    Py_INCREF(<object>iv)
                    Py_DECREF(<object>cv)
                    PyTuple_SET_ITEM(<object>current, idx, <object>iv)

        self.initial = self.current
        self.current = PyTuple_New(length)
        self.init()

    cdef reset_attr(self, EntityAttribute attr):
        cdef int idx = attr._index_
        cdef PyObject* iv
        cdef PyObject* cv
        cdef PyObject* initial = <PyObject*>self.initial
        cdef PyObject* current = <PyObject*>self.current

        cv = PyTuple_GET_ITEM(<object>current, idx)
        if cv is <PyObject*>NOTSET:
            return

        Py_INCREF(<object>cv)
        iv = PyTuple_GET_ITEM(<object>initial, idx)
        Py_XDECREF(iv)
        PyTuple_SET_ITEM(<object>initial, idx, <object>cv)

        val = (<EntityAttributeImpl>attr._impl_).state_init(<object>cv)
        cv = PyTuple_GET_ITEM(<object>current, idx)
        Py_XDECREF(cv)
        Py_INCREF(<object>val)
        PyTuple_SET_ITEM(<object>current, idx, <object>val)

    def changed_realtions(self):
        cdef EntityAttribute attr

        for i in range(self.field_count, len(self.entity.__attrs__)):
            attr = self.entity.__attrs__[i]
            if isinstance(attr, Relation):
                val = self.attr_changes(attr)
                if val is not NOTSET:
                    yield (attr, val)

    def __eq__(EntityState self, other):
        if not isinstance(other, EntityState):
            return False

        cdef EntityState other_state = (<EntityState>other)

        if self.entity is not other_state.entity:
            if self.entity.__qname__ == other_state.entity.__qname__:
                return self.is_eq_reflected(other_state)
            else:
                return False

        cdef int idx
        cdef EntityAttribute attr

        for attr in self.entity.__attrs__:
            sv = self.get_value(attr)
            ov = other_state.get_value(attr)
            nv = (<EntityAttributeImpl>attr._impl_).state_get_dirty(sv, ov)

            if nv is not NOTSET:
                return False

        return True

    def __ne__(EntityState self, other):
        return not self.__eq__(other)

    def __bool__(EntityState self):
        return self._is_empty() is False

    cdef bint is_eq_reflected(self, EntityState other):
        if len(self.entity.__attrs__) != len(other.entity.__attrs__):
            return False

        cdef EntityAttribute attr
        cdef EntityAttribute other_attr
        cdef EntityType other_entity = other.entity

        for attr in self.entity.__attrs__:
            try:
                other_attr = getattr(other_entity, attr._name_)
            except AttributeError:
                try:
                    other_attr = getattr(other_entity, attr._key_)
                except AttributeError:
                    return False

            sv = self.get_value(attr)
            ov = other.get_value(other_attr)
            nv = (<EntityAttributeImpl>attr._impl_).state_get_dirty(sv, ov)

            if nv is not NOTSET:
                return False

        return True


cdef class EntityBase:
    def __cinit__(self, state=None, **values):
        cdef EntityType model = type(self)

        if model.is_deferred():
            raise RuntimeError(f"Entity is not resolved, pending items: {model.__deferred__}")

        if isinstance(state, EntityState):
            self.__state__ = state
        else:
            self.__state__ = EntityState(model)

    def __init__(self, data = None, **kw):
        cdef PolymorphMeta poly
        cdef EntityType model

        if not isinstance(data, EntityState):
            self.__state__.init()

            if isinstance(data, dict):
                self.__state__.update(data, False)
            if len(kw) != 0:
                self.__state__.update(kw, False)

            model = type(self)
            poly = model.get_meta("polymorph", None)
            if poly is not None:
                poly_id = model.get_meta("polymorph_id", None)
                if poly_id is not None:
                    poly_id = PolymorphMeta.normalize_id(poly_id)
                    for i, idf in enumerate(poly.id_fields):
                        setattr(self, idf, poly_id[i])
        else:
            self.__state__.init()

    @classmethod
    def __init_subclass__(cls, *, str name=None, registry=None, bint _root=False, EntityType alias_target=None, **meta):
        cdef EntityType ent = cls
        cdef EntityType parent_entity
        cdef int mro_length = len(cls.__mro__)
        cdef dict meta_dict = {}

        if name is not None:
            ent.__name__ = name

        for i in reversed(range(1, mro_length - 2)):
            parent = cls.__mro__[i]
            if isinstance(parent, EntityType):
                parent_entity = <EntityType>parent
                if parent_entity.meta is not NULL:
                    meta_dict.update(<object>parent_entity.meta)

        if registry is None:
            for i in range(1, mro_length - 2):
                parent = cls.__mro__[i]
                if isinstance(parent, EntityType):
                    parent_entity = <EntityType>parent
                    if parent_entity.registry_ref is not <PyObject*>None:
                        registry = <object>parent_entity.registry_ref
                        break

        meta_dict.update(meta)
        meta_dict.pop("__fields__", None)

        Py_XINCREF(<PyObject*>(<object>meta_dict))
        Py_XDECREF(ent.meta)
        ent.meta = <PyObject*>(<object>meta_dict)

        if registry is not None:
            if isinstance(registry, ReferenceType):
                referenced = <object>PyWeakref_GetObject(registry)
                if not isinstance(referenced, Registry):
                    raise TypeError(f"Argument 'registry' has incorrect waekref {type(referenced)} expected: {Registry}")
            elif isinstance(registry, Registry):
                registry = <object>PyWeakref_NewRef(registry, None)
            else:
                raise TypeError(f"Argument 'registry' has incorrect type {type(registry)} expected: {Registry} or weakref")
        else:
            raise ValueError("Missing registry")

        Py_XINCREF(<PyObject*>(<object>registry))
        Py_XDECREF(ent.registry_ref)
        ent.registry_ref = <PyObject*>(<object>registry)

    @property
    def __pk__(self):
        cdef EntityAttribute attr
        cdef EntityType ent = type(self)
        cdef EntityState state = self.__state__
        cdef int length = len(ent.__pk__)
        cdef tuple res = PyTuple_New(length)
        cdef bint has_pk = False

        for idx, attr in enumerate(ent.__pk__):
            val = state.get_value(attr)

            if val is NOTSET:
                Py_INCREF(<object>None)
                PyTuple_SET_ITEM(<object>res, idx, <object>None)
            else:
                has_pk = True
                Py_INCREF(<object>val)
                PyTuple_SET_ITEM(<object>res, idx, <object>val)

        if has_pk:
            return res
        else:
            return ()

    @__pk__.setter
    def __pk__(self, val):
        cdef EntityAttribute attr
        cdef EntityType ent = type(self)
        cdef EntityState state = self.__state__
        cdef tuple pk = (<tuple>val) if isinstance(val, tuple) else (val,)

        for i, attr in enumerate(ent.__pk__):
            state.set_value(attr, pk[i])


    def __hash__(self):
        return hash(type(self).__qname__) ^ hash(self.__pk__)

    def __eq__(self, other):
        return isinstance(other, EntityBase) and self.__pk__ == other.__pk__

    def __ne__(self, other):
        return not isinstance(other, EntityBase) or self.__pk__ != other.__pk__

    def __bool__(EntityBase self):
        return self.__state__._is_empty() is False

    def __iter__(self):
        self.iter_index = 0
        return self

    def __next__(self):
        cdef EntityType ent = type(self)
        cdef EntityAttribute attr
        cdef int idx = self.iter_index

        if idx < len(ent.__attrs__):
            self.iter_index += 1

            attr = <EntityAttribute>(<tuple>(ent.__attrs__)[idx])
            if attr._key_ is None:
                return self.__next__()

            value = getattr(self, attr._key_)
            # value = self.__state__.get_value(attr)

            if value is NOTSET:
                return self.__next__()
            else:
                return (attr, value)
        else:
            raise StopIteration()

    def __json__(self):
        ctx = SerializerCtx()
        return EntitySerializer(self, ctx)

    def __getitem__(self, key):
        if isinstance(key, str):
            return getattr(self, key)
        elif isinstance(key, int):
            return getattr(self, type(self).__fields__[key]._key_)
        else:
            raise TypeError("Unsupported key type: %r" % type(key))

    def __setitem__(self, key, value):
        if isinstance(key, str):
            setattr(self, key, value)
        elif isinstance(key, int):
            setattr(self, type(self).__fields__[key]._key_, value)
        else:
            raise TypeError("Unsupported key type: %r" % type(key))

    def serialize(self, ctx=None):
        if ctx is None:
            ctx = SerializerCtx()
        return EntitySerializer(self, ctx)

    def as_dict(self):
        cdef dict res = {}

        for attr, value in self:
            res[attr._key_] = as_dict(value)

        return res

    def __repr__(self):
        cdef EntityType entity = type(self)
        is_type = entity.get_meta("is_type", False)
        if is_type is True:
            return "<%s %r>" % (type(self).__qname__, self.as_dict())
        else:
            return "<%s %r>" % (type(self).__qname__, self.__pk__)


cdef object as_dict(object o):
    if isinstance(o, EntityBase):
        return o.as_dict()
    elif isinstance(o, list):
        return [as_dict(v) for v in o]
    else:
        return o


class Entity(EntityBase, metaclass=EntityType, registry=REGISTRY, _root=True):
    pass


# TODO inherit from set
@cython.final
cdef class EntityDependency:
    def __cinit__(self, object registry_ref):
        self.registry_ref = registry_ref
        self.entity_names = set()

    cdef Registry get_registry(self):
        if self.registry_ref is not None:
            return <object>PyWeakref_GetObject(self.registry_ref)
        else:
            return None

    cpdef add_entity(self, EntityType entity):
        # if entity is builtin we dont need in dependecies
        if entity_is_builtin(entity):
            return self

        if self.get_registry() is not entity.get_registry():
            raise ValueError(f"Can't add different registry dependency: {entity}")

        self.entity_names.add(entity.__qname__)
        return self

    cpdef EntityDependency merge(self, EntityDependency other):
        if other.entity_names and self.get_registry() is not other.get_registry():
            raise ValueError("Can't merge different registry dependendcies")

        result = EntityDependency(self.registry_ref)
        result.entity_names = self.entity_names | other.entity_names
        return result


    cpdef EntityDependency intersection(self, EntityDependency other):
        if self.get_registry() is not other.get_registry():
            raise ValueError("Can't determine instersection of different registries")

        result = EntityDependency(self.registry_ref)
        result.entity_names = self.entity_names & other.entity_names
        return result

    cpdef list entities(self):
        cdef Registry registry = self.get_registry()
        cdef list result = []

        for name in self.entity_names:
            result.append(registry[name])

        return result

    cpdef EntityDependency clone(self):
        cdef EntityDependency result = EntityDependency(self.registry_ref)
        result.entity_names = set(self.entity_names)
        return result

    def __repr__(self):
        return repr(self.entity_names)



@cython.final
cdef class DependencyList:
    def __cinit__(self):
        self.items = []
        self.circular = {}

    cpdef add(self, EntityType item):
        cdef set cd = set()
        cd.add(item)

        if item not in self:
            self.items.append(item)

        for dep in sorted(item.__deps__.entities(), key=attrgetter("__qname__")):
            self._add(item, dep, cd)

    cpdef index(self, EntityType item):
        return (<list>self.items).index(item)

    cdef _add(self, EntityType entity, EntityType dep, set cd):
        cdef int index
        cdef int dindex

        if dep in cd:
            if entity not in self.circular:
                self._resolve_circular(entity, dep, cd)
                self._add(entity, dep, set())
                return

        try:
            index = self.items.index(entity)
        except ValueError:
            index = len(self)
            self.items.append(entity)

        cd.add(dep)

        all_deps_in_list = False

        try:
            dindex = self.items.index(dep)
        except ValueError:
            self.items.insert(index, dep)
        else:
            all_deps_in_list = True
            if entity not in self.circular or dep in self.circular[entity]:
                if dindex > index:
                    self.items.pop(dindex)
                    self.items.insert(index, dep)
                    all_deps_in_list = False

        if not all_deps_in_list:
            for dd in sorted(dep.__deps__.entities(), key=attrgetter("__qname__")):
                self._add(dep, dd, cd)

        cd.discard(dep)

    cdef _resolve_circular(self, EntityType entity, EntityType dep, set cd):
        entity_dep = _is_dependency(entity, dep)
        dep_entity = _is_dependency(dep, entity)

        if entity_dep and dep_entity:
            raise ValueError("Can't resolve circular reference")

        try:
            cdeps = self.circular[entity]
        except KeyError:
            cdeps = self.circular[entity] = set()

        if entity_dep:
            cdeps.add(dep)

        try:
            cdeps = self.circular[dep]
        except KeyError:
            cdeps = self.circular[dep] = set()

        if dep_entity:
            cdeps.add(entity)


cdef bint _is_dependency(EntityType a, EntityType b):
    cdef EntityAttribute attr
    cdef ForeignKey fk

    for attr in a.__attrs__:
        fk = attr.get_ext(ForeignKey)
        if fk and fk.ref.get_entity() is b:
            if isinstance(attr, Field) and not (<Field>attr).nullable:
                return True
    return False


@cython.final
cdef class PolymorphMeta:
    @staticmethod
    cdef tuple normalize_id(object id):
        if not isinstance(id, tuple):
            if isinstance(id, list):
                return tuple(id)
            else:
                return (id,)
        return id

    def __cinit__(self, object id_fields):
        self.id_fields = PolymorphMeta.normalize_id(id_fields)
        self._decls = []

    def items(self):
        for ref, id, relation in self._decls:
            yield (<object>PyWeakref_GetObject(ref), id, relation)

    def get_entity(self, poly_id):
        poly_id = PolymorphMeta.normalize_id(poly_id)

        for value in self._decls:
            if (<tuple>value)[1] == poly_id:
                return <object>PyWeakref_GetObject((<tuple>value)[0])

        raise ValueError(f"Not found entity with this id: {poly_id}")

    def get_id(self, EntityType entity):
        entity = get_alias_target(entity)
        for ent, id, relation in self.items():
            if ent is entity:
                return id

        raise ValueError(f"Not found id for this entity: {entity}")

    def new_instance(self, poly_id):
        cdef EntityType entity = self.get_entity(poly_id)
        return entity()

    cdef object add(self, object poly_id, EntityType entity, object relation):
        if not isinstance(relation, Relation):
            raise TypeError("Relation expected, but got: %r" % relation)


        for value in self.items():
            if (<tuple>value)[0] is entity:
                raise ValueError(f"{(<tuple>value)[0]} is already in polymorph")

        poly_id = PolymorphMeta.normalize_id(poly_id)
        self._decls.append((<object>PyWeakref_NewRef(entity, self.__on_entity_freed), poly_id, relation))

    cpdef list parents(self, EntityType entity):
        entity = get_alias_target(entity)
        cdef list result = []
        self._parents(entity, result)
        return result

    cdef object _parents(self, EntityType entity, list result):
        cdef Relation relation

        for value in self._decls:
            relation = <Relation>((<tuple>value)[2])
            if relation.get_entity() is entity:
                result.append(relation)
                self._parents((<RelationImpl>relation._impl_).get_joined_entity(), result)

    cpdef list children(self, EntityType entity):
        cdef Relation relation
        entity = get_alias_target(entity)
        cdef list result = []

        for value in self._decls:
            relation = <Relation>((<tuple>value)[2])
            if get_alias_target((<RelationImpl>relation._impl_).get_joined_entity()) is entity:
                result.append(relation)

        return result

    def __on_entity_freed(self, ref):
        for i in reversed(range(0, len(self._decls))):
            ent_ref = (<tuple>self._decls[i])[0]
            if ent_ref is ref:
                del self._decls[i]
