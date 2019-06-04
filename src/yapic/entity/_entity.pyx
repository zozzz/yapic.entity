import cython
import random
import string

from collections.abc import ItemsView
from operator import attrgetter

from cpython.object cimport PyObject
from cpython.ref cimport Py_DECREF, Py_INCREF, Py_XDECREF, Py_XINCREF
from cpython.tuple cimport PyTuple_SetItem, PyTuple_GetItem, PyTuple_New, PyTuple_GET_SIZE, PyTuple_SET_ITEM, PyTuple_GET_ITEM, PyTuple_Pack
from cpython.module cimport PyImport_Import, PyModule_GetDict

from ._field cimport Field, PrimaryKey, ForeignKey
from ._field_impl cimport AutoImpl
from ._relation cimport Relation, ManyToOne, RelatedItem, RelatedAttribute
from ._factory cimport Factory, get_type_hints, new_instance_from_forward, is_forward_decl
from ._expression cimport Visitor, Expression
from ._registry cimport Registry
from ._entity_serializer import EntitySerializer, SerializerCtx


cdef class NOTSET:
    pass


REGISTRY = Registry()


cdef class EntityType(type):
    def __cinit__(self, *args, **kwargs):
        # XXX: DONT REMOVE
        (name, bases, attrs) = args


        cdef list fields = kwargs.get("__fields__", [])
        cdef list __attrs__ = []

        cdef Factory factory
        cdef EntityAttribute attr
        cdef EntityType aliased
        cdef EntityType base_entity = None
        cdef PolymorphMeta poly_meta = None
        cdef tuple hints
        cdef object polymorph = self.__meta__.get("polymorph", None)

        for base in bases:
            if isinstance(base, EntityType):
                if base_entity is None:
                    base_entity = base
                else:
                    raise ValueError("More than one Entity base is not allowed")

        try:
            is_alias = self.__meta__["is_alias"] is True
            is_alias = is_alias and len(bases) == 1 and isinstance(bases[0], EntityType)
        except:
            is_alias = False

        if polymorph:
            if not isinstance(polymorph, PolymorphMeta):
                polymorph = PolymorphMeta(self, polymorph)
                self.__meta__["polymorph"] = polymorph
            poly_meta = <PolymorphMeta>polymorph

        if is_alias:
            aliased = <EntityType>bases[0]

            if not aliased.resolve_deferred():
                raise RuntimeError("Can't alias deferred entity")

            for v in aliased.__attrs__:
                if isinstance(v, EntityAttribute):
                    attr = (<EntityAttribute>v).clone()
                    attr._key_ = (<EntityAttribute>v)._key_

                    if isinstance(attr, Field):
                        fields.append(attr)
                    else:
                        __attrs__.append(attr)

                    if attr._key_:
                        setattr(self, attr._key_, attr)
        elif fields:
            for attr in fields:
                attr._key_ = attr._name_
                setattr(self, attr._name_, attr)
        else:
            hints = get_type_hints(self)

            if poly_meta and base_entity.__fields__:
                poly_join = None
                poly_relation = Relation(ManyToOne(base_entity, RelatedItem()))

                for attr in base_entity.__attrs__:
                    if attr.get_ext(PrimaryKey):
                        self_pk = Field(AutoImpl(), name=attr._name_) \
                            // ForeignKey(attr, on_delete="CASCADE", on_update="CASCADE") \
                            // PrimaryKey()
                        fields.append(self_pk)
                        (<EntityAttribute>self_pk)._key_ = attr._key_
                        setattr(self, attr._key_, self_pk)

                        if poly_join is None:
                            poly_join = self_pk == attr
                        else:
                            poly_join &= self_pk == attr
                    elif attr._key_:
                        # TODO: stateless
                        self_attr = RelatedAttribute(poly_relation, name=attr._key_)
                        __attrs__.append(self_attr)
                        (<EntityAttribute>self_attr)._key_ = attr._key_
                        if attr._key_:
                            setattr(self, attr._key_, self_attr)

                (<Relation>poly_relation)._default_ = poly_join
                __attrs__.append(poly_relation)
                poly_meta.add(self.__meta__["polymorph_id"], self, poly_relation)


            if hints[1] is not None:
                for name, type in (<dict>hints[1]).items():
                    if poly_meta and hasattr(base_entity, name):
                        continue

                    factory = Factory.create(type)
                    if factory is None:
                        continue

                    try:
                        value = attrs[name]
                    except KeyError:
                        value = getattr(self, name, None)
                        if isinstance(value, EntityAttribute):
                            value = value.clone()

                    attr_type = factory.hints[0]

                    if issubclass(attr_type, EntityAttribute):
                        attr = init_attribute(factory(), value)
                        attr._key_ = name
                        if not attr._name_:
                            attr._name_ = name

                        if isinstance(attr, Field):
                            fields.append(attr)
                        else:
                            __attrs__.append(attr)

                        setattr(self, name, attr)

        # add dynamic attributes
        for k, v in attrs.items():
            if isinstance(v, DynamicAttribute):
                __attrs__.append(v)
                (<DynamicAttribute>v)._key_ = k

        self.__fix_entries__ = None
        self.__deferred__ = []
        self.__fields__ = tuple(fields)
        self.__attrs__ = tuple(fields + __attrs__)
        self.__deps__ = set()

        pk = []

        for i, attr in enumerate(self.__attrs__):
            attr._index_ = i

            if not attr.bind(self):
                self.__deferred__.append(attr)

            if isinstance(attr, Field) and attr.get_ext(PrimaryKey):
                pk.append(attr)

        self.__pk__ = tuple(pk)

        if not self.__deferred__:
            self.__entity_ready__()

    def __init__(self, *args, _root=False, __fields__=None, is_alias=False, **kwargs):
        type.__init__(self, *args)

        if _root is False:
            if not is_alias:
                # TODO: better solution for registering entity early for resolving
                module = PyImport_Import(self.__module__)
                mdict = PyModule_GetDict(module)
                (<object>mdict)[args[0]] = self

                self.__register__()

    @property
    def __meta__(self):
        return <object>self.meta

    @property
    def __registry__(self):
        return <object>self.registry

    @property
    def __qname__(self):
        try:
            schema = self.__meta__["schema"]
        except KeyError:
            return self.__name__
        else:
            if schema is None:
                return self.__name__
            else:
                return f"{schema}.{self.__name__}"

    @staticmethod
    def __prepare__(*args, **kwargs):
        scope = type.__prepare__(*args, **kwargs)
        scope["__slots__"] = ()
        return scope

    def __repr__(self):
        aliased = get_alias_target(self)
        if aliased is self:
            return "<Entity %s>" % self.__qname__
        else:
            return "<Alias of %r>" % aliased

    def alias(self, str alias = None):
        if alias is None:
            # alias = "".join(random.choices(string.ascii_letters, k=6))
            alias = ""

        aliased = get_alias_target(self)
        return EntityType(alias, (aliased,), {}, name=alias, schema=None, is_alias=True)

    def __dealloc__(self):
        Py_XDECREF(self.meta)
        Py_XDECREF(self.registry)

    cdef object resolve_deferred(self):
        cdef EntityAttribute attr
        cdef list deferred = self.__deferred__
        cdef int index = len(deferred) - 1

        if index < 0:
            return True

        while index >= 0:
            attr = deferred[index]
            if attr.bind(self):
                deferred.pop(index)
            index -= 1

        if len(deferred) == 0:
            self.__entity_ready__()
            return True
        else:
            return False

    cpdef object __entity_ready__(self):
        for attr in self.__attrs__:
            self.__deps__ |= attr._deps_


cpdef bint is_entity_alias(object o):
    cdef dict meta
    if isinstance(o, EntityType):
        meta = (<EntityType>o).__meta__
        return meta.get("is_alias", False) is True
    return False


cpdef EntityType get_alias_target(EntityType o):
    cdef tuple mro
    if is_entity_alias(o):
        mro = o.__mro__
        return mro[1]
    else:
        return o

cdef EntityAttribute init_attribute(EntityAttribute by_type, object value):
    cdef EntityAttribute res
    cdef EntityAttributeExt ext

    if isinstance(value, EntityAttribute):
        res = <EntityAttribute>value

        if res._impl is None:
            res._impl = by_type._impl

        if by_type._exts_:
            res._exts_[0:0] = by_type._exts_
        return res
    elif isinstance(value, EntityAttributeExt):
        ext = <EntityAttributeExt>value

        if ext.attr:
            by_type._exts_.extend(ext.attr._exts_)
        else:
            by_type._exts_.append(ext)
            by_type._exts_.extend(ext._tmp)

        for ext in by_type._exts_:
            ext.attr = by_type

        return by_type
    else:
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
                self._impl = impl
            else:
                self._impl_ = None
                self._impl = impl
        else:
            self._impl = None

        self._exts_ = []
        self._deps_ = set()

    def __floordiv__(EntityAttribute self, EntityAttributeExt other):
        if other.attr is not None:
            if other.attr is not self:
                raise RuntimeError("Can't rebind entity attribute")
        else:
            other.attr = self
        self._exts_.append(other)
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

    cdef object bind(self, EntityType entity):
        if self._entity_ is not None and self._entity_ is not entity:
            raise RuntimeError("Can't rebind entity attribute")
        self._entity_ = entity

        if self._impl_ is None:
            if self._impl is None:
                raise TypeError("Missing attribute implementation: %r" % self)

            if is_forward_decl(self._impl):
                try:
                    self._impl_ = new_instance_from_forward(self._impl)
                except NameError as e:
                    return False
            else:
                self._impl_ = self._impl
            self._impl = None

        if not self._impl_.inited:
            if self._impl_.init(self) is False:
                return False
            else:
                self._impl_.inited = True

        cdef EntityAttributeExt ext
        for ext in self._exts_:
            if not ext.bound and not ext.bind(self):
                return False
            ext.bound = True

        return True

    cpdef clone(self):
        raise NotImplementedError()

    cpdef clone_exts(self, EntityAttribute attr):
        cdef EntityAttributeExt ext

        res = []
        for ext in self._exts_:
            ext = ext.clone()
            ext.attr = attr
            res.append(ext)
        return res

    cpdef object get_ext(self, ext_type):
        for ext in self._exts_:
            if isinstance(ext, ext_type):
                return ext


cdef class DynamicAttribute(EntityAttribute):
    def __cinit__(self, *args, get, set=None, delete=None):
        self._get = get
        self._set = set
        self._delete = delete

    def __get__(self, instance, owner):
        return self._get(instance)

    def __set__(self, EntityBase instance, value):
        if self._set:
            self._set(instance, value)
        else:
            raise ValueError("Can't set attribute: '%s'" % self._key_)

    def __delete__(self, EntityBase instance):
        if self._delete:
            self._delete(instance)
        else:
            raise ValueError("Can't delete attribute: '%s'" % self._key_)

    cpdef clone(self):
        return type(self)(self._impl, get=self._get, set=self._set, delete=self._delete)

    def __repr__(self):
        return "<dynamic %s>" % self._key_


cdef class EntityAttributeExt:
    def __cinit__(self, *args, **kwargs):
        self._tmp = []
        self.bound = False

    def __floordiv__(EntityAttributeExt self, EntityAttributeExt other):
        if not self.attr and other.attr:
            self.attr = other.attr

        if self.attr:
            self.attr._exts_.append(self)
            self.attr._exts_.extend(self._tmp)
            return self.attr
        else:
            self._tmp.append(other)
            self._tmp.extend(other._tmp)
            return self

    cpdef object bind(self, EntityAttribute attr):
        # TODO: handle error, only if after bind is called
        # if self.attr is not None:
        #     if self.attr is not attr:
        #         raise RuntimeError("Can't rebind entity attribute extension")
        #     else:
        #         return
        self.attr = attr
        return True

    cpdef object clone(self):
        return type(self)()

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return repr(self) == repr(other)

    def __ne__(self, other):
        return repr(self) == repr(other)

    def __repr__(self):
        return f"@{type(self).__name__}()"


cdef class EntityAttributeImpl:
    def __cinit__(self, *args, **kwargs):
        self.inited = False

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
        return isinstance(self, type(other)) or isinstance(other, type(self))

    def __ne__(self, other):
        return not isinstance(self, type(other)) and not isinstance(other, type(self))


cdef inline state_set_value(PyObject* initial, PyObject* current, EntityAttribute attr, object value):
    cdef int idx = attr._index_
    cdef PyObject* iv = PyTuple_GET_ITEM(<object>initial, idx)
    cdef PyObject* cv = PyTuple_GET_ITEM(<object>current, idx)

    nv = (<EntityAttributeImpl>attr._impl_).state_set(<object>iv, <object>cv, value)
    Py_INCREF(<object>nv)
    PyTuple_SET_ITEM(<object>current, idx, <object>nv)


@cython.final
@cython.freelist(1000)
cdef class EntityState:

    # @staticmethod
    # cdef EntityState create_from_dict(EntityType entity, dict data):
    #     state = EntityState(entity)
    #     state.update(data, True)
    #     return state

    #TODO: refactor, use dict instead of tuple
    # remove create_from_dict
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
        Py_XDECREF(cv)
        PyTuple_SET_ITEM(<object>current, attr._index_, <object>NULL)

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


cdef class EntityBase:
    def __cinit__(self, state=None, **values):
        cdef EntityType model = type(self)

        if not model.resolve_deferred():
            print(model.__deferred__)
            raise RuntimeError("Entity is not resolved...")

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
            poly = (<dict>model.__meta__).get("polymorph", None)
            if poly is not None:
                poly_id = (<dict>model.__meta__).get("polymorph_id", None)
                if poly_id is not None:
                    poly_id = PolymorphMeta.normalize_id(poly_id)
                    for i, idf in enumerate(poly.id_fields):
                        setattr(self, idf, poly_id[i])
        else:
            self.__state__.init()

    @classmethod
    def __init_subclass__(cls, *, str name=None, Registry registry=None, bint _root=False, **meta):
        cdef EntityType ent = cls
        cdef EntityType parent_entity
        cdef int mro_length = len(cls.__mro__)
        cdef PyObject* _registry

        if name is not None:
            ent.__name__ = name

        meta_dict = {}
        _registry = <PyObject*>(<object>registry)

        for i in range(1, mro_length - 2):
            parent = cls.__mro__[i]
            if isinstance(parent, EntityType):
                parent_entity = <EntityType>parent

                if parent_entity.meta is not NULL:
                    meta_dict.update(<object>parent_entity.meta)

                if _registry is <PyObject*>None:
                    _registry = parent_entity.registry

        meta_dict.update(meta)
        meta_dict.pop("__fields__", None)

        Py_XDECREF(ent.meta)
        ent.meta = <PyObject*>(<object>meta_dict)
        Py_XINCREF(ent.meta)

        Py_XDECREF(ent.registry)
        ent.registry = _registry
        Py_XINCREF(ent.registry)


    @classmethod
    def __register__(cls):
        (<Registry>cls.__registry__).register(cls.__qname__, cls)

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
            if isinstance(value, EntityBase):
                res[attr._key_] = value.as_dict()
            elif isinstance(value, list):
                res[attr._key_] = [v.as_dict() for v in value]
            else:
                res[attr._key_] = value

        return res

    def __repr__(self):
        is_type = (type(self).__meta__.get("is_type", False))
        if is_type is True:
            return "<%s %r>" % (type(self).__qname__, self.as_dict())
        else:
            return "<%s %r>" % (type(self).__qname__, self.__pk__)


class Entity(EntityBase, metaclass=EntityType, registry=REGISTRY, _root=True):
    pass


@cython.final
cdef class DependencyList(list):
    cpdef add(self, EntityType item):
        cdef list self_ = <list>self

        try:
            idx = self_.index(item)
        except ValueError:
            idx = len(self)
            self_.append(item)

        for dep in sorted(item.__deps__, key=attrgetter("__name__")):
            try:
                didx = self_.index(dep)
            except ValueError:
                self_.insert(idx, dep)
                self.add(dep)
            else:
                if didx > idx:
                    self_.pop(didx)
                    self_.insert(idx, dep)
                    self.add(dep)

@cython.final
cdef class PolymorphMeta:
    def __cinit__(self, EntityType ent, object id):
        self.id_fields = PolymorphMeta.normalize_id(id)
        self.entities = {}

    def new_entity(self, poly_id):
        poly_id = PolymorphMeta.normalize_id(poly_id)
        cdef EntityType entity = None

        for entity, v in self.entities.items():
            id, rel = <tuple>v
            if id == poly_id:
                # data = {}
                # for i, f in enumerate(self.id_fields):
                #     data[f] = poly_id[i]
                return entity()

        raise ValueError("Unexpected value for polymorph id: %r" % poly_id)

    cdef object add(self, object id, EntityType entity, object relation):
        if not isinstance(relation, Relation):
            raise TypeError("Relation expected, but got: %r" % relation)

        id = PolymorphMeta.normalize_id(id)
        self.entities[entity] = (id, relation)

    @staticmethod
    cdef tuple normalize_id(object id):
        if not isinstance(id, tuple):
            if isinstance(id, list):
                return tuple(id)
            else:
                return (id,)
        return id

    cpdef list parents(self, EntityType entity):
        entity = get_alias_target(entity)
        cdef list result = []
        self._parents(entity, result)
        return result

    cdef object _parents(self, EntityType entity, list result):
        cdef Relation relation

        for x in self.entities.values():
            relation = <Relation>((<tuple>x)[1])
            if relation._entity_ is entity:
                result.append(relation)
                self._parents(relation._impl_._joined, result)


    cpdef list children(self, EntityType entity):
        cdef Relation relation
        entity = get_alias_target(entity)
        cdef list result = []

        for x in self.entities.values():
            relation = <Relation>((<tuple>x)[1])
            if relation._impl_._joined is entity:
                result.append(relation)

        return result
