import cython
import random
import string

from collections.abc import ItemsView
from operator import attrgetter

from cpython.object cimport PyObject
from cpython.ref cimport Py_DECREF, Py_INCREF, Py_XDECREF, Py_XINCREF
from cpython.tuple cimport PyTuple_SetItem, PyTuple_GetItem, PyTuple_New, PyTuple_GET_SIZE, PyTuple_SET_ITEM, PyTuple_GET_ITEM, PyTuple_Pack
from cpython.module cimport PyImport_Import, PyModule_GetDict

from ._field cimport Field, PrimaryKey
from ._relation cimport Relation
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
        cdef tuple hints

        try:
            is_alias = self.__meta__["is_alias"] is True
            is_alias = is_alias and len(bases) == 1 and isinstance(bases[0], EntityType)
        except:
            is_alias = False

        if is_alias:
            aliased = <EntityType>bases[0]

            for v in aliased.__attrs__:
                if isinstance(v, EntityAttribute):
                    attr = (<EntityAttribute>v).clone()
                    attr._key_ = (<EntityAttribute>v)._key_

                    if isinstance(attr, Field):
                        fields.append(attr)
                    else:
                        __attrs__.append(attr)

                    setattr(self, attr._key_, attr)
        elif fields:
            for attr in fields:
                attr._key_ = attr._name_
                setattr(self, attr._name_, attr)
        else:
            hints = get_type_hints(self)

            if hints[1] is not None:
                for name, type in (<dict>hints[1]).items():
                    factory = Factory.create(type)
                    if factory is None:
                        continue

                    try:
                        value = attrs[name]
                    except KeyError:
                        value = None

                    attr_type = factory.hints[0]

                    if issubclass(attr_type, EntityAttribute):
                        attr = init_attribute(factory(), value)
                        attr._key_ = name
                        if not attr._name_:
                            attr._name_ = name

                        # if isinstance(attr, Field):
                        #     fields.append(attr)
                        # elif isinstance(attr, Relation):
                        #     relations.append(attr)
                        if isinstance(attr, Field):
                            fields.append(attr)
                        else:
                            __attrs__.append(attr)

                        setattr(self, name, attr)

        self.__fix_entries__ = None
        self.__deferred__ = []
        self.__fields__ = tuple(fields)
        self.__attrs__ = tuple(fields + __attrs__)

        pk = []

        for i, attr in enumerate(self.__attrs__):
            attr._index_ = i

            if not attr.bind(self):
                self.__deferred__.append(attr)

            if isinstance(attr, Field) and attr.get_ext(PrimaryKey):
                pk.append(attr)

        self.__pk__ = tuple(pk)

    def __init__(self, *args, _root=False, __fields__=None, is_alias=False, **kwargs):
        type.__init__(self, *args)

        if _root is False:
            # if __fields__ is present, this entitiy, not created normally
            if not __fields__ and not is_alias:
                module = PyImport_Import(self.__module__)
                mdict = PyModule_GetDict(module)

                # XXX little hacky, insert class instance into module dict before call register
                (<object>mdict)[args[0]] = self

            if not is_alias:
                self.__register__()

    @property
    def __meta__(self):
        return <object>self.meta

    @property
    def __registry__(self):
        return <object>self.registry

    @property
    def __deps__(self):
        cdef EntityAttribute attr
        if self.deps is None:
            deps = set()

            for attr in self.__attrs__:
                attr._impl_  # XXX prettier mode, for forcing implementation init
                deps |= attr._deps_

            self.deps = deps
        return self.deps

    @staticmethod
    def __prepare__(*args, **kwargs):
        scope = type.__prepare__(*args, **kwargs)
        scope["__slots__"] = ()
        return scope

    def __repr__(self):
        return "<Entity %s>" % self.__name__

    def alias(self, str alias = None):
        if alias is None:
            alias = "".join(random.choices(string.ascii_letters, k=6))

        aliased = get_alias_target(self)
        return EntityType(alias, (aliased,), {}, name=alias, schema=None, is_alias=True)

    def __dealloc__(self):
        Py_XDECREF(self.meta)
        Py_XDECREF(self.registry)

    cdef object resolve_deferred(self):
        cdef EntityAttribute attr
        cdef list deferred = self.__deferred__
        cdef int index = len(deferred) - 1

        while index >= 0:
            attr = deferred[index]
            if attr.bind(self):
                deferred.pop(index)
            index -= 1

        return len(deferred) == 0


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


cdef class EntityAttribute(Expression):
    def __cinit__(self, *args, **kwargs):
        if args:
            self._impl = args[0]
        else:
            self._impl = None

        self._impl_ = None
        self._exts_ = []
        self._deps_ = set()

    def __floordiv__(Field self, EntityAttributeExt other):
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
                raise TypeError("Missing attribute implementation")

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
        return type(self) is type(other)

    def __ne__(self, other):
        return type(self) is not type(other)


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

    @staticmethod
    cdef EntityState create_from_dict(EntityType entity, dict data):
        state = EntityState(entity)
        state.update(data, True)
        return state

    def __cinit__(self, EntityType entity, tuple initial_data=None):
        cdef int length = len(entity.__attrs__)
        self.entity = entity
        self.initial = initial_data if initial_data is not None else PyTuple_New(length)
        self.current = PyTuple_New(length)
        self.field_count = len(entity.__fields__)

        self.init_current()

    cdef object init_current(self):
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


    cdef object update(self, dict data, bint is_initial):
        cdef EntityAttribute attr
        cdef EntityType entity = self.entity
        cdef PyObject* current = <PyObject*>self.initial if is_initial else <PyObject*>self.current
        cdef PyObject* initial = <PyObject*>self.initial

        for k, v in data.items():
            attr = getattr(entity, k)
            state_set_value(initial, current, attr, v)

    cdef bint set_value(self, EntityAttribute attr, object value):
        state_set_value(<PyObject*>self.initial, <PyObject*>self.current, attr, value)
        return 1

    cdef object get_value(self, EntityAttribute attr):
        cdef PyObject* initial = <PyObject*>self.initial
        cdef PyObject* current = <PyObject*>self.current
        cdef PyObject* cv = PyTuple_GET_ITEM(<object>current, attr._index_)

        if cv is <PyObject*>NOTSET:
            cv = PyTuple_GET_ITEM(<object>initial, attr._index_)
        return <object>cv

    cdef void del_value(self, EntityAttribute attr):
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
        self.init_current()

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

        if state:
            if isinstance(state, EntityState):
                self.__state__ = state
            elif isinstance(state, dict):
                self.__state__ = EntityState.create_from_dict(model, state)
            else:
                raise TypeError("Unsupported state argumented: %r" % state)

        if values:
            if not self.__state__:
                self.__state__ = EntityState.create_from_dict(model, values)
            else:
                self.__state__.update(values, True)

        if not self.__state__:
            self.__state__ = EntityState(model)

        # cdef tuple fields = <tuple>model.__fields__
        # cdef tuple fdata = PyTuple_New(PyTuple_GET_SIZE(fields))
        # self.__fstate__ = FieldState(fields, fdata)

        # cdef tuple relations = <tuple>model.__relations__
        # self.__rstate__ = RelationState(relations)

    @classmethod
    def __init_subclass__(cls, *, str name=None, Registry registry=None, bint _root=False, **meta):
        cdef EntityType ent = cls
        cdef EntityType parent

        if name is not None:
            ent.__name__ = name

        meta_dict = dict(meta)
        meta_dict.pop("__fields__", None)
        Py_XDECREF(ent.meta)
        ent.meta = <PyObject*>(<object>meta_dict)
        Py_XINCREF(ent.meta)

        if registry is not None:
            Py_XDECREF(ent.registry)
            ent.registry = <PyObject*>(<object>registry)
            Py_XINCREF(ent.registry)
        else:
            length = len(cls.__mro__)
            for i in range(1, length - 2):
                parent = cls.__mro__[i]

                if parent.registry is not NULL:
                    Py_XDECREF(ent.registry)
                    ent.registry = parent.registry
                    Py_XINCREF(ent.registry)
                    break

        # if _root is False:
        #     cls.__register__()

    @classmethod
    def __register__(cls):
        try:
            schema = cls.__meta__["schema"]
        except KeyError:
            name = cls.__name__
        else:
            if schema is None:
                name = cls.__name__
            else:
                name = f"{schema}.{cls.__name__}"

        (<Registry>cls.__registry__).register(name, cls)


    @property
    def __pk__(self):
        cdef EntityAttribute attr
        cdef EntityType ent = type(self)
        cdef EntityState state = self.__state__
        cdef int length = len(ent.__pk__)
        cdef tuple res = PyTuple_New(length)
        cdef bint is_set = False

        for idx, attr in enumerate(ent.__pk__):
            val = state.get_value(attr)

            if val is NOTSET:
                Py_INCREF(<object>None)
                PyTuple_SET_ITEM(<object>res, idx, <object>None)
            else:
                is_set = True
                Py_INCREF(<object>val)
                PyTuple_SET_ITEM(<object>res, idx, <object>val)

        if is_set:
            return res
        else:
            return ()

    def __hash__(self):
        return hash(type(self)) ^ hash(self.__pk__)

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
            value = self.__state__.get_value(attr)

            if value is NOTSET:
                return self.__next__()
            else:
                return (attr, value)
        else:
            raise StopIteration()

    def __json__(self):
        ctx = SerializerCtx()
        return EntitySerializer(self, ctx)

    def serialize(self, ctx=None):
        if ctx is None:
            ctx = SerializerCtx()
        return EntitySerializer(self, ctx)

    def as_dict(self):
        cdef dict res = {}

        for attr, value in self:
            res[attr._key_] = value

        return res


class Entity(EntityBase, metaclass=EntityType, registry=REGISTRY, _root=True):
    def __repr__(self):
        return "<%s %r>" % (type(self).__name__, self.__pk__)


@cython.final
cdef class DependencyList(list):
    cpdef add(self, EntityType item):
        try:
            idx = self.index(item)
        except ValueError:
            idx = len(self)
            self.append(item)

        for dep in sorted(item.__deps__, key=attrgetter("__name__")):
            if dep not in self:
                self.insert(idx, dep)
            self.add(dep)

