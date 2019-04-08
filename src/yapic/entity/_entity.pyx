import cython
import random
import string

from cpython.object cimport PyObject
from cpython.ref cimport Py_DECREF, Py_INCREF, Py_XDECREF, Py_XINCREF
from cpython.tuple cimport PyTuple_SetItem, PyTuple_GetItem, PyTuple_New, PyTuple_GET_SIZE, PyTuple_SET_ITEM, PyTuple_GET_ITEM, PyTuple_Pack

from ._field cimport Field, PrimaryKey
from ._relation cimport Relation, RelationState
from ._factory cimport Factory, get_type_hints, new_instance_from_forward, is_forward_decl
from ._expression cimport Visitor


cdef class EntityType(type):
    def __cinit__(self, *args, **kwargs):
        # XXX: DONT REMOVE
        (name, bases, attrs) = args


        cdef list fields = []
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

            for k, v in aliased.__dict__.items():
                if isinstance(v, EntityAttribute):
                    attr = (<EntityAttribute>v).clone()
                    attr._attr_name_in_class = k

                    if isinstance(attr, Field):
                        fields.append(attr)
                    else:
                        __attrs__.append(attr)

                    # if isinstance(v, Field):
                    #     fields.append(attr)
                    # elif isinstance(v, Relation):
                    #     relations.append(attr)

                    setattr(self, k, attr)
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
                        attr._attr_name_in_class = name
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

        self.__fields__ = tuple(fields)

        for i, attr in enumerate(fields):
            attr._index_ = i
            attr.bind(self)

        for attr in __attrs__:
            i += 1
            attr._index_ = i
            attr.bind(self)

        self.__attrs__ = tuple(fields + __attrs__)

    def __init__(self, *args, **kwargs):
        type.__init__(self, *args)

    @property
    def __meta__(self):
        return <object>self.meta

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
            by_type._exts_ = ext.attr._exts_
        else:
            by_type._exts_ = [ext]
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
        self._exts_ = []

        if self._impl is not None \
                and not is_forward_decl(self._impl) \
                and not isinstance(self._impl, EntityAttributeImpl):
            raise ValueError("Invalid attribute implementation: %r" % self._impl)

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
            return (<EntityBase>instance).__state__.get_value(self)
        else:
            raise TypeError("Instance must be 'None' or 'EntityBase'")

    def __set__(self, EntityBase instance, value):
        instance.__state__.set_value(self, value)

    def __delete__(self, EntityBase instance):
        instance.__state__.del_value(self)

    @property
    def _impl_(self):
        if is_forward_decl(self._impl):
            self._impl = new_instance_from_forward(self._impl)
            if isinstance(self._impl, EntityAttributeImpl):
                try:
                    (<EntityAttributeImpl>self._impl).init(self._entity_)
                    (<EntityAttributeImpl>self._impl).inited = True
                except Exception as e:
                    raise RuntimeError("Can't init attribute impl, original exception: %s" % e)
            else:
                raise ValueError("Invalid attribute implementation: %r" % self._impl)
        return self._impl

    cdef bind(self, EntityType entity):
        if self._entity_ is not None:
            raise RuntimeError("Can't rebind entity attribute")
        self._entity_ = entity

        cdef EntityAttributeExt ext
        for ext in self._exts_:
            ext.bind(self)

        if self._impl is None:
            raise RuntimeError("Missing attribute implementation")

        if isinstance(self._impl, EntityAttributeImpl) and not (<EntityAttributeImpl>self._impl).inited:
            (<EntityAttributeImpl>self._impl).init(entity)
            (<EntityAttributeImpl>self._impl).inited = True

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
        if self.attr is not None:
            if self.attr is not attr:
                raise RuntimeError("Can't rebind entity attribute extension")
            else:
                return
        self.attr = attr

    cpdef object clone(self):
        return type(self)()


cdef class EntityAttributeImpl:
    def __cinit__(self, *args, **kwargs):
        self.inited = False

    cpdef init(self, EntityType entity):
        raise NotImplementedError()

    cpdef object clone(self):
        raise NotImplementedError()

    cdef PyObject* state_set(self, PyObject* current, PyObject* value):
        return value

    cdef PyObject* state_get(self, PyObject* current):
        return current

    # cdef object state_del(self, PyObject*& current):
    #     raise NotImplementedError()


# cdef class EntityAliasExpression(Expression):
#     def __cinit__(self, EntityType entity, str alias):
#         self.entity = entity
#         self.value = alias
#         self.__fields__ = tuple((<EntityAttribute>item).clone() for item in entity.__fields__)
#         self.__relations__ = tuple((<EntityAttribute>item).clone() for item in entity.__relations__)

#     def __repr__(self):
#         return "<Alias %s AS %s>" % (self.entity, self.value)

#     cpdef alias(self, str alias):
#         return EntityAliasExpression(self.entity, self.value)

#     cpdef visit(self, Visitor visitor):
#         return visitor.visit_entity_alias(self)


@cython.final
@cython.freelist(1000)
cdef class FieldState:
    def __cinit__(self, tuple fields, tuple data):
        self.fields = fields
        self.data = data
        self.dirty = PyTuple_New(PyTuple_GET_SIZE(data))

    cdef bint set_value(self, int index, object value):
        cdef PyObject* fields = <PyObject*>self.fields
        cdef PyObject* data = <PyObject*>self.data
        cdef PyObject* dirty = <PyObject*>self.dirty
        cdef PyObject* cv
        cdef PyObject* newValue = <PyObject*>value
        cdef Field field

        cv = PyTuple_GET_ITEM(<object>data, index)
        if cv == NULL:
            Py_INCREF(<object>newValue)
            PyTuple_SET_ITEM(<object>dirty, index, <object>newValue)
        else:
            field = <Field>PyTuple_GET_ITEM(<object>fields, index)
            if not field.values_is_eq(<object>cv, <object>newValue):
                cv = PyTuple_GET_ITEM(<object>dirty, index)
                if cv != NULL:
                    Py_DECREF(<object>cv)
                Py_INCREF(<object>newValue)
                PyTuple_SET_ITEM(<object>dirty, index, <object>newValue)

    cdef object get_value(self, int index):
        cdef PyObject* container = <PyObject*>self.dirty
        cdef PyObject* cv = PyTuple_GET_ITEM(<object>container, index)

        if cv == NULL:
            container = <PyObject*>self.data
            cv = PyTuple_GET_ITEM(<object>container, index)
            if cv == NULL:
                return None

        Py_INCREF(<object>cv)
        return <object>cv

    cdef void del_value(self, int index):
        cdef PyObject* container = <PyObject*>self.dirty
        cdef PyObject* cv = PyTuple_GET_ITEM(<object>container, index)
        if cv != NULL:
            Py_DECREF(<object>cv)
        PyTuple_SET_ITEM(<object>container, index, <object>NULL)

    @property
    def changes(self):
        cdef PyObject* fields = <PyObject*>self.fields
        cdef PyObject* data = <PyObject*>self.data
        cdef PyObject* dirty = <PyObject*>self.dirty
        cdef PyObject* cv
        cdef PyObject* ov
        cdef PyObject* field

        for i from 0 <= i < PyTuple_GET_SIZE(<object>dirty):
            cv = PyTuple_GET_ITEM(<object>dirty, i)
            if cv == NULL:
                continue

            ov = PyTuple_GET_ITEM(<object>data, i)
            if ov == NULL:
                ov = <PyObject*>None

            field = PyTuple_GET_ITEM(<object>fields, i)
            yield PyTuple_Pack(3, <PyObject*>field, ov, cv)

    cpdef reset(self):
        cdef PyObject* data = <PyObject*>self.data
        cdef PyObject* dirty = <PyObject*>self.dirty
        cdef PyObject* cv
        cdef int l = PyTuple_GET_SIZE(<object>dirty)

        for i from 0 <= i < l:
            cv = PyTuple_GET_ITEM(<object>dirty, i)
            if cv == NULL:
                cv = PyTuple_GET_ITEM(<object>data, i)
                if cv == NULL:
                    cv = <PyObject*>None
                Py_INCREF(<object>cv)
                PyTuple_SET_ITEM(<object>dirty, i, <object>cv)

        self.data = <tuple>dirty
        self.dirty = PyTuple_New(l)

    def __iter__(self):
        cdef PyObject* fields = <PyObject*>self.fields
        cdef PyObject* data = <PyObject*>self.data
        cdef PyObject* dirty = <PyObject*>self.dirty
        cdef PyObject* cv
        cdef PyObject* field

        for i from 0 <= i < PyTuple_GET_SIZE(<object>dirty):
            cv = PyTuple_GET_ITEM(<object>dirty, i)
            if cv == NULL:
                cv = PyTuple_GET_ITEM(<object>data, i)
                if cv == NULL:
                    cv = <PyObject*>None

            field = PyTuple_GET_ITEM(<object>fields, i)
            yield PyTuple_Pack(2, <PyObject*>(<EntityAttribute>field)._name_, cv)

    def __repr__(self):
        return "<FieldState %r>" % (dict(self),)




    # cdef bint set_value(self, int index, object value):
    #     cdef PyObject* data = <PyObject*>self.data
    #     cdef PyObject* cv = PyTuple_GET_ITEM(<object>data, index)

    #     if cv is not NULL:
    #         Py_DECREF(<object>cv)

    #     Py_INCREF(<object>value)
    #     PyTuple_SET_ITEM(<object>data, index, value)

    # cdef object get_value(self, int index):
    #     cdef PyObject* data = <PyObject*>self.data
    #     cdef PyObject* cv = PyTuple_GET_ITEM(<object>data, index)
    #     Py_INCREF(<object>cv)
    #     return <object>cv

    # cdef void del_value(self, int index):
    #     cdef PyObject* data = <PyObject*>self.data
    #     cdef PyObject* cv = PyTuple_GET_ITEM(<object>data, index)

    #     if cv is not NULL:
    #         Py_DECREF(<object>cv)

    #     PyTuple_SET_ITEM(<object>data, index, <object>NULL)

    # cpdef reset(self):
    #     self.data = PyTuple_New(PyTuple_GET_SIZE(self.relations))

    def __repr__(self):
        return "<RelationState>"


@cython.final
@cython.freelist(1000)
cdef class EntityState:
    @staticmethod
    cdef EntityState create_from_dict(EntityType entity, dict data):
        state = EntityState(entity)
        state.update(entity, data, True)
        return state

    def __cinit__(self, EntityType entity, tuple initial_data=None):
        self.data = PyTuple_New(len(entity.__attrs__))
        self.field_count = len(entity.__fields__)

    cdef object update(self, EntityType entity, dict data, bint is_initial):
        cdef EntityAttribute attr

        for k, v in data.items():
            attr = getattr(entity, k)
            self.set_value(attr, v)

    cdef bint set_value(self, EntityAttribute attr, object value):
        cdef int index = attr._index_
        cdef PyObject* data = <PyObject*>self.data
        cdef EntityAttributeImpl impl = <EntityAttributeImpl>attr._impl_
        cdef PyObject* current = PyTuple_GET_ITEM(<object>data, index)
        cdef PyObject* newValue = impl.state_set(current, <PyObject*>value)

        Py_XINCREF(newValue)
        Py_XDECREF(current)
        PyTuple_SET_ITEM(<object>data, index, <object>newValue)

    cdef object get_value(self, EntityAttribute attr):
        cdef int index = attr._index_
        cdef PyObject* data = <PyObject*>self.data
        cdef PyObject* current = PyTuple_GET_ITEM(<object>data, index)
        cdef EntityAttributeImpl impl = <EntityAttributeImpl>attr._impl_
        return <object>impl.state_get(current)

    cdef void del_value(self, EntityAttribute attr):
        cdef int index = attr._index_
        cdef PyObject* data = <PyObject*>self.data
        cdef PyObject* current = PyTuple_GET_ITEM(<object>data, index)

        Py_XDECREF(current)
        PyTuple_SET_ITEM(<object>data, index, <object>NULL)

    cdef list data_for_insert(self, EntityType entity):
        cdef list res = []
        cdef PyObject* data = <PyObject*>self.data
        cdef PyObject* cv
        cdef EntityAttribute field
        cdef EntityAttributeImpl impl

        for field in entity.__fields__:
            cv = PyTuple_GET_ITEM(<object>data, field._index_)
            cv = (<EntityAttributeImpl>field._impl_).state_get(cv)

            if field.get_ext(PrimaryKey) and cv is NULL:
                continue

            if cv is NULL:
                cv = <PyObject*>field._default_

            res.append((field, <object>cv))

        return res

    cdef list data_for_update(self, EntityType entity):
        pass


    cpdef reset(self):
        pass


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
                self.__state__.update(model, values, True)

        if not self.__state__:
            self.__state__ = EntityState(model)

        # cdef tuple fields = <tuple>model.__fields__
        # cdef tuple fdata = PyTuple_New(PyTuple_GET_SIZE(fields))
        # self.__fstate__ = FieldState(fields, fdata)

        # cdef tuple relations = <tuple>model.__relations__
        # self.__rstate__ = RelationState(relations)

    @classmethod
    def __init_subclass__(cls, *, str name=None, **meta):
        cdef EntityType ent = cls
        if name is not None:
            cls.__name__ = name

        meta_dict = dict(meta)
        Py_XDECREF(ent.meta)
        ent.meta = <PyObject*>(<object>meta_dict)
        Py_XINCREF(ent.meta)


class Entity(EntityBase, metaclass=EntityType):
    def __repr__(self):
        return "<%s %s>" % (type(self).__name__, "TODO: PK_VALUE")
