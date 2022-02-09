import cython
import hashlib
from cpython.object cimport PyObject
from cpython.weakref cimport PyWeakref_NewRef, PyWeakref_GetObject
from cpython.module cimport PyImport_Import, PyModule_GetDict

from ._expression cimport Expression, Visitor
from ._entity cimport EntityType, EntityBase, EntityAttribute, EntityAttributeExt, EntityAttributeExtGroup, EntityAttributeImpl, get_alias_target
from ._factory cimport ForwardDecl, get_type_hints, new_instance, new_instance_from_forward, is_forward_decl
from ._field_impl cimport AutoImpl


cdef class Field(EntityAttribute):
    def __cinit__(self, impl = None, *, name = None, default = None, size = None, nullable = None, on_update = None):
        self._default_ = default
        self._name_ = name
        self.type_cache = {}
        self.nullable = nullable
        self.on_update = on_update

        if size is not None:
            if isinstance(size, list) or isinstance(size, tuple):
                if len(size) == 2:
                    self.min_size = size[0]
                    self.max_size = size[1]
                else:
                    raise ValueError("TODO:")

            elif isinstance(size, int):
                self.min_size = 0
                self.max_size = size
            else:
                raise ValueError("TODO:")
        else:
            self.min_size = -1
            self.max_size = -1

    def __getattr__(self, key):
        return self._impl_.getattr(self, key)

    def __getitem__(self, key):
        return self._impl_.getitem(self, key)

    cdef object init(self, EntityType entity):
        if self.nullable is None:
            self.nullable = True
            if self.get_ext(PrimaryKey):
                self.nullable = False
            elif not callable(self._default_):
                self.nullable = bool(self._default_ is None)
            elif isinstance(self._default_, Expression):
                self.nullable = False
        return EntityAttribute.init(self, entity)

    cpdef clone(self):
        cdef Field res = type(self)(self._impl_,
            name=self._name_,
            default=self._default_,
            size=(self.min_size, self.max_size),
            nullable=self.nullable,
            on_update=self.on_update)
        res._exts_ = self.clone_exts(res)
        res._deps_ = self._deps_.clone()
        res.type_cache = self.type_cache
        return res

    cpdef StorageType get_type(self, StorageTypeFactory factory):
        try:
            return self.type_cache[factory]
        except KeyError:
            type = factory.create(self)
            self.type_cache[factory] = type
            return type

    def __repr__(self):
        cdef EntityType entity = self.get_entity()
        if entity:
            return "<Field %s: %s of %s>" % (self._name_, self._impl_, entity)
        else:
            return "<Field %s: %s (unbound)>" % (self._name_, self._impl_)

    cpdef visit(self, Visitor visitor):
        return visitor.visit_field(self)

    cpdef copy_into(self, EntityAttribute other):
        EntityAttribute.copy_into(self, other)
        cdef Field other_field = other
        other_field.type_cache = self.type_cache

        if self.min_size >= 0:
            other_field.min_size = self.min_size

        if self.max_size >= 0:
            other_field.max_size = self.max_size

        other_field.nullable = self.nullable
        other_field.on_update = self.on_update


# field_proxy_attrs = ("__proxied__", "__repr__", "clone")

# cdef class FieldProxy(Field):
#     def __cinit__(self, Field field):
#         self.__proxied__ = field

#     def __getattribute__(self, key):
#         if key in field_proxy_attrs:
#             return object.__getattribute__(self, key)
#         else:
#             return getattr(self.__proxied__, key)

#     def __getitem__(self, key):
#         return self.__proxied__[key]

#     def __repr__(self):
#         return "<FieldProxy %r>" % self.__proxied__

#     cpdef clone(self):
#         return type(self)(self.__proxied__.clone())


cdef class FieldExtension(EntityAttributeExt):
    pass


cdef class StorageType:
    cpdef object encode(self, object value):
        raise NotImplementedError()

    cpdef object decode(self, object value):
        raise NotImplementedError()


cdef class StorageTypeFactory:
    cpdef StorageType create(self, Field field):
        raise NotImplementedError()


cdef class PrimaryKey(FieldExtension):
    pass


cdef class AutoIncrement(FieldExtension):
    def __cinit__(self, object sequence=None):
        self._seq_arg = sequence

    cpdef object bind(self):
        cdef EntityType entity
        cdef EntityType aliased
        cdef EntityAttribute attr = self.get_attr()

        if self.sequence is None:
            if self._seq_arg is None:
                entity = attr.get_entity()
                aliased = get_alias_target(entity)

                if entity is aliased:
                    schema = entity.get_meta("schema", "public")
                    name = f"{entity.__name__}_{attr._name_}_seq"
                    try:
                        self.sequence = entity.get_registry()[name if schema == "public" else f"{schema}.{name}"]
                    except KeyError:
                        self.sequence = EntityType(name, (EntityBase,), {}, schema=schema, registry=entity.get_registry(), is_sequence=True)
                else:
                    self.sequence = getattr(aliased, attr._key_).get_ext(AutoIncrement).sequence
            elif isinstance(self._seq_arg, EntityType):
                self.sequence = self._seq_arg
            elif isinstance(self._seq_arg, (list, tuple)):
                if len(self._seq_arg) == 2:
                    schema, name = self._seq_arg
                elif len(self._seq_arg) == 1:
                    schema = "public"
                    name = self._seq_arg[0]
                else:
                    raise ValueError(f"Invalid sequence value: {self._seq_arg}")

                entity = get_alias_target(attr.get_entity())

                try:
                    self.sequence = entity.get_registry()[name if not schema or schema == "public" else f"{schema}.{name}"]
                except KeyError:
                    self.sequence = EntityType(name, (EntityBase,), {}, schema=schema, registry=entity.get_registry(), is_sequence=True)

        attr._deps_.add_entity(self.sequence)
        return FieldExtension.bind(self)

    cpdef object clone(self):
        return type(self)(self._seq_arg)

    def __repr__(self):
        return "@AutoIncrement(%r)" % self.sequence.__qname__



cdef class Index(FieldExtension):
    def __cinit__(self, *, str expr = None, str name = None, str method = "btree", bint unique = False, str collate = None):
        self.name = name
        self.method = method
        self.unique = unique
        self.collate = collate
        self.expr = expr

    cpdef object clone(self):
        return type(self)(expr=self.expr, name=self.name, method=self.method, unique=self.unique, collate=self.collate)

    cpdef object init(self, EntityAttribute attr):
        if not self.name:
            self.name = f"idx_{attr.get_entity().__name__}__{attr._name_}"
        self.group_by = self.name

        FieldExtension.init(self, attr)

    def __repr__(self):
        return "@%sIndex(%s USING %s ON %s %s)" % ("Unique" if self.unique else "", self.name, self.method, self.expr or self.get_attr()._name_, self.collate or "")


cdef class Unique(FieldExtension):
    pass


# todo: faster eval, with Py_CompileString(ref, "<string>", Py_eval_input); PyEval_EvalCode

_CodeType = type(compile("1", "<string>", "eval"))

cdef class ForeignKey(FieldExtension):
    @classmethod
    def validate_group(self, EntityAttributeExtGroup group):
        cdef tuple items = group.items
        cdef ForeignKey main = items[0]
        cdef ForeignKey fk

        for i in range(1, len(items)):
            fk = <ForeignKey>items[i]
            if fk.ref.get_entity() != main.ref.get_entity():
                raise ValueError(f"Can't use fields from different entities in the same foreign key: '{main.name}'")
            elif fk.on_update != main.on_update:
                raise ValueError(f"Can't use different 'on_update' value in the same foreign key: '{main.name}'")
            elif fk.on_delete != main.on_delete:
                raise ValueError(f"Can't use different 'on_update' value in the same foreign key: '{main.name}'")

        group.name = main.name

    def __cinit__(self, field, *, str name = None, str on_update = "RESTRICT", str on_delete = "RESTRICT"):
        self.ref = None

        if isinstance(field, str):
            self._ref = compile(field, field, "eval")
        elif isinstance(field, _CodeType):
            self._ref = field
        elif not isinstance(field, Field):
            raise TypeError("Incorrect argument for ForeignKey field parameter")
        else:
            self.ref = self._ref = field

        self.name = name
        self.on_update = on_update
        self.on_delete = on_delete

    cpdef object init(self, EntityAttribute attr):
        cdef Index index = attr.get_ext(Index)
        if not index and not attr.get_ext(PrimaryKey):
            index = Index()
            attr // index
            index.init(attr)

        if self.name is None:
            self.group_by = attr._name_
        else:
            self.group_by = self.name

        FieldExtension.init(self, attr)


    cpdef object bind(self):
        if FieldExtension.bind(self) is False:
            return False

        cdef Field field = self.attr
        cdef EntityType field_entity

        if self.ref is None:
            field_entity = field.get_entity()
            module = PyImport_Import(field_entity.__module__)
            mdict = PyModule_GetDict(module)
            ldict = {field_entity.__qualname__.split(".").pop(): field_entity}
            ldict.update(field_entity.__registry__.locals)
            try:
                self.ref = eval(self._ref, <object>mdict, <object>ldict)
            except NameError as e:
                return False

        field._deps_.add_entity(self.ref.get_entity())

        if self.name is None:
            self.name = compute_fk_name(field, self.ref)

        if isinstance(field._impl_, AutoImpl):
            (<AutoImpl>field._impl_)._ref_impl = self.ref._impl_
            field.min_size = self.ref.min_size
            field.max_size = self.ref.max_size

        return True

    cpdef object clone(self):
        if isinstance(self._ref, tuple):
            ref = ".".join(self._ref)
        else:
            ref = self._ref
        return type(self)(ref, name=self.name, on_update=self.on_update, on_delete=self.on_delete)

    def __repr__(self):
        return "@ForeignKey(%s, %r, on_update=%s, on_delete=%s)" % (self.name, self.ref, self.on_update, self.on_delete)


cdef compute_fk_name(Field field_from, Field field_to):
    name = "fk_%s__%s-%s__%s" % (
        field_from.get_entity().__name__,
        field_from._name_,
        field_to.get_entity().__name__,
        field_to._name_
    )

    if len(name) >= 63:
        return f"fk_{hashlib.md5(name.encode()).hexdigest()}"
    else:
        return name


cdef dict collect_foreign_keys(EntityType entity):
    cdef dict fks = {}
    cdef Field field
    cdef Field referenced
    cdef ForeignKey fk
    cdef ForeignKey fkInFks

    for field in entity.__fields__:
        for ext in field._exts_:
            if isinstance(ext, ForeignKey):
                fk = <ForeignKey>ext
            else:
                continue

            if fk is not None:
                referenced = fk.ref

                if fk.name in fks:
                    for fkInFks in fks[fk.name]:
                        if fkInFks.ref.get_entity() != referenced.get_entity():
                            raise ValueError("Can't use fields from different entities in the same foreign key")
                        elif fk.on_update != fkInFks.on_update:
                            raise ValueError("Can't use different 'on_update' value in the same foreign key")
                        elif fk.on_delete != fkInFks.on_delete:
                            raise ValueError("Can't use different 'on_delete' value in the same foreign key")
                    fks[fk.name].append(fk)
                else:
                    fks[fk.name] = [fk]

    for k, v in fks.items():
        fks[k] = tuple(v)

    return fks
