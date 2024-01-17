import cython
import hashlib
from cpython.object cimport PyObject
from cpython.weakref cimport PyWeakref_NewRef, PyWeakref_GetObject
from cpython.module cimport PyImport_Import, PyModule_GetDict

from ._expression cimport Expression, Visitor
from ._entity cimport EntityType, EntityBase, EntityAttribute, EntityAttributeExt, EntityAttributeExtGroup, EntityAttributeImpl, get_alias_target, Registry
from ._factory cimport ForwardDecl, get_type_hints, new_instance, new_instance_from_forward, is_forward_decl
from ._field_impl cimport AutoImpl, ArrayImpl
from ._resolve cimport ResolveContext




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

    cpdef object init(self):
        if self.nullable is None:
            self.nullable = True
            if self.get_ext(PrimaryKey):
                self.nullable = False
            elif not callable(self._default_):
                self.nullable = bool(self._default_ is None)
            elif isinstance(self._default_, Expression):
                self.nullable = False
        return EntityAttribute.init(self)

    cpdef clone(self):
        cdef Field res = type(self)(self._impl_.clone(),
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
        cdef str factory_key = str(factory)
        try:
            return self.type_cache[factory_key]
        except KeyError:
            type = factory.create(self)
            self.type_cache[factory_key] = type
            return type

    def __repr__(self):
        return f"<Field {self._name_}: {self._impl_} {self._entity_repr()}>"

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

    def __str__(self):
        t = type(self)
        return f"{t.__module__}::{t.__name__}"


cdef class PrimaryKey(FieldExtension):
    pass


cdef class AutoIncrement(FieldExtension):
    def __cinit__(self, object sequence=None):
        self._seq_arg = sequence

    cdef object _resolve_deferred(self, ResolveContext ctx):
        if FieldExtension._resolve_deferred(self, ctx) is False:
            return False

        cdef EntityType entity
        cdef EntityType aliased
        cdef EntityAttribute attr = self.get_attr()
        cdef Registry registry

        if self.sequence is None:
            from ._entity import _EntityBase

            if self._seq_arg is None:
                entity = attr.get_entity()
                aliased = get_alias_target(entity)
                registry = entity.get_registry()

                if entity is aliased:
                    schema = entity.get_meta("schema", None)
                    name = f"{entity.__name__}_{attr._name_}_seq"
                    try:
                        self.sequence = registry[name if schema is None or schema == "public" else f"{schema}.{name}"]
                    except KeyError:
                        self.sequence = EntityType(name, (_EntityBase,), {}, schema=schema, registry=<object>entity.registry_ref, is_sequence=True)
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
                registry = entity.get_registry()

                try:
                    self.sequence = registry[name if not schema or schema == "public" else f"{schema}.{name}"]
                except KeyError:
                    self.sequence = EntityType(name, (_EntityBase,), {}, schema=schema, registry=<object>entity.registry_ref, is_sequence=True)

        attr._deps_.add_entity(self.sequence)
        return self.sequence.is_resolved()

    cpdef object clone(self):
        return type(self)(self._seq_arg)

    def __repr__(self):
        if self.sequence:
            return "@AutoIncrement(%r)" % self.sequence.__qname__
        else:
            return "@AutoIncrement(%r)" % self._seq_arg



cdef class Index(FieldExtension):
    def __cinit__(self, *, str expr = None, str name = None, str method = None, bint unique = False, str collate = None):
        self.name = name
        self.method = method
        self.unique = unique
        self.collate = collate
        self.expr = expr

    cpdef object clone(self):
        return type(self)(expr=self.expr, name=self.name, method=self.method, unique=self.unique, collate=self.collate)

    cpdef object init(self):
        cdef EntityAttribute attr = self.get_attr()
        if not self.name:
            self.name = f"idx_{attr.get_entity().__name__}__{attr._name_}"

        if not self.method:
            if isinstance(attr._impl_, ArrayImpl):
                self.method = "gin"
            if isinstance(attr._impl_, AutoImpl):
                if isinstance((<AutoImpl>attr._impl_)._ref_impl, ArrayImpl):
                    self.method = "gin"

            if not self.method:
                self.method = "btree"

        self.add_to_group(self.name)
        return FieldExtension.init(self)

    def __repr__(self):
        return "@%sIndex(%s USING %s ON %s %s)" % ("Unique" if self.unique else "", self.name, self.method, self.expr or self.get_attr(), self.collate or "")


cdef class Unique(FieldExtension):
    @classmethod
    def validate_group(self, EntityAttributeExtGroup group):
        cdef list items = group.items
        cdef Unique main = items[0]
        group.name = main.name

    def __cinit__(self, *, str name = None):
        self.name = name

    cpdef object init(self):
        cdef EntityAttribute attr = self.get_attr()

        if not self.name:
            name = f"unique_{attr.get_entity().__name__}__{attr._name_}"
            if len(name) >= 63:
                name = f"unique_{hashlib.md5(name.encode()).hexdigest()}"
            self.name = name

        self.add_to_group(self.name)
        return FieldExtension.init(self)

    cpdef object clone(self):
        return Unique(name=self.name)

    def __repr__(self):
        return f"@Unique(name={self.name})"

# todo: faster eval, with Py_CompileString(ref, "<string>", Py_eval_input); PyEval_EvalCode

_CodeType = type(compile("1", "<string>", "eval"))

cdef class FKBase(FieldExtension):
    def __cinit__(self, field, *, str name = None, str on_update = "RESTRICT", str on_delete = "RESTRICT"):
        self.ref = None

        if isinstance(field, str):
            self._ref = field
        elif not isinstance(field, Field):
            raise TypeError("Incorrect argument for ForeignKey field parameter")
        else:
            self.ref = self._ref = field

        self.name = name
        self.on_update = on_update
        self.on_delete = on_delete

    cdef object _resolve_deferred(self, ResolveContext ctx):
        if FieldExtension._resolve_deferred(self, ctx) is False:
            return False

        if self.ref is None:
            try:
                self.ref = ctx.eval(self._ref, {})
            except NameError:
                return False
            self._ref = None

        cdef Field field = self.get_attr()
        field._deps_.add_entity(self.ref.get_entity())

        if self.name is None:
            self.name = compute_fk_name(field, self.ref)

        return True

    cpdef object clone(self):
        if self.ref is not None:
            ref = self.ref
        else:
            ref = self._ref
        return type(self)(ref, name=self.name, on_update=self.on_update, on_delete=self.on_delete)

    def __repr__(self):
        return f"@{type(self).__name__}({self.name}, {self.ref}, on_update={self.on_update}, on_delete={self.on_delete})"


cdef class ForeignKey(FKBase):
    @classmethod
    def validate_group(self, EntityAttributeExtGroup group):
        cdef list items = group.items
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

    cdef _bind(self, object attr_ref):
        FieldExtension._bind(self, attr_ref)

        cdef EntityAttribute attr = self.get_attr()
        cdef Index index = attr.get_ext(Index)
        if not index and not attr.get_ext(PrimaryKey):
            attr // Index()

    cdef object _resolve_deferred(self, ResolveContext ctx):
        if FKBase._resolve_deferred(self, ctx) is False:
            return False

        self.add_to_group(self.name)

        cdef Field field = self.get_attr()
        if isinstance(field._impl_, AutoImpl):
            (<AutoImpl>field._impl_)._ref_impl = self.ref._impl_
            field.min_size = self.ref.min_size
            field.max_size = self.ref.max_size

        return True


cdef class ForeignKeyList(FKBase):
    # @classmethod
    # def validate_group(self, EntityAttributeExtGroup group):
    #     cdef list items = group.items
    #     cdef ForeignKey main = items[0]
    #     cdef ForeignKey fk

    #     for i in range(1, len(items)):
    #         fk = <ForeignKey>items[i]
    #         if fk.ref.get_entity() != main.ref.get_entity():
    #             raise ValueError(f"Can't use fields from different entities in the same foreign key: '{main.name}'")
    #         elif fk.on_update != main.on_update:
    #             raise ValueError(f"Can't use different 'on_update' value in the same foreign key: '{main.name}'")
    #         elif fk.on_delete != main.on_delete:
    #             raise ValueError(f"Can't use different 'on_update' value in the same foreign key: '{main.name}'")

    #     group.name = main.name

    # XXX: Exception https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html#PLPGSQL-STATEMENTS-RAISE
    cdef _bind(self, object attr_ref):
        FKBase._bind(self, attr_ref)

        cdef EntityAttribute attr = self.get_attr()
        cdef Index index = attr.get_ext(Index)
        if not index:
            attr // Index()

    cdef object _resolve_deferred(self, ResolveContext ctx):
        if FKBase._resolve_deferred(self, ctx) is False:
            return False

        cdef Field field = self.get_attr()

        field.get_entity().__triggers__.extend(self.__check_trigger())
        self.ref.get_entity().__triggers__.extend(self.__update_trigger())
        self.ref.get_entity().__triggers__.extend(self.__delete_trigger())

        # self.add_to_group(self.name)


        if isinstance(field._impl_, AutoImpl):
            (<AutoImpl>field._impl_)._ref_impl = ArrayImpl(self.ref._impl_)
            field.min_size = self.ref.min_size
            field.max_size = self.ref.max_size

        return True

    cdef tuple __check_trigger(self):
        from .sql.pgsql._trigger import PostgreTrigger

        cdef Field field = self.get_attr()
        cdef ref_entiy = entity_qname_pg(self.ref.get_entity())
        cdef when = f'OLD."{field._name_}" IS DISTINCT FROM NEW."{field._name_}"'
        cdef declare = "DECLARE missing RECORD;"
        cdef body = f"""
            IF array_length(array_remove(NEW."{field._name_}", NULL), 1)
                <> array_length((SELECT array_agg(DISTINCT ref_id) FROM unnest(array_remove(NEW."{field._name_}", NULL)) fkl(ref_id)), 1) THEN
                RAISE EXCEPTION 'ForeignKeyList items is not unique %', NEW
                    USING ERRCODE = 'unique_violation';
            END IF;

            FOR missing IN SELECT ref_id
                FROM unnest(array_remove(NEW."{field._name_}", NULL)) fkl(ref_id)
                WHERE NOT EXISTS(SELECT 1 FROM {ref_entiy} WHERE "{self.ref._name_}" = fkl.ref_id)
            LOOP
                RAISE EXCEPTION 'ForeignKeyList missing entry {ref_entiy}."{self.ref._name_}"=%" %', missing.ref_id, NEW
                    USING ERRCODE = 'foreign_key_violation';
            END LOOP;
            RETURN NEW;
        """

        return (
            PostgreTrigger(name=f"{self.name}-CI", before="INSERT", for_each="ROW", declare=declare, body=body),
            PostgreTrigger(name=f"{self.name}-CU", before="UPDATE", for_each="ROW", when=when, declare=declare, body=body)
        )

    # TODO: CONSTRAINT TRIGGER
    cdef tuple __update_trigger(self):
        from .sql.pgsql._trigger import PostgreTrigger

        cdef str on_update = self.on_update.upper()
        if on_update == "NO ACTION":
            return tuple()

        cdef str when = f'OLD."{self.ref._name_}" IS DISTINCT FROM NEW."{self.ref._name_}"'
        cdef dict args = {}

        if on_update == "CASCADE":
            args["after"] = "UPDATE"
            args["body"] = f"""
            UPDATE {entity_qname_pg(self.get_attr()._entity_)}
                SET "{self.get_attr()._name_}" = array_replace("{self.get_attr()._name_}", OLD."{self.ref._name_}", NEW."{self.ref._name_}")
            WHERE "{self.get_attr()._name_}" @> ARRAY[OLD."{self.ref._name_}"];
            RETURN NEW;
            """
        elif on_update == "RESTRICT":
            args["before"] = "UPDATE"
            args["body"] = f"""
            IF EXISTS(SELECT 1 FROM {entity_qname_pg(self.get_attr()._entity_)} WHERE "{self.get_attr()._name_}" @> ARRAY[OLD."{self.ref._name_}"]) THEN
                RAISE EXCEPTION 'ForeignKeyList prevent of record update, because has references %', NEW
                    USING ERRCODE = 'foreign_key_violation';
            END IF;
            RETURN NEW;
            """
        else:
            raise ValueError(f"Invalid value for on_update = {self.on_update}")

        return (PostgreTrigger(name=f"{self.name}-RU", for_each="ROW", when=when, **args),)

    cdef tuple __delete_trigger(self):
        from .sql.pgsql._trigger import PostgreTrigger

        cdef str on_delete = self.on_delete.upper()
        if on_delete == "NO ACTION":
            return tuple()

        cdef dict args = {}

        if on_delete == "CASCADE":
            args["after"] = "DELETE"
            args["body"] = f"""
            UPDATE {entity_qname_pg(self.get_attr()._entity_)}
                SET "{self.get_attr()._name_}" = array_remove("{self.get_attr()._name_}", OLD."{self.ref._name_}")
            WHERE "{self.get_attr()._name_}" @> ARRAY[OLD."{self.ref._name_}"];
            RETURN OLD;
            """
        elif on_delete == "RESTRICT":
            args["before"] = "DELETE"
            args["body"] = f"""
            IF EXISTS(SELECT 1 FROM {entity_qname_pg(self.get_attr()._entity_)} WHERE "{self.get_attr()._name_}" @> ARRAY[OLD."{self.ref._name_}"]) THEN
                RAISE EXCEPTION 'ForeignKeyList prevent of record delete, because has references %', OLD
                    USING ERRCODE = 'foreign_key_violation';
            END IF;
            RETURN OLD;
            """
        elif on_delete == "SET NULL":
            args["after"] = "DELETE"
            args["body"] = f"""
            UPDATE {entity_qname_pg(self.get_attr()._entity_)}
                SET "{self.get_attr()._name_}" = array_replace("{self.get_attr()._name_}", OLD."{self.ref._name_}", NULL)
            WHERE "{self.get_attr()._name_}" @> ARRAY[OLD."{self.ref._name_}"];
            RETURN OLD;
            """
        else:
            raise ValueError(f"Invalid value for on_delete = {self.on_delete}")

        return (PostgreTrigger(name=f"{self.name}-RD", for_each="ROW", **args),)


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


cdef class Check(FieldExtension):
    @classmethod
    def validate_group(self, EntityAttributeExtGroup group):
        cdef list items = group.items
        cdef Check main = items[0]
        group.name = main.name

    def __cinit__(self, str expr, *, str name = None):
        self.expr = expr
        self.name = name
        self.props = {}

    cpdef object init(self):
        cdef EntityAttribute attr = self.get_attr()

        if not self.name:
            name = f"chk_{attr.get_entity().__name__}__{attr._name_}"
            if len(name) >= 63:
                name = f"chk_{hashlib.md5(name.encode()).hexdigest()}"
            self.name = name

        if "field" not in self.props:
            self.props["field"] = attr._key_

        if "hash" not in self.props:
            self.props["hash"] = self.expr_hash()

        self.add_to_group(self.name)
        return FieldExtension.init(self)

    cdef str expr_hash(self):
        return hashlib.md5(self.expr.encode()).hexdigest()

    cpdef object clone(self):
        check = Check(self.expr, name=self.name)
        check.props = dict(self.props)
        return check

    def __repr__(self):
        if "hash" in self.props:
            return f"Check(name={self.name}, expr_hash={self.props['hash']})"
        else:
            return f"Check(name={self.name}, expr={self.expr}, props={self.props})"


cdef str entity_qname_pg(EntityType entity):
    cdef schema = entity.get_meta("schema", "public")
    if schema == "public":
        return f'"{entity.__name__}"'
    else:
        return f'"{schema}"."{entity.__name__}"'
