from yapic.entity._entity cimport EntityType
from yapic.entity._field cimport Field
from yapic.entity._expression cimport DirectionExpression
from yapic.entity._expression import and_


cdef class Query:
    def __cinit__(self, ctx = None):
        pass

    def select_from(self, from_):
        if self.from_clause is None:
            self.from_clause = []

        if from_ not in self.from_clause:
            self.from_clause.append(from_)

        return self

    def column(self, *columns):
        if self.columns is None:
            self.columns = []

        for col in columns:
            if isinstance(col, EntityType):
                self.column((<EntityType>col).__fields__)
            elif isinstance(col, Field):
                if col not in self.columns:
                    self.columns.append(col)
            elif isinstance(col, RawExpression):
                self.columns.append(col)
            else:
                raise ValueError("Invalid value for column: %r" % col)

        return self

    def where(self, *expr, **eq):
        if self.where_clause is None:
            self.where_clause = []

        self.where_clause.append(and_(*expr))
        if eq:
            raise NotImplementedError()
        return self


    def order(self, *expr):
        if self.orders is None:
            self.orders = []

        for item in expr:
            if isinstance(item, DirectionExpression):
                self.orders.append(item)
            elif isinstance(item, Field):
                self.order.append((<Field>item).asc())
            elif isinstance(item, RawExpression):
                self.order.append(item)
            else:
                raise ValueError("Invalid value for order: %r" % item)

        return self

    def group(self, *expr):
        if self.groups is None:
            self.groups = []

        for item in expr:
            if not isinstance(item, Field):
                raise ValueError("Invalid value for group: %r" % item)
            else:
                self.groups.append(item)

        return self


    def having(self, *expr):
        if self.havings is None:
            self.havings = []

    def distinct(self, *expr):
        if self.distincts is None:
            self.distincts = []

    def prefix(self, *prefix):
        if self.prefixes is None:
            self.prefixes = []

        for p in prefix:
            if p not in self.prefixes:
                self.prefixes.append(p)

        return self

    def suffix(self, *suffix):
        if self.suffixes is None:
            self.suffixes = []

        for s in suffix:
            if s not in self.suffixes:
                self.suffixes.append(s)

        return self

    def join(self, EntityType ent, condition = None, type = "INNER"):
        if self.joins is None:
            self.joins = []

    def limit(self, int count):
        if self.range is None:
            self.range = slice(0, count)
        else:
            self.range = slice(self.range.start, self.range.start + count)

    def offset(self, int offset):
        if self.range is None:
            self.range = slice(offset, None)
        else:
            if self.range.stop:
                count = self.range.stop - self.range.start
                stop = offset + count
            else:
                stop = None

            self.range = slice(offset, stop)

    def as_alias(self):
        pass

    def as_subquery(self):
        pass

    cpdef Query clone(self):
        cdef Query q = type(self)()

        if self.from_clause: q.from_clause = list(self.from_clause)
        if self.columns: q.columns = list(self.columns)
        if self.where_clause: q.where_clause = list(self.where_clause)
        if self.orders: q.orders = list(self.orders)
        if self.groups: q.groups = list(self.groups)
        if self.havings: q.havings = list(self.havings)
        if self.distincts: q.distincts = list(self.distincts)
        if self.prefixes: q.prefixes = list(self.prefixes)
        if self.suffixes: q.suffixes = list(self.suffixes)
        if self.joins: q.joins = list(self.joins)
        if self.range: q.range = slice(self.range.start, self.range.stop, self.range.step)

        return q


cdef class RawExpression(Expression):
    def __cinit__(self, str sql):
        self.sql = sql

    def __hash__(self):
        return hash(self.sql)

    def __eq__(self, other):
        if isinstance(other, RawExpression):
            return (<RawExpression>other).sql == self.sql
        return False

    def __ne__(self, other):
        if isinstance(other, RawExpression):
            return (<RawExpression>other).sql == self.sql
        return False


cpdef raw(self, str sql):
    return RawExpression(sql)
