

cdef class Query:
    def __cinit__(self, ctx = None):
        pass

    def select_from(self, from_):
        if self.from_clause is None:
            self.from_clause = []

        if from_ not in self.from_clause:
            self.from_clause.append(from_)

    def column(self, *columns):
        if self.columns is None:
            self.columns = []

    def where(self, *expr, **eq):
        if self.where_clause is None:
            self.where_clause = []

    def order(self, *expr):
        if self.orders is None:
            self.orders = []

    def group(self, *expr):
        if self.groups is None:
            self.groups = []

    def having(self, *expr):
        if self.havings is None:
            self.havings = []

    def distinct(self, *expr):
        if self.distincts is None:
            self.distincts = []

    def prefix(self, *prefix):
        if self.prefixes is None:
            self.prefixes = []

    def suffix(self, *prefix):
        if self.suffixes is None:
            self.suffixes = []

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

    def clone(self):
        ctx = self.ctx.clone()
        return type(self)(ctx)
