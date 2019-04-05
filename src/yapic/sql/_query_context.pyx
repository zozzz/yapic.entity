
cdef class QueryContext:
    async def fetch(self, num=None, timeout=None):
        pass

    async def fetchrow(self, timeout=None):
        pass

    async def forward(self, num=None, timeout=None):
        pass

    async def fetchval(self, timeout=None):
        pass
