from typing import Awaitable, TypeVar, Generic, List, Any, Union, AsyncIterator, Generator
from .._entity import Entity

ENT = TypeVar("ENT", bound=Entity)


class QueryContext(Generic[ENT], Awaitable[ENT]):
    async def fetch(self, num=None, *, timeout=None) -> List[Union[ENT, Any]]:
        pass

    async def fetchrow(self, *, timeout=None) -> Union[ENT, Any]:
        pass

    async def forward(self, num, *, timeout=None):
        pass

    async def fetchval(self, column=0, *, timeout=None) -> Any:
        pass

    async def first(self, *, timeout=None) -> Union[ENT, Any]:
        pass

    def __aiter__(self) -> AsyncIterator[Union[ENT, Any]]:
        pass

    def __await__(self) -> Generator[Any, None, Union[ENT, Any]]:
        pass
