```python
from typing import Any
from yapic.entity import Entity, Int, String, Serial, Json
from yapic.entity.sql import Entity as SqlEntity


class JsonDoc(Entity):
   id: Int
   name: String


class User(SqlEntity):
   id: Serial
   json: Json[JsonDoc]
   any_json: Json[Any]

```
