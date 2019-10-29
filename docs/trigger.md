# Trigger

```python

class User(Entity):
   id: Serial
   created_time: DateTimeTz
   updated_time: DateTimeTz

User.__triggers__["updated_time"] = PostgreTrigger(
   before="INSERT",
   after="UPDATE",
   for_each="ROW | STATEMENT",
   when="OLD.* IS DISTINCT FROM NEW.*",
   params=[],
   args=[],
   body="""
   """,
)

```
