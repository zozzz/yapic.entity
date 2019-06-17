- EntityType.registry = WeakRef
- Field.entity = WeakRef
- Extension.attr = WeakRef
- Lock insert / update on tables with `__fix_entries__` (CREATE TRIGGER, ALTER TABLE DISABLE / ENABLE TRIGGER)
- kitalálni, hogyan lehet módosítani a composite typeot
- A relation-ben mégsem kéne alapértelmezésben aliast használni, vagy ha igen, akkor valahogy jobban kéne támogatni azt
- Type.encode-nak kéne egy encode_inline függvény, ami raw expressionnel tér vissza, és ez csak akkor van használva,
  ha nem paraméterként van megadva a queryben a paraméter
