# PostGIS

## Python

```python

class Point:
   lat: Numeric
   lng: Numeric
   srid: Int


class Place(Entity):
   id: Serial
   location: postgis.Point

```

## SQL

### DDL

```sql
CREATE TABLE "Place" (
   "location" GEOMETRY(POINT, 4326)
)
```


### Insert

```sql
INSERT INTO "Place" ("location") VALUES (ST_SetSRID(ST_MakePoint(x, y), 4326))
INSERT INTO "Place" ("location") VALUES (ST_SetSRID(ST_MakePoint(lng, lat), 4326))
```


### Select
```sql
SELECT ST_X("location"::GEOMETRY) as lng, ST_Y("location"::GEOMETRY) as lat, ST_SRID("location") as srid FROM "Place"
```
