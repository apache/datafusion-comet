<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Comet Geo Functions

Comet provides 40 geospatial SQL functions registered as Spark SQL extensions.
All functions execute natively in the Rust/DataFusion engine when Comet is enabled
(`spark.comet.exec.enabled=true`). Geometries are represented as WKT strings.

## Constructors

Functions that create geometry values.

### st_geomfromwkt

```sql
st_geomfromwkt(wkt STRING) -> STRING
```

Parses a WKT string and returns the geometry. Returns `null` if the input is `null`.

```sql
SELECT st_geomfromwkt('POINT(1.0 2.0)');
-- POINT (1 2)
```

### st_geomfromgeojson

```sql
st_geomfromgeojson(geojson STRING) -> STRING
```

Parses a GeoJSON string and returns the geometry as WKT.

```sql
SELECT st_geomfromgeojson('{"type":"Point","coordinates":[1.0,2.0]}');
-- POINT (1 2)
```

### st_point

```sql
st_point(x DOUBLE, y DOUBLE) -> STRING
```

Creates a point geometry from x (longitude) and y (latitude) coordinates.

```sql
SELECT st_point(1.0, 2.0);
-- POINT(1.0 2.0)
```

### st_makeenvelope

```sql
st_makeenvelope(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE) -> STRING
```

Creates a rectangular polygon (envelope/bounding box) from corner coordinates.
Returns `null` if any argument is `null`.

```sql
SELECT st_makeenvelope(0.0, 0.0, 1.0, 1.0);
-- POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))
```

### st_makeline

```sql
st_makeline(geom1 STRING, geom2 STRING) -> STRING
```

Creates a linestring connecting two geometries.

```sql
SELECT st_makeline(st_point(0.0, 0.0), st_point(1.0, 1.0));
-- LINESTRING (0 0, 1 1)
```

---

## Serializers

Functions that convert a geometry to a text format.

### st_astext

```sql
st_astext(geom STRING) -> STRING
```

Returns the WKT representation of a geometry.

```sql
SELECT st_astext(st_point(1.0, 2.0));
-- POINT (1 2)
```

### st_asgeojson

```sql
st_asgeojson(geom STRING) -> STRING
```

Returns the GeoJSON representation of a geometry.

```sql
SELECT st_asgeojson(st_point(1.0, 2.0));
-- {"type":"Point","coordinates":[1.0,2.0]}
```

---

## Predicates

Functions that test a spatial relationship between two geometries and return a boolean.

### st_contains

```sql
st_contains(geom1 STRING, geom2 STRING) -> BOOLEAN
```

Returns `true` if `geom1` contains `geom2` (i.e. no point of `geom2` is outside `geom1`).

```sql
SELECT st_contains(
  st_makeenvelope(0.0, 0.0, 2.0, 2.0),
  st_point(1.0, 1.0)
);
-- true
```

### st_intersects

```sql
st_intersects(geom1 STRING, geom2 STRING) -> BOOLEAN
```

Returns `true` if the two geometries share any point.

```sql
SELECT st_intersects(
  st_makeenvelope(0.0, 0.0, 2.0, 2.0),
  st_makeenvelope(1.0, 1.0, 3.0, 3.0)
);
-- true
```

### st_within

```sql
st_within(geom1 STRING, geom2 STRING) -> BOOLEAN
```

Returns `true` if `geom1` is completely inside `geom2`. The inverse of `st_contains`.

```sql
SELECT st_within(
  st_point(1.0, 1.0),
  st_makeenvelope(0.0, 0.0, 2.0, 2.0)
);
-- true
```

### st_covers

```sql
st_covers(geom1 STRING, geom2 STRING) -> BOOLEAN
```

Returns `true` if every point of `geom2` lies within or on the boundary of `geom1`.
Similar to `st_contains` but includes boundary points.

### st_coveredby

```sql
st_coveredby(geom1 STRING, geom2 STRING) -> BOOLEAN
```

Returns `true` if every point of `geom1` lies within or on the boundary of `geom2`.
The inverse of `st_covers`.

### st_equals

```sql
st_equals(geom1 STRING, geom2 STRING) -> BOOLEAN
```

Returns `true` if the two geometries represent the same geometric shape
(point-set equality, not string equality).

### st_touches

```sql
st_touches(geom1 STRING, geom2 STRING) -> BOOLEAN
```

Returns `true` if the geometries share boundary points but their interiors do not intersect.

### st_crosses

```sql
st_crosses(geom1 STRING, geom2 STRING) -> BOOLEAN
```

Returns `true` if the geometries have some but not all interior points in common,
and the dimension of the intersection is less than the maximum dimension of either geometry.

### st_disjoint

```sql
st_disjoint(geom1 STRING, geom2 STRING) -> BOOLEAN
```

Returns `true` if the two geometries share no points. The inverse of `st_intersects`.

### st_overlaps

```sql
st_overlaps(geom1 STRING, geom2 STRING) -> BOOLEAN
```

Returns `true` if the two geometries of the same dimension intersect but neither contains the other.

---

## Measurements

Functions that compute a numeric value from one or two geometries.

### st_area

```sql
st_area(geom STRING) -> DOUBLE
```

Returns the area of a polygon geometry. Returns `0.0` for points and linestrings.

```sql
SELECT st_area(st_makeenvelope(0.0, 0.0, 2.0, 3.0));
-- 6.0
```

### st_length

```sql
st_length(geom STRING) -> DOUBLE
```

Returns the length of a linestring, or the perimeter of a polygon geometry.

```sql
SELECT st_length(st_makeline(st_point(0.0, 0.0), st_point(3.0, 4.0)));
-- 5.0
```

### st_perimeter

```sql
st_perimeter(geom STRING) -> DOUBLE
```

Returns the perimeter of a polygon geometry. Returns `0.0` for points and linestrings.

```sql
SELECT st_perimeter(st_makeenvelope(0.0, 0.0, 1.0, 1.0));
-- 4.0
```

### st_distance

```sql
st_distance(geom1 STRING, geom2 STRING) -> DOUBLE
```

Returns the minimum planar (Cartesian) distance between two geometries.

```sql
SELECT st_distance(st_point(0.0, 0.0), st_point(3.0, 4.0));
-- 5.0
```

### st_distancesphere

```sql
st_distancesphere(geom1 STRING, geom2 STRING) -> DOUBLE
```

Returns the great-circle distance in metres between two point geometries,
assuming a spherical Earth model.

```sql
SELECT st_distancesphere(st_point(-0.1276, 51.5074), st_point(2.3522, 48.8566));
-- ~341550 (London to Paris, metres)
```

### st_hausdorffdistance

```sql
st_hausdorffdistance(geom1 STRING, geom2 STRING) -> DOUBLE
```

Returns the Hausdorff distance between two geometries. Useful for measuring
how similar two shapes are.

### st_numpoints

```sql
st_numpoints(geom STRING) -> BIGINT
```

Returns the number of vertices in a geometry.

```sql
SELECT st_numpoints(st_makeenvelope(0.0, 0.0, 1.0, 1.0));
-- 5
```

### st_x

```sql
st_x(geom STRING) -> DOUBLE
```

Returns the x-coordinate (longitude) of a point geometry.

```sql
SELECT st_x(st_point(1.5, 2.5));
-- 1.5
```

### st_y

```sql
st_y(geom STRING) -> DOUBLE
```

Returns the y-coordinate (latitude) of a point geometry.

```sql
SELECT st_y(st_point(1.5, 2.5));
-- 2.5
```

---

## Accessors

Functions that return a property or derived geometry from a single geometry.

### st_isempty

```sql
st_isempty(geom STRING) -> BOOLEAN
```

Returns `true` if the geometry is empty (contains no points).

### st_geometrytype

```sql
st_geometrytype(geom STRING) -> STRING
```

Returns the type name of the geometry: `Point`, `LineString`, `Polygon`,
`MultiPoint`, `MultiLineString`, `MultiPolygon`, or `GeometryCollection`.

```sql
SELECT st_geometrytype(st_point(1.0, 2.0));
-- Point
```

---

## Transformations

Functions that return a new geometry derived from the input.

### st_centroid

```sql
st_centroid(geom STRING) -> STRING
```

Returns the geometric centre (centroid) of a geometry as a point.

```sql
SELECT st_centroid(st_makeenvelope(0.0, 0.0, 2.0, 2.0));
-- POINT (1 1)
```

### st_envelope

```sql
st_envelope(geom STRING) -> STRING
```

Returns the minimum bounding rectangle of a geometry as a polygon.

```sql
SELECT st_envelope(st_makeline(st_point(1.0, 2.0), st_point(3.0, 4.0)));
-- POLYGON ((1 2, 3 2, 3 4, 1 4, 1 2))
```

### st_convexhull

```sql
st_convexhull(geom STRING) -> STRING
```

Returns the smallest convex polygon that contains all points of the geometry.

### st_buffer

```sql
st_buffer(geom STRING, distance DOUBLE) -> STRING
```

Returns a geometry that represents all points within `distance` of `geom`.
The result is a polygon for point and linestring inputs.

```sql
SELECT st_buffer(st_point(0.0, 0.0), 1.0);
-- POLYGON ((1 0, ...))   (approximated circle)
```

### st_simplify

```sql
st_simplify(geom STRING, tolerance DOUBLE) -> STRING
```

Simplifies a geometry using the Douglas-Peucker algorithm. Points within
`tolerance` of the simplified line are removed. May produce invalid topologies.

```sql
SELECT st_simplify(st_makeenvelope(0.0, 0.0, 1.0, 1.0), 0.1);
```

### st_simplifypreservetopology

```sql
st_simplifypreservetopology(geom STRING, tolerance DOUBLE) -> STRING
```

Same as `st_simplify` but guarantees the result is topologically valid
(no self-intersections, no collapse to empty).

### st_flipcoordinates

```sql
st_flipcoordinates(geom STRING) -> STRING
```

Swaps the x and y coordinates of every vertex. Useful for converting between
(longitude, latitude) and (latitude, longitude) conventions.

```sql
SELECT st_flipcoordinates(st_point(1.0, 2.0));
-- POINT (2 1)
```

### st_boundary

```sql
st_boundary(geom STRING) -> STRING
```

Returns the boundary of a geometry. For a polygon this is its ring(s);
for a linestring it is its two endpoints; for a point it is empty.

---

## Set Operations

Functions that compute a new geometry from two input geometries.

### st_union

```sql
st_union(geom1 STRING, geom2 STRING) -> STRING
```

Returns a geometry representing all points in either `geom1` or `geom2`.

```sql
SELECT st_union(
  st_makeenvelope(0.0, 0.0, 1.0, 1.0),
  st_makeenvelope(0.5, 0.5, 1.5, 1.5)
);
```

### st_intersection

```sql
st_intersection(geom1 STRING, geom2 STRING) -> STRING
```

Returns a geometry representing the points shared by both `geom1` and `geom2`.

```sql
SELECT st_intersection(
  st_makeenvelope(0.0, 0.0, 2.0, 2.0),
  st_makeenvelope(1.0, 1.0, 3.0, 3.0)
);
-- POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))
```

### st_difference

```sql
st_difference(geom1 STRING, geom2 STRING) -> STRING
```

Returns a geometry representing the points in `geom1` that are not in `geom2`.

```sql
SELECT st_difference(
  st_makeenvelope(0.0, 0.0, 2.0, 2.0),
  st_makeenvelope(1.0, 1.0, 3.0, 3.0)
);
```

### st_symdifference

```sql
st_symdifference(geom1 STRING, geom2 STRING) -> STRING
```

Returns a geometry representing the points in either `geom1` or `geom2`
but not both (the symmetric difference / XOR of the two shapes).

```sql
SELECT st_symdifference(
  st_makeenvelope(0.0, 0.0, 2.0, 2.0),
  st_makeenvelope(1.0, 1.0, 3.0, 3.0)
);
```

---

## Function Summary

| Function | Arguments | Return type | Category |
|---|---|---|---|
| `st_geomfromwkt` | wkt | STRING | Constructor |
| `st_geomfromgeojson` | geojson | STRING | Constructor |
| `st_point` | x, y | STRING | Constructor |
| `st_makeenvelope` | xmin, ymin, xmax, ymax | STRING | Constructor |
| `st_makeline` | geom1, geom2 | STRING | Constructor |
| `st_astext` | geom | STRING | Serializer |
| `st_asgeojson` | geom | STRING | Serializer |
| `st_contains` | geom1, geom2 | BOOLEAN | Predicate |
| `st_intersects` | geom1, geom2 | BOOLEAN | Predicate |
| `st_within` | geom1, geom2 | BOOLEAN | Predicate |
| `st_covers` | geom1, geom2 | BOOLEAN | Predicate |
| `st_coveredby` | geom1, geom2 | BOOLEAN | Predicate |
| `st_equals` | geom1, geom2 | BOOLEAN | Predicate |
| `st_touches` | geom1, geom2 | BOOLEAN | Predicate |
| `st_crosses` | geom1, geom2 | BOOLEAN | Predicate |
| `st_disjoint` | geom1, geom2 | BOOLEAN | Predicate |
| `st_overlaps` | geom1, geom2 | BOOLEAN | Predicate |
| `st_area` | geom | DOUBLE | Measurement |
| `st_length` | geom | DOUBLE | Measurement |
| `st_perimeter` | geom | DOUBLE | Measurement |
| `st_distance` | geom1, geom2 | DOUBLE | Measurement |
| `st_distancesphere` | geom1, geom2 | DOUBLE | Measurement |
| `st_hausdorffdistance` | geom1, geom2 | DOUBLE | Measurement |
| `st_numpoints` | geom | BIGINT | Measurement |
| `st_x` | geom | DOUBLE | Measurement |
| `st_y` | geom | DOUBLE | Measurement |
| `st_isempty` | geom | BOOLEAN | Accessor |
| `st_geometrytype` | geom | STRING | Accessor |
| `st_centroid` | geom | STRING | Transformation |
| `st_envelope` | geom | STRING | Transformation |
| `st_convexhull` | geom | STRING | Transformation |
| `st_buffer` | geom, distance | STRING | Transformation |
| `st_simplify` | geom, tolerance | STRING | Transformation |
| `st_simplifypreservetopology` | geom, tolerance | STRING | Transformation |
| `st_flipcoordinates` | geom | STRING | Transformation |
| `st_boundary` | geom | STRING | Transformation |
| `st_union` | geom1, geom2 | STRING | Set operation |
| `st_intersection` | geom1, geom2 | STRING | Set operation |
| `st_difference` | geom1, geom2 | STRING | Set operation |
| `st_symdifference` | geom1, geom2 | STRING | Set operation |
