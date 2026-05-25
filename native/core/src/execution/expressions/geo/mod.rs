// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod st_area;
mod st_as_geojson;
mod st_as_text;
mod st_boundary;
mod st_buffer;
mod st_centroid;
mod st_contains;
mod st_convex_hull;
mod st_covered_by;
mod st_covers;
mod st_crosses;
mod st_difference;
mod st_disjoint;
mod st_distance;
mod st_distance_sphere;
mod st_envelope;
mod st_equals;
mod st_flip_coordinates;
mod st_geom_from_geojson;
mod st_geom_from_wkt;
mod st_geometry_type;
mod st_hausdorff_distance;
mod st_intersection;
mod st_intersects;
mod st_is_empty;
mod st_length;
mod st_make_envelope;
mod st_make_line;
mod st_num_points;
mod st_overlaps;
mod st_perimeter;
mod st_point;
mod st_simplify;
mod st_simplify_preserve_topology;
mod st_sym_difference;
mod st_touches;
mod st_union;
mod st_within;
mod st_x;
mod st_y;

use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::ScalarUDF;

pub fn register_geo_functions(ctx: &SessionContext) {
    // Constructors
    ctx.register_udf(ScalarUDF::new_from_impl(st_geom_from_wkt::StGeomFromWkt::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_geom_from_geojson::StGeomFromGeoJson::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_point::StPoint::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_make_envelope::StMakeEnvelope::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_make_line::StMakeLine::default()));
    // Serializers
    ctx.register_udf(ScalarUDF::new_from_impl(st_as_text::StAsText::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_as_geojson::StAsGeoJson::default()));
    // Predicates
    ctx.register_udf(ScalarUDF::new_from_impl(st_contains::StContains::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_intersects::StIntersects::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_within::StWithin::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_covers::StCovers::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_covered_by::StCoveredBy::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_equals::StEquals::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_touches::StTouches::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_crosses::StCrosses::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_disjoint::StDisjoint::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_overlaps::StOverlaps::default()));
    // Measurements
    ctx.register_udf(ScalarUDF::new_from_impl(st_distance::StDistance::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_distance_sphere::StDistanceSphere::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_area::StArea::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_length::StLength::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_perimeter::StPerimeter::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_hausdorff_distance::StHausdorffDistance::default()));
    // Transformations
    ctx.register_udf(ScalarUDF::new_from_impl(st_centroid::StCentroid::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_buffer::StBuffer::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_envelope::StEnvelope::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_convex_hull::StConvexHull::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_simplify::StSimplify::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_simplify_preserve_topology::StSimplifyPreserveTopology::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_flip_coordinates::StFlipCoordinates::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_boundary::StBoundary::default()));
    // Set operations
    ctx.register_udf(ScalarUDF::new_from_impl(st_union::StUnion::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_intersection::StIntersection::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_difference::StDifference::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_sym_difference::StSymDifference::default()));
    // Accessors
    ctx.register_udf(ScalarUDF::new_from_impl(st_is_empty::StIsEmpty::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_geometry_type::StGeometryType::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_num_points::StNumPoints::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_x::StX::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_y::StY::default()));
}

