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
mod st_buffer;
mod st_centroid;
mod st_contains;
mod st_convex_hull;
mod st_distance;
mod st_envelope;
mod st_geometry_type;
mod st_intersection;
mod st_intersects;
mod st_is_empty;
mod st_length;
mod st_num_points;
mod st_simplify;
mod st_union;
mod st_within;
mod st_x;
mod st_y;

use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::ScalarUDF;

pub fn register_geo_functions(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(st_contains::StContains::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(
        st_intersects::StIntersects::default(),
    ));
    ctx.register_udf(ScalarUDF::new_from_impl(st_distance::StDistance::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_within::StWithin::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_area::StArea::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_centroid::StCentroid::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_length::StLength::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_is_empty::StIsEmpty::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(
        st_geometry_type::StGeometryType::default(),
    ));
    ctx.register_udf(ScalarUDF::new_from_impl(
        st_num_points::StNumPoints::default(),
    ));
    ctx.register_udf(ScalarUDF::new_from_impl(st_x::StX::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_y::StY::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_envelope::StEnvelope::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(
        st_convex_hull::StConvexHull::default(),
    ));
    ctx.register_udf(ScalarUDF::new_from_impl(st_simplify::StSimplify::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_buffer::StBuffer::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_union::StUnion::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(
        st_intersection::StIntersection::default(),
    ));
}
