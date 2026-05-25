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
mod st_centroid;
mod st_contains;
mod st_distance;
mod st_intersects;
mod st_within;

use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::ScalarUDF;

pub fn register_geo_functions(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(
        st_contains::StContains::default(),
    ));
    ctx.register_udf(ScalarUDF::new_from_impl(
        st_intersects::StIntersects::default(),
    ));
    ctx.register_udf(ScalarUDF::new_from_impl(
        st_distance::StDistance::default(),
    ));
    ctx.register_udf(ScalarUDF::new_from_impl(st_within::StWithin::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(st_area::StArea::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(
        st_centroid::StCentroid::default(),
    ));
}
