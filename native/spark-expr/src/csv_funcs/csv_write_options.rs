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

use std::fmt::Result;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CsvWriteOptions {
    pub delimiter: String,
    pub quote: char,
    pub escape: char,
    pub null_value: String,
    pub timezone: String,
    pub date_format: String,
    pub timestamp_format: String,
    pub timestamp_ntz_format: String,
    pub quote_all: bool,
    pub escape_quotes: bool,
    pub ignore_leading_white_space: bool,
    pub ignore_trailing_white_space: bool,
}

impl Display for CsvWriteOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "csv_write_options(delimiter={}, quote={}, escape={}, null_value={}, timezone={}, date_format={}, timestamp_format={}, timestamp_ntz_format={}, quote_all={}, escape_quotes={}, ignore_leading_white_space={}, ignore_trailing_white_space={})",
            self.delimiter, self.quote, self.escape, self.null_value, self.timezone, self.date_format, self.timestamp_format, self.timestamp_ntz_format, self.quote_all, self.escape_quotes, self.ignore_leading_white_space, self.ignore_trailing_white_space
        )
    }
}

impl CsvWriteOptions {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        delimiter: String,
        quote: String,
        escape: String,
        null_value: String,
        timezone: String,
        date_format: String,
        timestamp_format: String,
        timestamp_ntz_format: String,
        quote_all: bool,
        escape_quotes: bool,
        ignore_leading_white_space: bool,
        ignore_trailing_white_space: bool,
    ) -> Self {
        Self {
            delimiter,
            quote: quote.chars().next().unwrap(),
            escape: escape.chars().next().unwrap(),
            null_value,
            timezone,
            date_format,
            timestamp_format,
            timestamp_ntz_format,
            quote_all,
            escape_quotes,
            ignore_leading_white_space,
            ignore_trailing_white_space,
        }
    }
}
