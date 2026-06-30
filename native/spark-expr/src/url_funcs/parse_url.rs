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

//! Spark-compatible parse_url / try_parse_url UDFs.
//!
//! The upstream datafusion-spark crate uses the `url` crate (WHATWG URL Standard),
//! which diverges from Spark's java.net.URI (RFC 3986) on several edge cases.
//! This module uses RFC 3986 Appendix B regex parsing to match Spark exactly.

use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, ArrayRef, LargeStringArray, StringArray, StringArrayType, StringViewArray,
};
use arrow::datatypes::DataType;
use datafusion::common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion::common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use regex::Regex;

// RFC 3986 Appendix B decomposition regex.
// Groups: 2=scheme, 4=authority, 5=path, 7=query, 9=fragment
static URI_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?$").unwrap()
});

fn extract_host(authority: &str) -> Option<String> {
    let host_port = match authority.rfind('@') {
        Some(pos) => &authority[pos + 1..],
        None => authority,
    };
    if host_port.is_empty() {
        return None;
    }
    if host_port.starts_with('[') {
        // IPv6: [::1] or [::1]:8080
        let bracket_end = host_port.find(']')?;
        Some(host_port[..=bracket_end].to_string())
    } else {
        // host or host:port - strip port
        match host_port.rfind(':') {
            Some(colon_pos) => {
                let after_colon = &host_port[colon_pos + 1..];
                if after_colon.is_empty() || after_colon.bytes().all(|b| b.is_ascii_digit()) {
                    Some(host_port[..colon_pos].to_string())
                } else {
                    // java.net.URI rejects non-digit ports
                    None
                }
            }
            None => Some(host_port.to_string()),
        }
    }
}

fn extract_userinfo(authority: &str) -> Option<String> {
    authority.rfind('@').map(|pos| authority[..pos].to_string())
}

fn extract_query_value(query: &str, key: &str) -> Option<String> {
    // Spark uses Pattern.compile("(&|^)" + key + "=([^&]*)") with no escaping,
    // so the key is treated as a regex pattern.
    let pattern = format!("(&|^){}=([^&]*)", key);
    match Regex::new(&pattern) {
        Ok(re) => re
            .captures(query)
            .and_then(|caps| caps.get(2).map(|m| m.as_str().to_string())),
        Err(_) => None,
    }
}

fn has_invalid_uri_chars(s: &str) -> bool {
    s.bytes()
        .any(|b| b == b' ' || b == b'{' || b == b'}' || b == b'<' || b == b'>' || b < 0x20)
}

fn invalid_url_err(value: &str) -> datafusion::common::DataFusionError {
    exec_datafusion_err!(
        "[INVALID_URL] The provided URL '{}' is not valid. Use `try_parse_url` to tolerate \
         malformed URLs and return NULL instead. SQLSTATE: 22P02",
        value
    )
}

fn parse_url_component(value: &str, part: &str, key: Option<&str>) -> Result<Option<String>> {
    if key.is_some() && part != "QUERY" {
        return Ok(None);
    }

    if value.is_empty() {
        return match part {
            "PATH" | "FILE" => Ok(Some(String::new())),
            _ => Ok(None),
        };
    }

    if has_invalid_uri_chars(value) {
        return Err(invalid_url_err(value));
    }

    let caps = match URI_REGEX.captures(value) {
        Some(c) => c,
        None => return Ok(None),
    };

    let scheme = caps.get(2).map(|m| m.as_str());
    let authority = caps.get(4).map(|m| m.as_str());
    let path = caps.get(5).map_or("", |m| m.as_str());
    let query = caps.get(7).map(|m| m.as_str());
    let fragment = caps.get(9).map(|m| m.as_str());

    if scheme.is_none() {
        if value.contains("://") {
            return Err(invalid_url_err(value));
        }
        return Ok(None);
    }

    // java.net.URI rejects unbalanced brackets in authority
    if let Some(auth) = authority {
        let has_open = auth.contains('[');
        let has_close = auth.contains(']');
        if has_open != has_close {
            return Err(invalid_url_err(value));
        }
    }

    match part {
        "HOST" => Ok(authority.and_then(extract_host)),
        "PATH" => Ok(Some(path.to_string())),
        "QUERY" => match key {
            None => Ok(query.map(String::from)),
            Some(k) => Ok(query.and_then(|q| extract_query_value(q, k))),
        },
        "REF" => Ok(fragment.map(String::from)),
        "PROTOCOL" => Ok(scheme.map(String::from)),
        "FILE" => match query {
            Some(q) => Ok(Some(format!("{path}?{q}"))),
            None => Ok(Some(path.to_string())),
        },
        "AUTHORITY" => Ok(authority.filter(|a| !a.is_empty()).map(String::from)),
        "USERINFO" => Ok(authority.and_then(extract_userinfo)),
        _ => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// CometParseUrl UDF (failOnError = true)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CometParseUrl {
    signature: Signature,
}

impl Default for CometParseUrl {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::String(2), TypeSignature::String(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CometParseUrl {
    fn name(&self) -> &str {
        "parse_url"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_parse_url(&args.args, |x| x)
    }
}

// ---------------------------------------------------------------------------
// CometTryParseUrl UDF (failOnError = false)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CometTryParseUrl {
    signature: Signature,
}

impl Default for CometTryParseUrl {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::String(2), TypeSignature::String(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CometTryParseUrl {
    fn name(&self) -> &str {
        "try_parse_url"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_parse_url(&args.args, |x| match x {
            Err(_) => Ok(None),
            ok => ok,
        })
    }
}

fn invoke_parse_url(
    args: &[ColumnarValue],
    handle: impl Fn(Result<Option<String>>) -> Result<Option<String>>,
) -> Result<ColumnarValue> {
    let is_scalar = args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let result = dispatch_parse_url(&arrays, handle)?;
    if is_scalar {
        ScalarValue::try_from_array(&result, 0).map(ColumnarValue::Scalar)
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

// ---------------------------------------------------------------------------
// Array dispatch
// ---------------------------------------------------------------------------

fn dispatch_parse_url(
    args: &[ArrayRef],
    handle: impl Fn(Result<Option<String>>) -> Result<Option<String>>,
) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("parse_url expects 2 or 3 arguments, but got {}", args.len());
    }
    let url = &args[0];
    let part = &args[1];

    if args.len() == 3 {
        let key = &args[2];
        match (url.data_type(), part.data_type(), key.data_type()) {
            (DataType::Utf8, DataType::Utf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_array(url)?,
                    as_string_array(part)?,
                    as_string_array(key)?,
                    handle,
                )
            }
            (DataType::Utf8View, DataType::Utf8View, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringViewArray>(
                    as_string_view_array(url)?,
                    as_string_view_array(part)?,
                    as_string_view_array(key)?,
                    handle,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_large_string_array(part)?,
                    as_large_string_array(key)?,
                    handle,
                )
            }
            _ => exec_err!(
                "parse_url expects STRING arguments, got ({}, {}, {})",
                url.data_type(),
                part.data_type(),
                key.data_type()
            ),
        }
    } else {
        let null_keys = StringArray::new_null(url.len());

        match (url.data_type(), part.data_type()) {
            (DataType::Utf8, DataType::Utf8) => process_parse_url::<_, _, _, StringArray>(
                as_string_array(url)?,
                as_string_array(part)?,
                &null_keys,
                handle,
            ),
            (DataType::Utf8View, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringViewArray>(
                    as_string_view_array(url)?,
                    as_string_view_array(part)?,
                    &null_keys,
                    handle,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_large_string_array(part)?,
                    &null_keys,
                    handle,
                )
            }
            _ => exec_err!(
                "parse_url expects STRING arguments, got ({}, {})",
                url.data_type(),
                part.data_type()
            ),
        }
    }
}

fn process_parse_url<'a, A, B, C, T>(
    url_array: &'a A,
    part_array: &'a B,
    key_array: &'a C,
    handle: impl Fn(Result<Option<String>>) -> Result<Option<String>>,
) -> Result<ArrayRef>
where
    &'a A: StringArrayType<'a>,
    &'a B: StringArrayType<'a>,
    &'a C: StringArrayType<'a>,
    T: Array + FromIterator<Option<String>> + 'static,
{
    url_array
        .iter()
        .zip(part_array.iter())
        .zip(key_array.iter())
        .map(|((url, part), key)| {
            if let (Some(url), Some(part)) = (url, part) {
                handle(parse_url_component(url, part, key))
            } else {
                Ok(None)
            }
        })
        .collect::<Result<T>>()
        .map(|array| Arc::new(array) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sa(vals: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(vals.to_vec())) as ArrayRef
    }

    // -----------------------------------------------------------------------
    // Unit tests for parse_url_component
    // -----------------------------------------------------------------------

    #[test]
    fn test_host() -> Result<()> {
        assert_eq!(
            parse_url_component("https://example.com/a?x=1", "HOST", None)?,
            Some("example.com".into())
        );
        Ok(())
    }

    #[test]
    fn test_path_normal() -> Result<()> {
        assert_eq!(
            parse_url_component("https://example.com/a/b", "PATH", None)?,
            Some("/a/b".into())
        );
        Ok(())
    }

    #[test]
    fn test_fix1_empty_string_path() -> Result<()> {
        assert_eq!(parse_url_component("", "PATH", None)?, Some("".into()));
        assert_eq!(parse_url_component("", "FILE", None)?, Some("".into()));
        assert_eq!(parse_url_component("", "HOST", None)?, None);
        assert_eq!(parse_url_component("", "QUERY", None)?, None);
        assert_eq!(parse_url_component("", "PROTOCOL", None)?, None);
        assert_eq!(parse_url_component("", "REF", None)?, None);
        assert_eq!(parse_url_component("", "AUTHORITY", None)?, None);
        assert_eq!(parse_url_component("", "USERINFO", None)?, None);
        Ok(())
    }

    #[test]
    fn test_fix2_file_without_path() -> Result<()> {
        assert_eq!(
            parse_url_component("http://host?foo=bar", "FILE", None)?,
            Some("?foo=bar".into())
        );
        Ok(())
    }

    #[test]
    fn test_fix3_path_trailing_slash() -> Result<()> {
        assert_eq!(
            parse_url_component("https://example.com/", "PATH", None)?,
            Some("/".into())
        );
        Ok(())
    }

    #[test]
    fn test_fix4_query_key_no_decode() -> Result<()> {
        assert_eq!(
            parse_url_component(
                "http://example.com/path?key=value%20encoded",
                "QUERY",
                Some("key")
            )?,
            Some("value%20encoded".into())
        );
        Ok(())
    }

    #[test]
    fn test_query_no_key() -> Result<()> {
        assert_eq!(
            parse_url_component("https://ex.com/p?a=1&b=2", "QUERY", None)?,
            Some("a=1&b=2".into())
        );
        Ok(())
    }

    #[test]
    fn test_query_with_key() -> Result<()> {
        assert_eq!(
            parse_url_component("https://ex.com/p?a=1&b=2", "QUERY", Some("a"))?,
            Some("1".into())
        );
        assert_eq!(
            parse_url_component("https://ex.com/p?a=1&b=2", "QUERY", Some("c"))?,
            None
        );
        Ok(())
    }

    #[test]
    fn test_ref_protocol_userinfo_file_authority() -> Result<()> {
        let url = "ftp://user:pwd@ftp.example.com:21/files?x=1#frag";
        assert_eq!(parse_url_component(url, "REF", None)?, Some("frag".into()));
        assert_eq!(
            parse_url_component(url, "PROTOCOL", None)?,
            Some("ftp".into())
        );
        assert_eq!(
            parse_url_component(url, "USERINFO", None)?,
            Some("user:pwd".into())
        );
        assert_eq!(
            parse_url_component(url, "FILE", None)?,
            Some("/files?x=1".into())
        );
        // Authority includes port (matches java.net.URI)
        assert_eq!(
            parse_url_component(url, "AUTHORITY", None)?,
            Some("user:pwd@ftp.example.com:21".into())
        );
        Ok(())
    }

    #[test]
    fn test_malformed_no_scheme() -> Result<()> {
        assert_eq!(parse_url_component("notaurl", "HOST", None)?, None);
        Ok(())
    }

    #[test]
    fn test_malformed_with_invalid_chars() {
        let result = parse_url_component("not a url at all", "HOST", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_malformed_with_scheme_separator() {
        let result = parse_url_component("://missing-scheme", "HOST", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_ipv6_host() -> Result<()> {
        assert_eq!(
            parse_url_component("http://[::1]:8080/path", "HOST", None)?,
            Some("[::1]".into())
        );
        Ok(())
    }

    #[test]
    fn test_null_handling_array() -> Result<()> {
        let urls = sa(&[Some("https://example.com/path?k=v"), None]);
        let parts = sa(&[Some("HOST"), Some("HOST")]);
        let out = dispatch_parse_url(&[urls, parts], |x| x)?;
        let out_sa = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out_sa.value(0), "example.com");
        assert!(out_sa.is_null(1));
        Ok(())
    }

    #[test]
    fn test_three_arg_query_key() -> Result<()> {
        let urls = sa(&[
            Some("https://example.com/a?x=1&y=2"),
            Some("https://ex.com/?a=1"),
        ]);
        let parts = sa(&[Some("QUERY"), Some("QUERY")]);
        let keys = sa(&[Some("y"), Some("b")]);
        let out = dispatch_parse_url(&[urls, parts, keys], |x| x)?;
        let out_sa = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out_sa.value(0), "2");
        assert!(out_sa.is_null(1));
        Ok(())
    }

    #[test]
    fn test_try_parse_url_tolerates_errors() -> Result<()> {
        let handler = |x: Result<Option<String>>| match x {
            Err(_) => Ok(None),
            ok => ok,
        };
        let urls = sa(&[Some("://bad"), Some("http://ok.com/p")]);
        let parts = sa(&[Some("HOST"), Some("HOST")]);
        let out = dispatch_parse_url(&[urls, parts], handler)?;
        let out_sa = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(out_sa.is_null(0));
        assert_eq!(out_sa.value(1), "ok.com");
        Ok(())
    }

    #[test]
    fn test_spaces_in_url() {
        let result = parse_url_component("http://ho st/path", "HOST", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_double_slashes_in_path() -> Result<()> {
        assert_eq!(
            parse_url_component("http://example.com//double//slashes", "PATH", None)?,
            Some("//double//slashes".into())
        );
        Ok(())
    }

    #[test]
    fn test_empty_query() -> Result<()> {
        assert_eq!(
            parse_url_component("http://example.com/path?", "QUERY", None)?,
            Some("".into())
        );
        Ok(())
    }

    #[test]
    fn test_file_with_no_fragment() -> Result<()> {
        assert_eq!(
            parse_url_component("http://example.com#frag", "FILE", None)?,
            Some("".into())
        );
        Ok(())
    }

    #[test]
    fn test_duplicate_query_keys() -> Result<()> {
        assert_eq!(
            parse_url_component("http://example.com/path?a=1&a=2", "QUERY", Some("a"))?,
            Some("1".into())
        );
        Ok(())
    }

    #[test]
    fn test_query_value_contains_equals() -> Result<()> {
        assert_eq!(
            parse_url_component("http://host/p?a=b=c", "QUERY", Some("a"))?,
            Some("b=c".into())
        );
        assert_eq!(
            parse_url_component("http://host/p?tok=abc=def=&x=1", "QUERY", Some("tok"))?,
            Some("abc=def=".into())
        );
        Ok(())
    }

    #[test]
    fn test_three_arg_non_query_returns_null() -> Result<()> {
        assert_eq!(
            parse_url_component("http://host/path", "HOST", Some("key"))?,
            None
        );
        assert_eq!(
            parse_url_component("http://host/path", "PATH", Some("key"))?,
            None
        );
        Ok(())
    }

    #[test]
    fn test_host_empty_port() -> Result<()> {
        assert_eq!(
            parse_url_component("http://host:/path", "HOST", None)?,
            Some("host".into())
        );
        Ok(())
    }

    #[test]
    fn test_empty_authority() -> Result<()> {
        assert_eq!(
            parse_url_component("http:///path", "AUTHORITY", None)?,
            None
        );
        Ok(())
    }

    #[test]
    fn test_regex_metachar_in_query_key() -> Result<()> {
        // Spark treats key as regex: ".bc" matches "abc"
        assert_eq!(
            parse_url_component("http://h/p?abc=1", "QUERY", Some(".bc"))?,
            Some("1".into())
        );
        assert_eq!(
            parse_url_component("http://h/p?abc=1", "QUERY", Some("a.c"))?,
            Some("1".into())
        );
        // Literal key that doesn't match as regex
        assert_eq!(
            parse_url_component("http://h/p?abc=1", "QUERY", Some("x.c"))?,
            None
        );
        Ok(())
    }

    #[test]
    fn test_non_digit_port_returns_null_host() -> Result<()> {
        assert_eq!(parse_url_component("http://host:abc/", "HOST", None)?, None);
        // Other parts still work
        assert_eq!(
            parse_url_component("http://host:abc/", "PATH", None)?,
            Some("/".into())
        );
        assert_eq!(
            parse_url_component("http://host:abc/", "AUTHORITY", None)?,
            Some("host:abc".into())
        );
        Ok(())
    }

    #[test]
    fn test_unbalanced_ipv6_bracket() {
        let result = parse_url_component("http://[::1/path", "AUTHORITY", None);
        assert!(result.is_err());
        let result = parse_url_component("http://[::1/path", "PATH", None);
        assert!(result.is_err());
        let result = parse_url_component("http://[::1/path", "HOST", None);
        assert!(result.is_err());
    }
}
