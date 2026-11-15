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
//! This module splits raw URI components at the RFC 3986 Appendix B delimiters.

use std::cell::RefCell;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, LargeStringArray, StringArray, StringArrayType, StringViewArray,
};
use arrow::datatypes::DataType;
use datafusion::common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use regex::Regex;

use crate::SparkError;

fn extract_host(authority: &str) -> Option<&str> {
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
        Some(&host_port[..=bracket_end])
    } else {
        // host or host:port - strip port
        match host_port.rfind(':') {
            Some(colon_pos) => {
                let after_colon = &host_port[colon_pos + 1..];
                if after_colon.is_empty() || after_colon.bytes().all(|b| b.is_ascii_digit()) {
                    Some(&host_port[..colon_pos])
                } else {
                    // java.net.URI rejects non-digit ports
                    None
                }
            }
            None => Some(host_port),
        }
    }
}

fn extract_userinfo(authority: &str) -> Option<&str> {
    authority.rfind('@').map(|pos| &authority[..pos])
}

thread_local! {
    // The query regex is derived solely from `key`, which is almost always
    // constant across a batch (e.g. `parse_url(col, 'QUERY', 'v')`). Naive code
    // recompiles it for every row; cache the last (key, compiled regex) pair so
    // it is compiled once per distinct key instead. A `None` regex records that
    // `key` failed to compile as a pattern.
    static QUERY_KEY_REGEX: RefCell<Option<(String, Option<Regex>)>> =
        const { RefCell::new(None) };
}

fn extract_query_value<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    // Spark uses Pattern.compile("(&|^)" + key + "=([^&]*)") with no escaping,
    // so the key is treated as a regex pattern.
    QUERY_KEY_REGEX.with(|cell| {
        let mut slot = cell.borrow_mut();
        // Drop the cached entry when the key changes, then (re)build lazily.
        if slot.as_ref().map(|(k, _)| k.as_str()) != Some(key) {
            *slot = None;
        }
        let (_, re) = slot.get_or_insert_with(|| {
            let pattern = format!("(&|^){}=([^&]*)", key);
            (key.to_string(), Regex::new(&pattern).ok())
        });
        let re = re.as_ref()?;
        re.captures(query)
            .and_then(|caps| caps.get(2).map(|m| m.as_str()))
    })
}

fn has_invalid_uri_chars(s: &str) -> bool {
    s.bytes()
        .any(|b| b == b' ' || b == b'{' || b == b'}' || b == b'<' || b == b'>' || b < 0x20)
}

fn invalid_url_err(value: &str) -> datafusion::common::DataFusionError {
    SparkError::InvalidUrl {
        url: value.to_string(),
    }
    .into()
}

// These slices preserve the raw URI components, including empty query/fragment values.
#[derive(Debug, PartialEq)]
struct UriParts<'a> {
    scheme: Option<&'a str>,
    authority: Option<&'a str>,
    path: &'a str,
    // FILE is the contiguous raw path and optional query, including the '?'.
    file: &'a str,
    query: Option<&'a str>,
    fragment: Option<&'a str>,
}

fn decompose_uri(value: &str) -> UriParts<'_> {
    let (scheme, rest) = match value.find([':', '/', '?', '#']) {
        Some(i) if i > 0 && value.as_bytes()[i] == b':' => (Some(&value[..i]), &value[i + 1..]),
        _ => (None, value),
    };
    let (authority, tail) = if let Some(rest) = rest.strip_prefix("//") {
        let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        (Some(&rest[..end]), &rest[end..])
    } else {
        (None, rest)
    };
    let (file, fragment) = match tail.split_once('#') {
        Some((file, fragment)) => (file, Some(fragment)),
        None => (tail, None),
    };
    let (path, query) = match file.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (file, None),
    };
    UriParts {
        scheme,
        authority,
        path,
        file,
        query,
        fragment,
    }
}

fn parse_url_component<'a>(
    value: &'a str,
    part: &str,
    key: Option<&str>,
) -> Result<Option<&'a str>> {
    if key.is_some() && part != "QUERY" {
        return Ok(None);
    }

    if value.is_empty() {
        return match part {
            "PATH" | "FILE" => Ok(Some(value)),
            _ => Ok(None),
        };
    }

    if has_invalid_uri_chars(value) {
        return Err(invalid_url_err(value));
    }

    let UriParts {
        scheme,
        authority,
        path,
        file,
        query,
        fragment,
    } = decompose_uri(value);

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
        "PATH" => Ok(Some(path)),
        "QUERY" => match key {
            None => Ok(query),
            Some(k) => Ok(query.and_then(|q| extract_query_value(q, k))),
        },
        "REF" => Ok(fragment),
        "PROTOCOL" => Ok(scheme),
        "FILE" => Ok(Some(file)),
        "AUTHORITY" => Ok(authority.filter(|a| !a.is_empty())),
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
    handle: impl for<'s> Fn(Result<Option<&'s str>>) -> Result<Option<&'s str>>,
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
    handle: impl for<'s> Fn(Result<Option<&'s str>>) -> Result<Option<&'s str>>,
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
    handle: impl for<'s> Fn(Result<Option<&'s str>>) -> Result<Option<&'s str>>,
) -> Result<ArrayRef>
where
    &'a A: StringArrayType<'a>,
    &'a B: StringArrayType<'a>,
    &'a C: StringArrayType<'a>,
    T: Array + FromIterator<Option<&'a str>> + 'static,
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
    fn uri_decomposition_matches_appendix_b_regex() {
        let regex =
            Regex::new(r"^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?$").unwrap();
        for url in [
            "https://u:p@host:80/p?q#f",
            "HTTP://host?#",
            "ftp://[::1]:80//a/b",
            "mailto:a@b",
            "例:é?k=值#片",
            "//host/p?x=1#f?y",
            "scheme:///p",
            "https:/p",
            "relative/path:part",
            ":leading",
            "?#",
            "x:/p?a=?b#f#x",
            "",
        ] {
            let caps = regex.captures(url).unwrap();
            let component = |i| caps.get(i).map(|m| m.as_str());
            let path = component(5).unwrap();
            let query = component(7);
            let file = query.map_or_else(|| path.to_owned(), |q| format!("{path}?{q}"));
            assert_eq!(
                decompose_uri(url),
                UriParts {
                    scheme: component(2),
                    authority: component(4),
                    path,
                    file: &file,
                    query,
                    fragment: component(9),
                },
                "{url}"
            );
        }
    }

    #[test]
    fn file_and_query_components() -> Result<()> {
        for (url, part, key, expected) in [
            ("http://h?x=1#f?y", "FILE", None, "?x=1"),
            ("http://h?#", "FILE", None, "?"),
            ("HTTP://h/p?#f", "FILE", None, "/p?"),
            ("ftp://h/p?x=1#f", "FILE", None, "/p?x=1"),
            ("https://h/é?k=值#片", "PATH", None, "/é"),
            // The key may introduce capture groups; both Spark and the existing
            // matcher return group 2 of the full pattern, not its last group.
            ("https://h/?abc=1&v=2", "QUERY", Some("(abc)"), "abc"),
            ("https://h/?abc=1&v=2", "QUERY", Some("v"), "2"),
        ] {
            let result = parse_url_component(url, part, key)?.unwrap();
            assert_eq!(result, expected);
        }
        // Extraction must still reject an invalid component after the requested host.
        assert!(parse_url_component("https://host/p#invalid fragment", "HOST", None).is_err());
        Ok(())
    }

    #[test]
    fn string_view_host_does_not_retain_url_buffer() -> Result<()> {
        let url = format!("https://long.example.com/{}", "x".repeat(128 * 1024));
        let urls = Arc::new(StringViewArray::from(vec![url.as_str()]));
        let parts = Arc::new(StringViewArray::from(vec!["HOST"]));
        let output = dispatch_parse_url(&[urls, parts], |x| x)?;
        let hosts = as_string_view_array(&output)?;
        assert_eq!(hosts.value(0), "long.example.com");
        assert!(hosts.get_buffer_memory_size() < url.len());
        Ok(())
    }

    #[test]
    fn test_host() -> Result<()> {
        assert_eq!(
            parse_url_component("https://example.com/a?x=1", "HOST", None)?,
            Some("example.com")
        );
        Ok(())
    }

    #[test]
    fn test_path_normal() -> Result<()> {
        assert_eq!(
            parse_url_component("https://example.com/a/b", "PATH", None)?,
            Some("/a/b")
        );
        Ok(())
    }

    #[test]
    fn test_fix1_empty_string_path() -> Result<()> {
        assert_eq!(parse_url_component("", "PATH", None)?, Some(""));
        assert_eq!(parse_url_component("", "FILE", None)?, Some(""));
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
            Some("?foo=bar")
        );
        Ok(())
    }

    #[test]
    fn test_fix3_path_trailing_slash() -> Result<()> {
        assert_eq!(
            parse_url_component("https://example.com/", "PATH", None)?,
            Some("/")
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
            Some("value%20encoded")
        );
        Ok(())
    }

    #[test]
    fn test_query_no_key() -> Result<()> {
        assert_eq!(
            parse_url_component("https://ex.com/p?a=1&b=2", "QUERY", None)?,
            Some("a=1&b=2")
        );
        Ok(())
    }

    #[test]
    fn test_query_with_key() -> Result<()> {
        assert_eq!(
            parse_url_component("https://ex.com/p?a=1&b=2", "QUERY", Some("a"))?,
            Some("1")
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
        assert_eq!(parse_url_component(url, "REF", None)?, Some("frag"));
        assert_eq!(parse_url_component(url, "PROTOCOL", None)?, Some("ftp"));
        assert_eq!(
            parse_url_component(url, "USERINFO", None)?,
            Some("user:pwd")
        );
        assert_eq!(parse_url_component(url, "FILE", None)?, Some("/files?x=1"));
        // Authority includes port (matches java.net.URI)
        assert_eq!(
            parse_url_component(url, "AUTHORITY", None)?,
            Some("user:pwd@ftp.example.com:21")
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
            Some("[::1]")
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
        fn handler(x: Result<Option<&str>>) -> Result<Option<&str>> {
            match x {
                Err(_) => Ok(None),
                ok => ok,
            }
        }
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
            Some("//double//slashes")
        );
        Ok(())
    }

    #[test]
    fn test_empty_query() -> Result<()> {
        assert_eq!(
            parse_url_component("http://example.com/path?", "QUERY", None)?,
            Some("")
        );
        Ok(())
    }

    #[test]
    fn test_file_with_no_fragment() -> Result<()> {
        assert_eq!(
            parse_url_component("http://example.com#frag", "FILE", None)?,
            Some("")
        );
        Ok(())
    }

    #[test]
    fn test_duplicate_query_keys() -> Result<()> {
        assert_eq!(
            parse_url_component("http://example.com/path?a=1&a=2", "QUERY", Some("a"))?,
            Some("1")
        );
        Ok(())
    }

    #[test]
    fn test_query_value_contains_equals() -> Result<()> {
        assert_eq!(
            parse_url_component("http://host/p?a=b=c", "QUERY", Some("a"))?,
            Some("b=c")
        );
        assert_eq!(
            parse_url_component("http://host/p?tok=abc=def=&x=1", "QUERY", Some("tok"))?,
            Some("abc=def=")
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
            Some("host")
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
            Some("1")
        );
        assert_eq!(
            parse_url_component("http://h/p?abc=1", "QUERY", Some("a.c"))?,
            Some("1")
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
            Some("/")
        );
        assert_eq!(
            parse_url_component("http://host:abc/", "AUTHORITY", None)?,
            Some("host:abc")
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
