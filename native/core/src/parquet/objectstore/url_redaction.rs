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

//! URL forms for messages, store keys and object_store, which quotes URLs in its errors. The
//! password and query (a SAS token) can carry a credential; the path is not treated as secret.

use std::borrow::Cow;
use url::{Position, Url};

use crate::parquet::objectstore::s3_blob_fs_support::scheme_of_unparsed;
use crate::parquet::parquet_support::is_azure_scheme;

/// Keeps the username: in an ABFS URL it is the container, which `MicrosoftAzureBuilder` reads.
pub(crate) fn without_secrets(url: &Url) -> Url {
    let mut stripped = url.clone();
    stripped.set_query(None);
    stripped.set_fragment(None);
    // Fails only for a URL that cannot have userinfo, so there is nothing to clear.
    let _ = stripped.set_password(None);
    stripped
}

/// [`without_secrets`] minus the username, which outside Azure can be an access key ID.
pub(crate) fn without_credentials(url: &Url) -> Url {
    let mut stripped = without_secrets(url);
    // Fails only for a URL that cannot have userinfo, so there is nothing to clear.
    let _ = stripped.set_username("");
    stripped
}

pub(crate) fn authority_without_password(url: &Url) -> Cow<'_, str> {
    if url.password().is_none() {
        return Cow::Borrowed(&url[Position::BeforeUsername..Position::AfterPort]);
    }
    Cow::Owned(without_secrets(url)[Position::BeforeUsername..Position::AfterPort].to_string())
}

pub(crate) fn display_authority(url: &Url) -> String {
    format!("{}://{}", url.scheme(), authority_without_password(url))
}

/// `url` without its userinfo password, query and fragment, for a message.
///
/// A URL with no `//` after its scheme parses with an opaque path that can hold userinfo, so it is
/// shown as `<unparseable>`. A string that fails to parse is shown as given when it has none of
/// `://`, `@`, `?` and `#`, so a local path with `@`, `?` or `#` is `<unparseable>`. Any other
/// string that fails to parse is shown as `scheme://<unparseable>`, or `<unparseable>` without a
/// valid scheme. A parsed URL with `@` anywhere after its authority (path, query or fragment) can
/// hold a password whose `/`, `?` or `#` ended the authority early, so it is
/// `scheme://<unparseable>`, as is a real path or query with `@` in it.
/// Otherwise the password, query and fragment are dropped, and the username is shown as `***`
/// except in an ABFS URL, where it names the container. A URL with none of these is shown as
/// given.
pub(crate) fn redacted_url_display(url: &str) -> String {
    let parsed = match Url::parse(url) {
        Ok(parsed) if parsed.cannot_be_a_base() => return "<unparseable>".to_string(),
        Ok(parsed) if parsed[Position::BeforePath..].contains('@') => {
            return format!("{}://<unparseable>", parsed.scheme())
        }
        Ok(parsed) => parsed,
        Err(_) if !url.contains(['@', '?', '#']) && !url.contains("://") => return url.to_string(),
        Err(_) => {
            return match scheme_of_unparsed(url) {
                Some(scheme) => format!("{scheme}://<unparseable>"),
                None => "<unparseable>".to_string(),
            }
        }
    };
    let masks_username = !parsed.username().is_empty() && !is_azure_scheme(parsed.scheme());
    if !masks_username
        && parsed.password().is_none()
        && parsed.query().is_none()
        && parsed.fragment().is_none()
    {
        return url.to_string();
    }
    let mut shown = without_secrets(&parsed);
    if masks_username && shown.set_username("***").is_err() {
        return format!("{}://<unparseable>", parsed.scheme());
    }
    shown.to_string()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Userinfo, query and fragment markers the test URLs carry, none of which may be shown.
    const URL_MARKERS: [&str; 4] = ["alice", "SECRET", "SAS-MARKER", "FRAG-MARKER"];

    pub(crate) fn assert_no_url_marker(shown: &str) {
        for marker in URL_MARKERS {
            assert!(!shown.contains(marker), "{marker} echoed: {shown}");
        }
    }

    #[test]
    fn redacted_url_display_drops_userinfo_query_and_fragment() {
        for (url, expected) in [
            (
                "s3a://bucket/a/part-0.parquet?sig=SAS-MARKER#FRAG-MARKER",
                "s3a://bucket/a/part-0.parquet",
            ),
            (
                "s3a://alice:SECRET@bucket/a/part-0.parquet?sig=SAS-MARKER",
                "s3a://***@bucket/a/part-0.parquet",
            ),
            (
                "https://bucket.s3.amazonaws.com/a/part-0.parquet?X-Amz-Signature=SAS-MARKER",
                "https://bucket.s3.amazonaws.com/a/part-0.parquet",
            ),
            ("s3a://bucket/p#FRAG-MARKER", "s3a://bucket/p"),
            ("s3a://alice@bucket/p", "s3a://***@bucket/p"),
            ("s3a://:SECRET@bucket/p", "s3a://bucket/p"),
            ("s3://alice%3ASECRET@bucket/p", "s3://***@bucket/p"),
            (
                "s3://alice:SECRET@[::1]:9000/p?sig=SAS-MARKER",
                "s3://***@[::1]:9000/p",
            ),
        ] {
            let shown = redacted_url_display(url);
            assert_no_url_marker(&shown);
            assert_eq!(shown, expected);
        }
    }

    #[test]
    fn redacted_url_display_keeps_the_abfs_container() {
        for url in [
            "abfss://data:SECRET@acct.dfs.core.windows.net/t/p.parquet?sig=SAS-MARKER",
            "abfss://data:SECRET@acct.dfs.core.windows.net/t/p.parquet",
        ] {
            let shown = redacted_url_display(url);
            assert_no_url_marker(&shown);
            assert_eq!(shown, "abfss://data@acct.dfs.core.windows.net/t/p.parquet");
        }
    }

    #[test]
    fn redacted_url_display_names_only_the_scheme_of_an_unparseable_url() {
        for url in [
            "s3a://alice:SECRET@bucket:notaport/p?sig=SAS-MARKER",
            "s3a://bucket:notaport/p?sig=SAS-MARKER",
            "s3a://bucket:notaport/p",
            // The `/` in the password ends the authority, which then has a bad port.
            "s3a://alice:SECRET/x@bucket/p",
        ] {
            assert!(
                Url::parse(url).is_err(),
                "fixture must fail to parse: {url}"
            );
            let shown = redacted_url_display(url);
            assert_no_url_marker(&shown);
            assert_eq!(shown, "s3a://<unparseable>");
        }
    }

    #[test]
    fn redacted_url_display_hides_opaque_and_relative_strings_with_secrets() {
        for (url, expected) in [
            // No `//`, so these parse with an opaque path that keeps the userinfo; in the
            // second, the "scheme" is the username.
            ("s3a:alice:SECRET@bucket/p", "<unparseable>"),
            ("alice:SECRET@bucket://x", "<unparseable>"),
            // Relative, so they fail to parse, and carry userinfo, a query or a fragment.
            ("t/p.parquet?sig=SAS-MARKER", "<unparseable>"),
            ("t/p.parquet#FRAG-MARKER", "<unparseable>"),
            ("alice@bucket/p", "<unparseable>"),
            // Nothing to hide: shown as given.
            ("t/p.parquet", "t/p.parquet"),
            ("s3a://bucket/a b", "s3a://bucket/a b"),
            ("hdfs:/tmp/p.parquet", "hdfs:/tmp/p.parquet"),
        ] {
            let shown = redacted_url_display(url);
            assert_no_url_marker(&shown);
            assert_eq!(shown, expected, "for {url}");
        }
    }

    #[test]
    fn redacted_url_display_hides_a_password_that_parses_as_a_path() {
        for url in [
            // The `/` in the password ends the authority, so the username is the host, the
            // port is empty or numeric, and the rest of the password is in the path.
            "s3a://alice:/SECRET@bucket/p",
            "s3a://alice:123/SECRET@bucket/p",
            // A `?` or `#` ends the authority the same way, leaving the `@` in the query or
            // fragment.
            "s3a://alice:?SECRET@bucket/p",
            "s3a://alice:123?SECRET@bucket/p",
            "s3a://alice:#SECRET@bucket/p",
            "s3a://alice:12#SECRET@bucket/p",
            "s3a://alice:/SECRET#x@bucket/p",
            "s3a://bucket/a@b/p",
            "s3a://bucket/p?x=a@b",
        ] {
            assert!(Url::parse(url).is_ok(), "fixture must parse: {url}");
            let shown = redacted_url_display(url);
            assert_no_url_marker(&shown);
            assert_eq!(shown, "s3a://<unparseable>", "for {url}");
        }
    }
}
