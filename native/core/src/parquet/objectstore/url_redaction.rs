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
