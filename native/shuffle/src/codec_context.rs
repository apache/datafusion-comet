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

use arrow::ipc::writer::IpcWriteContext;
use std::io;
use zstd::zstd_safe::{CCtx, CParameter, DCtx, ResetDirective};

/// Largest zstd workspace worth caching between frames. Covers the commonly configured
/// levels; higher levels (tens to hundreds of MiB of window) fall back to a fresh context
/// per frame, which is what per-block encoding paid anyway.
///
/// Workspace sizes measured against zstd-sys 2.0.16+zstd.1.5.7 (`sizeof()` after one
/// streaming frame, no pledged source size). Levels 7/8 sit ~3% under the cap, so a zstd
/// upgrade can silently flip them to release-per-block; re-measure on any dependency bump.
///
/// | level | CCtx after encode | DCtx after decode |
/// |-------|-------------------|-------------------|
/// |  1    |  1,369,617        | 1,013,552         |
/// |  2    |  2,090,513        |                   |
/// |  3    |  3,663,377        | 2,586,416         |
/// |  4    |  4,974,097        |                   |
/// |  5    |  5,498,385        |                   |
/// |  6    |  5,498,385        |                   |
/// |  7    |  8,119,825        |                   |
/// |  8    |  8,119,825        |                   |
/// |  9    | 15,459,857 (over) |                   |
/// | 19    | 93,848,207 (over) | 8,877,872 (over)  |
const MAX_RETAINED_ZSTD_CONTEXT_BYTES: usize = 8 * 1024 * 1024;

/// Reusable compression state for encoding shuffle blocks.
///
/// A zstd context costs about a megabyte and real setup time, so a task shares one across all
/// the blocks it encodes instead of paying per block. Keep ownership task-scoped, never
/// per-output-partition -- a shuffle can have thousands of partitions. Local shuffle reuses
/// the zstd context between blocks but bounds what it retains
/// ([`Self::release_zstd_if_oversized`]) and drops it at spill/finish boundaries; the remote
/// (RSS) path frees it after each admitted encode via [`Self::release_zstd`], since its
/// memory accounting only reserves the workspace per invocation.
#[derive(Default)]
pub struct ShuffleCodecContext {
    /// Arrow's per-message IPC compression scratch, reused across encodes.
    pub(crate) arrow_ipc: IpcWriteContext,
    /// Lazily created, reused across blocks.
    zstd: Option<CCtx<'static>>,
    /// How many zstd contexts this value has created, so tests can assert that N blocks
    /// cost fewer than N creations instead of only observing retained/released state.
    #[cfg(test)]
    zstd_creations: u32,
}

impl ShuffleCodecContext {
    /// The shared zstd context primed for one frame at `level`, plus the Arrow IPC scratch
    /// (returned together because the encoder borrows the context for the whole frame).
    ///
    /// The session reset and level re-apply happen on every call: writers with different
    /// levels can share one context, and a failed encode must not leave state behind.
    pub(crate) fn zstd_cctx(
        &mut self,
        level: i32,
    ) -> io::Result<(&mut CCtx<'static>, &mut IpcWriteContext)> {
        let cctx = match &mut self.zstd {
            Some(cctx) => cctx,
            none => {
                #[cfg(test)]
                {
                    self.zstd_creations += 1;
                }
                none.insert(CCtx::try_create().ok_or_else(|| {
                    io::Error::other("failed to allocate zstd compression context")
                })?)
            }
        };
        cctx.reset(ResetDirective::SessionOnly)
            .map_err(map_zstd_error)?;
        cctx.set_parameter(CParameter::CompressionLevel(level))
            .map_err(map_zstd_error)?;
        Ok((cctx, &mut self.arrow_ipc))
    }

    /// Drops the cached zstd context, freeing its native workspace. The remote encode path
    /// calls this after every admitted encode so the memory lives and dies inside that
    /// invocation's reservation; the next zstd encode re-creates it lazily.
    pub(crate) fn release_zstd(&mut self) {
        self.zstd = None;
    }

    /// Drops the cached zstd context when its workspace outgrew
    /// [`MAX_RETAINED_ZSTD_CONTEXT_BYTES`] (a session reset keeps the allocation); the next
    /// encode re-creates it lazily.
    pub(crate) fn release_zstd_if_oversized(&mut self) {
        if self
            .zstd
            .as_ref()
            .is_some_and(|cctx| cctx.sizeof() > MAX_RETAINED_ZSTD_CONTEXT_BYTES)
        {
            self.zstd = None;
        }
    }

    /// Test hook for the release-vs-retain contract of the two encode paths.
    #[cfg(test)]
    pub(crate) fn holds_zstd_cctx(&self) -> bool {
        self.zstd.is_some()
    }

    /// Test hook: zstd contexts created so far, for pinning reuse across blocks.
    #[cfg(test)]
    pub(crate) fn creation_count(&self) -> u32 {
        self.zstd_creations
    }
}

/// Decode-side counterpart of [`ShuffleCodecContext`]: one context serves every frame a
/// reader decodes instead of allocating a fresh zstd context per frame.
#[derive(Default)]
pub struct ShuffleDecodeContext {
    /// Lazily created, reused across frames.
    zstd: Option<DCtx<'static>>,
    /// How many zstd contexts this value has created, so tests can assert that N frames
    /// cost fewer than N creations instead of only observing retained/released state.
    #[cfg(test)]
    zstd_creations: u32,
}

impl std::fmt::Debug for ShuffleDecodeContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // DCtx is an opaque FFI handle; report only whether a workspace is cached.
        f.debug_struct("ShuffleDecodeContext")
            .field("zstd_cached", &self.zstd.is_some())
            .finish()
    }
}

impl ShuffleDecodeContext {
    /// The shared zstd decompression context primed for one frame. The session reset clears
    /// anything a failed decode (e.g. a truncated fetch) left mid-frame.
    pub(crate) fn zstd_dctx(&mut self) -> io::Result<&mut DCtx<'static>> {
        let dctx = match &mut self.zstd {
            Some(dctx) => dctx,
            none => {
                #[cfg(test)]
                {
                    self.zstd_creations += 1;
                }
                none.insert(DCtx::try_create().ok_or_else(|| {
                    io::Error::other("failed to allocate zstd decompression context")
                })?)
            }
        };
        dctx.reset(ResetDirective::SessionOnly)
            .map_err(map_zstd_error)?;
        Ok(dctx)
    }

    /// Drops the cached zstd context when its workspace outgrew
    /// [`MAX_RETAINED_ZSTD_CONTEXT_BYTES`]: one frame advertising a large window grows the
    /// context past 100 MiB, and a session reset keeps that allocation.
    pub(crate) fn release_zstd_if_oversized(&mut self) {
        if self
            .zstd
            .as_ref()
            .is_some_and(|dctx| dctx.sizeof() > MAX_RETAINED_ZSTD_CONTEXT_BYTES)
        {
            self.zstd = None;
        }
    }

    /// Whether a zstd workspace is currently cached, so owners can check their retention
    /// contract.
    pub fn holds_zstd_dctx(&self) -> bool {
        self.zstd.is_some()
    }

    /// Test hook: zstd contexts created so far, for pinning reuse across frames.
    #[cfg(test)]
    pub(crate) fn creation_count(&self) -> u32 {
        self.zstd_creations
    }
}

fn map_zstd_error(code: usize) -> io::Error {
    io::Error::other(zstd::zstd_safe::get_error_name(code))
}
