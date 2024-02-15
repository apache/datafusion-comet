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

use std::{cmp, mem, ptr};

/// This is a copy of the `rdxsort-rs` crate, with the following changes:
/// - removed `Rdx` implementations for all types except for i64 which is the packed representation
/// of row addresses and partition ids from Spark.

pub trait Rdx {
    /// Sets the number of buckets used by the generic implementation.
    fn cfg_nbuckets() -> usize;

    /// Sets the number of rounds scheduled by the generic implementation.
    fn cfg_nrounds() -> usize;

    /// Returns the bucket, depending on the round.
    ///
    /// This should respect the radix, e.g.:
    ///
    /// - if the number of buckets is `2` and the type is an unsigned integer, then the result is
    ///   the bit starting with the least significant one.
    /// - if the number of buckets is `8` and the type is an unsigned integer, then the result is
    ///   the byte starting with the least significant one.
    ///
    /// **Never** return a bucker greater or equal the number of buckets. See warning above!
    fn get_bucket(&self, round: usize) -> usize;

    /// Describes the fact that the content of a bucket should be copied back in reverse order
    /// after a certain round.
    fn reverse(round: usize, bucket: usize) -> bool;
}

const MASK_LONG_LOWER_40_BITS: u64 = (1u64 << 40) - 1;
const MASK_LONG_UPPER_24_BITS: u64 = !MASK_LONG_LOWER_40_BITS;

/// `Rdx` implementation for particular i64 which represents a packed representation of row address
/// and partition id from Spark.
impl Rdx for i64 {
    #[inline]
    fn cfg_nbuckets() -> usize {
        16
    }

    #[inline]
    fn cfg_nrounds() -> usize {
        // Partition id is 3 bytes. Each byte has 2 rounds.
        6
    }

    #[inline]
    fn get_bucket(&self, round: usize) -> usize {
        let partition_id = (*self as u64 & MASK_LONG_UPPER_24_BITS) >> 40;

        let shift = round << 2;
        ((partition_id >> shift) & 15u64) as usize
    }

    #[inline]
    fn reverse(_round: usize, _bucket: usize) -> bool {
        false
    }
}

/// Radix Sort implementation for some type
pub trait RdxSort {
    /// Execute Radix Sort, overwrites (unsorted) content of the type.
    fn rdxsort(&mut self);
}

#[inline]
fn helper_bucket<T, I>(buckets_b: &mut [Vec<T>], iter: I, cfg_nbuckets: usize, round: usize)
where
    T: Rdx,
    I: Iterator<Item = T>,
{
    for x in iter {
        let b = x.get_bucket(round);
        assert!(
            b < cfg_nbuckets,
            "Your Rdx implementation returns a bucket >= cfg_nbuckets()!"
        );
        unsafe {
            buckets_b.get_unchecked_mut(b).push(x);
        }
    }
}

impl<T> RdxSort for [T]
where
    T: Rdx + Clone,
{
    fn rdxsort(&mut self) {
        // config
        let cfg_nbuckets = T::cfg_nbuckets();
        let cfg_nrounds = T::cfg_nrounds();

        // early return
        if cfg_nrounds == 0 {
            return;
        }

        let n = self.len();
        let presize = cmp::max(16, (n << 2) / cfg_nbuckets); // TODO: justify the presize value
        let mut buckets_a: Vec<Vec<T>> = Vec::with_capacity(cfg_nbuckets);
        let mut buckets_b: Vec<Vec<T>> = Vec::with_capacity(cfg_nbuckets);
        for _ in 0..cfg_nbuckets {
            buckets_a.push(Vec::with_capacity(presize));
            buckets_b.push(Vec::with_capacity(presize));
        }

        helper_bucket(&mut buckets_a, self.iter().cloned(), cfg_nbuckets, 0);

        for round in 1..cfg_nrounds {
            for bucket in &mut buckets_b {
                bucket.clear();
            }
            for (i, bucket) in buckets_a.iter().enumerate() {
                if T::reverse(round - 1, i) {
                    helper_bucket(
                        &mut buckets_b,
                        bucket.iter().rev().cloned(),
                        cfg_nbuckets,
                        round,
                    );
                } else {
                    helper_bucket(&mut buckets_b, bucket.iter().cloned(), cfg_nbuckets, round);
                }
            }
            mem::swap(&mut buckets_a, &mut buckets_b);
        }

        let mut pos = 0;
        for (i, bucket) in buckets_a.iter_mut().enumerate() {
            assert!(
                pos + bucket.len() <= self.len(),
                "bug: a buckets got oversized"
            );

            if T::reverse(cfg_nrounds - 1, i) {
                for x in bucket.iter().rev().cloned() {
                    unsafe {
                        *self.get_unchecked_mut(pos) = x;
                    }
                    pos += 1;
                }
            } else {
                unsafe {
                    ptr::copy_nonoverlapping(
                        bucket.as_ptr(),
                        self.get_unchecked_mut(pos),
                        bucket.len(),
                    );
                }
                pos += bucket.len();
            }
        }

        assert!(pos == self.len(), "bug: bucket size does not sum up");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAXIMUM_PARTITION_ID: i32 = (1i32 << 24) - 1;
    const MASK_LONG_LOWER_51_BITS: i64 = (1i64 << 51) - 1;
    const MASK_LONG_UPPER_13_BITS: i64 = !MASK_LONG_LOWER_51_BITS;
    const MASK_LONG_LOWER_27_BITS: i64 = (1i64 << 27) - 1;

    /// Copied from Spark class `PackedRecordPointer`.
    fn pack_pointer(pointer: i64, partition_id: i32) -> i64 {
        assert!(partition_id <= MAXIMUM_PARTITION_ID);

        let page_number = (pointer & MASK_LONG_UPPER_13_BITS) >> 24;
        let compressed_address = page_number | (pointer & MASK_LONG_LOWER_27_BITS);
        ((partition_id as i64) << 40) | compressed_address
    }

    #[test]
    fn test_rdxsort() {
        let mut v = vec![
            pack_pointer(1, 0),
            pack_pointer(2, 3),
            pack_pointer(3, 2),
            pack_pointer(4, 5),
            pack_pointer(5, 0),
            pack_pointer(6, 1),
            pack_pointer(7, 3),
            pack_pointer(8, 3),
        ];
        v.rdxsort();

        let expected = vec![
            pack_pointer(1, 0),
            pack_pointer(5, 0),
            pack_pointer(6, 1),
            pack_pointer(3, 2),
            pack_pointer(2, 3),
            pack_pointer(7, 3),
            pack_pointer(8, 3),
            pack_pointer(4, 5),
        ];

        assert_eq!(v, expected);
    }
}
