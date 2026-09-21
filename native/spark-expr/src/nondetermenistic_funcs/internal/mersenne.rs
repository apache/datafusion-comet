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

//! A bit-for-bit port of Apache Commons Math3's `MersenneTwister`, the PRNG Spark
//! seeds per partition with `randomSeed + partitionIndex`. It backs both `shuffle`
//! (via `RandomIndicesGenerator`) and `uuid` (via `RandomUUIDGenerator`).
//!
//! See:
//! - `org/apache/commons/math3/random/MersenneTwister.java`
//! - `org/apache/commons/math3/random/BitsStreamGenerator.java`

#[derive(Debug, Clone)]
pub(crate) struct SparkMersenneTwister {
    /// Bytes pool.
    mt: [i32; Self::N],
    /// Current index in the bytes pool.
    mti: usize,
}

impl SparkMersenneTwister {
    /// Size of the bytes pool.
    const N: usize = 624;
    /// Period second parameter.
    const M: usize = 397;
    /// X * MATRIX_A for X = {0, 1}.
    const MAG01: [i32; 2] = [0x0, 0x9908b0dfu32 as i32];

    pub(crate) fn new(seed: i64) -> Self {
        // `mti` is set by seeding before it is ever read; 0 is just a placeholder.
        let mut twister = SparkMersenneTwister {
            mt: [0i32; Self::N],
            mti: 0,
        };
        twister.set_seed_long(seed);
        twister
    }

    fn set_seed_int(&mut self, seed: i32) {
        // We use a long masked by 0xffffffff as a poor man's unsigned int.
        let mut long_mt = seed as i64;
        self.mt[0] = long_mt as i32;
        let mut mti = 1usize;
        while mti < Self::N {
            long_mt = (1812433253i64.wrapping_mul(long_mt ^ (long_mt >> 30)) + mti as i64)
                & 0xffffffffi64;
            self.mt[mti] = long_mt as i32;
            mti += 1;
        }
        self.mti = mti;
    }

    fn set_seed_int_array(&mut self, seed: &[i32]) {
        self.set_seed_int(19650218);
        let mut i = 1usize;
        let mut j = 0usize;

        for _ in 0..Self::N.max(seed.len()) {
            let mt_i = self.mt[i] as i64 & 0xffffffffi64;
            let mt_im1 = self.mt[i - 1] as i64 & 0xffffffffi64;
            let l = (mt_i ^ ((mt_im1 ^ (mt_im1 >> 30)).wrapping_mul(1664525)))
                .wrapping_add(seed[j] as i64)
                .wrapping_add(j as i64);
            self.mt[i] = (l & 0xffffffffi64) as i32;
            i += 1;
            j += 1;
            if i >= Self::N {
                self.mt[0] = self.mt[Self::N - 1];
                i = 1;
            }
            if j >= seed.len() {
                j = 0;
            }
        }

        for _ in 0..(Self::N - 1) {
            let mt_i = self.mt[i] as i64 & 0xffffffffi64;
            let mt_im1 = self.mt[i - 1] as i64 & 0xffffffffi64;
            let l = (mt_i ^ ((mt_im1 ^ (mt_im1 >> 30)).wrapping_mul(1566083941)))
                .wrapping_sub(i as i64);
            self.mt[i] = (l & 0xffffffffi64) as i32;
            i += 1;
            if i >= Self::N {
                self.mt[0] = self.mt[Self::N - 1];
                i = 1;
            }
        }

        // MSB is 1, assuring a non-zero initial array.
        self.mt[0] = 0x80000000u32 as i32;
    }

    fn set_seed_long(&mut self, seed: i64) {
        self.set_seed_int_array(&[(seed >> 32) as i32, seed as i32]);
    }

    fn next(&mut self, bits: u32) -> i32 {
        let mut y: i32;
        if self.mti >= Self::N {
            // Generate N words at one time.
            let mut mt_next = self.mt[0];
            for k in 0..(Self::N - Self::M) {
                let mt_curr = mt_next;
                mt_next = self.mt[k + 1];
                y = (mt_curr & (0x80000000u32 as i32)) | (mt_next & 0x7fffffff);
                self.mt[k] = self.mt[k + Self::M]
                    ^ (((y as u32) >> 1) as i32)
                    ^ Self::MAG01[(y & 0x1) as usize];
            }
            for k in (Self::N - Self::M)..(Self::N - 1) {
                let mt_curr = mt_next;
                mt_next = self.mt[k + 1];
                y = (mt_curr & (0x80000000u32 as i32)) | (mt_next & 0x7fffffff);
                self.mt[k] = self.mt[k + Self::M - Self::N]
                    ^ (((y as u32) >> 1) as i32)
                    ^ Self::MAG01[(y & 0x1) as usize];
            }
            y = (mt_next & (0x80000000u32 as i32)) | (self.mt[0] & 0x7fffffff);
            self.mt[Self::N - 1] =
                self.mt[Self::M - 1] ^ (((y as u32) >> 1) as i32) ^ Self::MAG01[(y & 0x1) as usize];
            self.mti = 0;
        }

        y = self.mt[self.mti];
        self.mti += 1;

        // Tempering.
        y ^= ((y as u32) >> 11) as i32;
        y ^= (y << 7) & (0x9d2c5680u32 as i32);
        y ^= (y << 15) & (0xefc60000u32 as i32);
        y ^= ((y as u32) >> 18) as i32;

        ((y as u32) >> (32 - bits)) as i32
    }

    /// Port of `BitsStreamGenerator.nextInt(int n)`. The caller always passes a
    /// strictly positive `n`, matching Spark's `random.nextInt(i + 1)`.
    // `isolate_lowest_one` requires Rust 1.97, newer than Comet's Rust 1.88 MSRV.
    #[allow(unknown_lints, clippy::manual_isolate_lowest_one)]
    pub(crate) fn next_int(&mut self, n: i32) -> i32 {
        if (n & n.wrapping_neg()) == n {
            // n is a power of two.
            return ((n as i64 * self.next(31) as i64) >> 31) as i32;
        }
        loop {
            let bits = self.next(31);
            let val = bits % n;
            if bits.wrapping_sub(val).wrapping_add(n.wrapping_sub(1)) >= 0 {
                return val;
            }
        }
    }

    /// Port of `BitsStreamGenerator.nextLong()`: two 32-bit draws combined into a
    /// signed 64-bit value. Used by `RandomUUIDGenerator`.
    pub(crate) fn next_long(&mut self) -> i64 {
        let high = (self.next(32) as i64) << 32;
        let low = (self.next(32) as i64) & 0xffffffffi64;
        high | low
    }
}
