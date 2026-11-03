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

//! Sorts a JVM shuffle spill's packed record pointers by partition id.

/// The first bit of the partition id in a Spark `PackedRecordPointer`, which keeps the id in its
/// upper 24 bits.
const PARTITION_ID_SHIFT: u32 = 40;

/// Bytes of the partition id, from the least significant.
const PARTITION_ID_BYTES: usize = 3;

/// Sorts the first `num_records` packed record pointers in `array` by the partition id in their
/// upper 24 bits, keeping the order of the pointers within a partition, using the
/// `num_records` longs after them as scratch space. Core of `Native.sortRowPartitionsNative`.
///
/// This is an LSD radix sort over the partition id's bytes, as Spark's `RadixSort.sort` does for
/// `ShuffleInMemorySorter`, and skips a byte that is the same in every pointer, so that a spill
/// with up to 256 partitions takes one pass. The scratch space is the half of the pointer array
/// that `ShuffleInMemorySorter` keeps free when it sorts with radix sort, so the sort allocates
/// nothing.
///
/// # Panics
/// If `array` holds fewer than `2 * num_records` longs.
pub fn sort_row_partitions(array: &mut [i64], num_records: usize) {
    assert!(
        array.len() / 2 >= num_records,
        "sort_row_partitions needs {num_records} longs of scratch space after {num_records} \
         records, but the array holds {} longs",
        array.len()
    );
    let (records, scratch) = array.split_at_mut(num_records);
    let scratch = &mut scratch[..num_records];

    let mut counts = [[0_usize; 256]; PARTITION_ID_BYTES];
    for &pointer in records.iter() {
        for (byte, counts) in counts.iter_mut().enumerate() {
            counts[digit(pointer, byte)] += 1;
        }
    }

    let mut sorted_in_scratch = false;
    for (byte, counts) in counts.iter().enumerate() {
        // Every pointer has the same value in this byte, so a pass would not move any of them.
        if counts.contains(&num_records) {
            continue;
        }
        let mut offsets = [0_usize; 256];
        let mut offset = 0;
        for (offsets, &count) in offsets.iter_mut().zip(counts) {
            *offsets = offset;
            offset += count;
        }
        if sorted_in_scratch {
            scatter(scratch, records, byte, &mut offsets);
        } else {
            scatter(records, scratch, byte, &mut offsets);
        }
        sorted_in_scratch = !sorted_in_scratch;
    }
    if sorted_in_scratch {
        records.copy_from_slice(scratch);
    }
}

/// The `byte`th byte of the partition id in `pointer`, from the least significant.
#[inline]
fn digit(pointer: i64, byte: usize) -> usize {
    ((pointer as u64) >> (PARTITION_ID_SHIFT as usize + 8 * byte)) as u8 as usize
}

/// Moves `src` into `dst`, ordered stably by the `byte`th byte of their partition ids, where
/// `offsets` holds where in `dst` the pointers of each value of that byte start.
#[inline]
fn scatter(src: &[i64], dst: &mut [i64], byte: usize, offsets: &mut [usize; 256]) {
    for &pointer in src {
        let offset = &mut offsets[digit(pointer, byte)];
        dst[*offset] = pointer;
        *offset += 1;
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

    /// splitmix64.
    fn mix(x: u64) -> u64 {
        let mut x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    /// Sorts `pointers` with `sort_row_partitions` in an array with scratch space filled with
    /// garbage, and checks the result against a stable sort by partition id.
    fn assert_sorts(pointers: &[i64]) {
        let mut array = pointers.to_vec();
        array.extend((0..pointers.len()).map(|i| mix(i as u64) as i64));
        sort_row_partitions(&mut array, pointers.len());

        let mut expected = pointers.to_vec();
        expected.sort_by_key(|pointer| (*pointer as u64) >> PARTITION_ID_SHIFT);
        assert_eq!(&array[..pointers.len()], &expected[..]);
    }

    /// `num_records` pointers whose lower 40 bits are their insertion order, in the partitions
    /// `partition_of` gives for a random value.
    fn pointers(num_records: usize, partition_of: impl Fn(u64) -> u64) -> Vec<i64> {
        (0..num_records)
            .map(|i| ((partition_of(mix(i as u64)) << PARTITION_ID_SHIFT) | i as u64) as i64)
            .collect()
    }

    #[test]
    fn sorts_packed_pointers() {
        let mut array = vec![
            pack_pointer(1, 0),
            pack_pointer(2, 3),
            pack_pointer(3, 2),
            pack_pointer(4, 5),
            pack_pointer(5, 0),
            pack_pointer(6, 1),
            pack_pointer(7, 3),
            pack_pointer(8, 3),
        ];
        array.resize(16, -1);
        sort_row_partitions(&mut array, 8);

        let expected = [
            pack_pointer(1, 0),
            pack_pointer(5, 0),
            pack_pointer(6, 1),
            pack_pointer(3, 2),
            pack_pointer(2, 3),
            pack_pointer(7, 3),
            pack_pointer(8, 3),
            pack_pointer(4, 5),
        ];
        assert_eq!(&array[..8], &expected[..]);
    }

    #[test]
    fn sorts_in_place() {
        let pointers = [
            5_i64 << 40 | 3,
            1,
            2_i64 << 40,
            1_i64 << 40 | 7,
            0,
            5_i64 << 40,
        ];
        assert_sorts(&pointers);
        let mut array = pointers.to_vec();
        array.resize(12, 0);
        sort_row_partitions(&mut array, 6);
        // Stable, so the lower 40 bits keep their input order within a partition.
        assert_eq!(&array[..2], &[1, 0]);
    }

    #[test]
    fn sorts_no_records() {
        let mut array: Vec<i64> = vec![];
        sort_row_partitions(&mut array, 0);
        assert!(array.is_empty());
    }

    #[test]
    fn sorts_one_partition() {
        assert_sorts(&pointers(10_000, |_| 0));
        assert_sorts(&pointers(10_000, |_| 70_000));
    }

    #[test]
    fn sorts_partitions_in_one_byte() {
        assert_sorts(&pointers(10_000, |r| r % 200));
    }

    #[test]
    fn sorts_partitions_in_two_bytes() {
        assert_sorts(&pointers(100_000, |r| r % 4000));
    }

    #[test]
    fn sorts_more_than_65536_partitions() {
        // Three passes, so the sort ends in the scratch space and copies back.
        assert_sorts(&pointers(300_000, |r| r % 100_000));
        assert_sorts(&pointers(100_000, |r| {
            r % (MAXIMUM_PARTITION_ID as u64 + 1)
        }));
    }

    #[test]
    fn sorts_partitions_differing_only_in_upper_bytes() {
        // The lowest byte is the same in every pointer, so its pass is skipped.
        assert_sorts(&pointers(10_000, |r| (r % 300) << 8));
        assert_sorts(&pointers(10_000, |r| ((r % 3) << 16) | 0x42));
    }

    #[test]
    fn sorts_skewed_partitions() {
        assert_sorts(&pointers(100_000, |r| {
            if r % 10 == 0 {
                (r >> 8) % 200
            } else {
                7
            }
        }));
        assert_sorts(&pointers(100_000, |r| {
            if r % 1000 == 0 {
                (r >> 8) % 100_000
            } else {
                0
            }
        }));
    }

    #[test]
    fn leaves_records_beyond_the_scratch_space() {
        let mut array = pointers(1000, |r| r % 100_000);
        let mut expected = array.clone();
        expected.sort_by_key(|pointer| (*pointer as u64) >> PARTITION_ID_SHIFT);
        array.extend(std::iter::repeat_n(0, 1000));
        array.extend([-1, -2, -3]);
        sort_row_partitions(&mut array, 1000);
        assert_eq!(&array[..1000], &expected[..]);
        assert_eq!(&array[2000..], &[-1, -2, -3]);
    }

    #[test]
    #[should_panic(expected = "needs 4 longs of scratch space")]
    fn rejects_an_array_without_scratch_space() {
        let mut array = vec![0_i64; 7];
        sort_row_partitions(&mut array, 4);
    }
}
