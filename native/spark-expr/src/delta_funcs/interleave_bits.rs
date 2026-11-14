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

use arrow::array::{Array, AsArray, BinaryArray, BinaryBuilder, Int32Array};
use arrow::datatypes::{DataType, Int32Type};
use datafusion::common::{plan_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::sync::Arc;

const NAME: &str = "delta_interleave_bits";

/// Largest number of inputs the kernel accepts; two 32-bit inputs fill the `u64` key.
const MAX_INPUTS: usize = 2;

const BITS_PER_INPUT: u32 = i32::BITS;
const BYTES_PER_INPUT: usize = 4;

/// Delta Lake's `InterleaveBits`, the Z-order key `OPTIMIZE ... ZORDER BY` sorts on.
///
/// Takes one or two `Int32` inputs and returns `4 * n` bytes: bits 31 down to 0 of each input's
/// two's-complement value, round-robin in argument order, so the first input supplies the higher
/// bit of each group. A null input counts as 0 and the result is never null.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DeltaInterleaveBits {
    signature: Signature,
}

impl DeltaInterleaveBits {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Int32, DataType::Int32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for DeltaInterleaveBits {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for DeltaInterleaveBits {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        check_arg_types(arg_types)?;
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let number_rows = args.number_rows;
        let args = args.args;
        check_arg_types(&args.iter().map(|a| a.data_type()).collect::<Vec<_>>())?;
        let all_scalar = args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let rows = if all_scalar { 1 } else { number_rows };
        let arrays = args
            .iter()
            .map(|a| a.to_array(rows))
            .collect::<Result<Vec<_>>>()?;
        let columns = arrays
            .iter()
            .map(|a| a.as_primitive::<Int32Type>())
            .collect::<Vec<_>>();
        let result = interleave_columns(&columns, rows);
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

fn check_arg_types(arg_types: &[DataType]) -> Result<()> {
    if !(1..=MAX_INPUTS).contains(&arg_types.len()) {
        return plan_err!(
            "{NAME} expects 1 to {MAX_INPUTS} arguments, got {}",
            arg_types.len()
        );
    }
    match arg_types.iter().find(|t| **t != DataType::Int32) {
        Some(other) => plan_err!("{NAME} expects Int32 input, got {other}"),
        None => Ok(()),
    }
}

fn interleave_columns(columns: &[&Int32Array], rows: usize) -> BinaryArray {
    let width = BYTES_PER_INPUT * columns.len();
    let mut builder = BinaryBuilder::with_capacity(rows, rows * width);
    let mut values = [0u32; MAX_INPUTS];
    for row in 0..rows {
        for (value, column) in values.iter_mut().zip(columns) {
            *value = if column.is_null(row) {
                0
            } else {
                column.value(row) as u32
            };
        }
        let key = interleave(&values[..columns.len()]);
        builder.append_value(&key.to_be_bytes()[size_of::<u64>() - width..]);
    }
    builder.finish()
}

/// Interleaves `values` MSB-first, one bit from each value in turn, into the low
/// `32 * values.len()` bits of the key.
fn interleave(values: &[u32]) -> u64 {
    let mut key = 0u64;
    for pos in (0..BITS_PER_INPUT).rev() {
        for &value in values {
            key = (key << 1) | u64::from((value >> pos) & 1);
        }
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow::buffer::{NullBuffer, ScalarBuffer};
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    /// Interleaved keys Delta's own `InterleaveBits.interleaveBits` produced for these inputs,
    /// with the fast path both on and off (the two agreed on every row).
    const DELTA_GOLDENS: &[(&[i32], &str)] = &[
        (&[i32::MIN], "80000000"),
        (&[i32::MAX], "7FFFFFFF"),
        (&[0], "00000000"),
        (&[-1], "FFFFFFFF"),
        (&[1], "00000001"),
        (&[1431655765], "55555555"),
        (&[-1431655766], "AAAAAAAA"),
        (&[1044735], "000FF0FF"),
        (&[-1044736], "FFF00F00"),
        (&[16909060], "01020304"),
        (&[i32::MIN, i32::MIN], "C000000000000000"),
        (&[i32::MIN, i32::MAX], "9555555555555555"),
        (&[i32::MIN, 0], "8000000000000000"),
        (&[i32::MIN, -1], "D555555555555555"),
        (&[i32::MIN, 1], "8000000000000001"),
        (&[i32::MIN, 1431655765], "9111111111111111"),
        (&[i32::MIN, -1431655766], "C444444444444444"),
        (&[i32::MIN, 1044735], "8000005555005555"),
        (&[i32::MIN, -1044736], "D555550000550000"),
        (&[i32::MIN, 16909060], "8001000400050010"),
        (&[i32::MAX, i32::MIN], "6AAAAAAAAAAAAAAA"),
        (&[i32::MAX, i32::MAX], "3FFFFFFFFFFFFFFF"),
        (&[i32::MAX, 0], "2AAAAAAAAAAAAAAA"),
        (&[i32::MAX, -1], "7FFFFFFFFFFFFFFF"),
        (&[i32::MAX, 1], "2AAAAAAAAAAAAAAB"),
        (&[i32::MAX, 1431655765], "3BBBBBBBBBBBBBBB"),
        (&[i32::MAX, -1431655766], "6EEEEEEEEEEEEEEE"),
        (&[i32::MAX, 1044735], "2AAAAAFFFFAAFFFF"),
        (&[i32::MAX, -1044736], "7FFFFFAAAAFFAAAA"),
        (&[i32::MAX, 16909060], "2AABAAAEAAAFAABA"),
        (&[0, i32::MIN], "4000000000000000"),
        (&[0, i32::MAX], "1555555555555555"),
        (&[0, 0], "0000000000000000"),
        (&[0, -1], "5555555555555555"),
        (&[0, 1], "0000000000000001"),
        (&[0, 1431655765], "1111111111111111"),
        (&[0, -1431655766], "4444444444444444"),
        (&[0, 1044735], "0000005555005555"),
        (&[0, -1044736], "5555550000550000"),
        (&[0, 16909060], "0001000400050010"),
        (&[-1, i32::MIN], "EAAAAAAAAAAAAAAA"),
        (&[-1, i32::MAX], "BFFFFFFFFFFFFFFF"),
        (&[-1, 0], "AAAAAAAAAAAAAAAA"),
        (&[-1, -1], "FFFFFFFFFFFFFFFF"),
        (&[-1, 1], "AAAAAAAAAAAAAAAB"),
        (&[-1, 1431655765], "BBBBBBBBBBBBBBBB"),
        (&[-1, -1431655766], "EEEEEEEEEEEEEEEE"),
        (&[-1, 1044735], "AAAAAAFFFFAAFFFF"),
        (&[-1, -1044736], "FFFFFFAAAAFFAAAA"),
        (&[-1, 16909060], "AAABAAAEAAAFAABA"),
        (&[1, i32::MIN], "4000000000000002"),
        (&[1, i32::MAX], "1555555555555557"),
        (&[1, 0], "0000000000000002"),
        (&[1, -1], "5555555555555557"),
        (&[1, 1], "0000000000000003"),
        (&[1, 1431655765], "1111111111111113"),
        (&[1, -1431655766], "4444444444444446"),
        (&[1, 1044735], "0000005555005557"),
        (&[1, -1044736], "5555550000550002"),
        (&[1, 16909060], "0001000400050012"),
        (&[1431655765, i32::MIN], "6222222222222222"),
        (&[1431655765, i32::MAX], "3777777777777777"),
        (&[1431655765, 0], "2222222222222222"),
        (&[1431655765, -1], "7777777777777777"),
        (&[1431655765, 1], "2222222222222223"),
        (&[1431655765, 1431655765], "3333333333333333"),
        (&[1431655765, -1431655766], "6666666666666666"),
        (&[1431655765, 1044735], "2222227777227777"),
        (&[1431655765, -1044736], "7777772222772222"),
        (&[1431655765, 16909060], "2223222622272232"),
        (&[-1431655766, i32::MIN], "C888888888888888"),
        (&[-1431655766, i32::MAX], "9DDDDDDDDDDDDDDD"),
        (&[-1431655766, 0], "8888888888888888"),
        (&[-1431655766, -1], "DDDDDDDDDDDDDDDD"),
        (&[-1431655766, 1], "8888888888888889"),
        (&[-1431655766, 1431655765], "9999999999999999"),
        (&[-1431655766, -1431655766], "CCCCCCCCCCCCCCCC"),
        (&[-1431655766, 1044735], "888888DDDD88DDDD"),
        (&[-1431655766, -1044736], "DDDDDD8888DD8888"),
        (&[-1431655766, 16909060], "8889888C888D8898"),
        (&[1044735, i32::MIN], "400000AAAA00AAAA"),
        (&[1044735, i32::MAX], "155555FFFF55FFFF"),
        (&[1044735, 0], "000000AAAA00AAAA"),
        (&[1044735, -1], "555555FFFF55FFFF"),
        (&[1044735, 1], "000000AAAA00AAAB"),
        (&[1044735, 1431655765], "111111BBBB11BBBB"),
        (&[1044735, -1431655766], "444444EEEE44EEEE"),
        (&[1044735, 1044735], "000000FFFF00FFFF"),
        (&[1044735, -1044736], "555555AAAA55AAAA"),
        (&[1044735, 16909060], "000100AEAA05AABA"),
        (&[-1044736, i32::MIN], "EAAAAA0000AA0000"),
        (&[-1044736, i32::MAX], "BFFFFF5555FF5555"),
        (&[-1044736, 0], "AAAAAA0000AA0000"),
        (&[-1044736, -1], "FFFFFF5555FF5555"),
        (&[-1044736, 1], "AAAAAA0000AA0001"),
        (&[-1044736, 1431655765], "BBBBBB1111BB1111"),
        (&[-1044736, -1431655766], "EEEEEE4444EE4444"),
        (&[-1044736, 1044735], "AAAAAA5555AA5555"),
        (&[-1044736, -1044736], "FFFFFF0000FF0000"),
        (&[-1044736, 16909060], "AAABAA0400AF0010"),
        (&[16909060, i32::MIN], "40020008000A0020"),
        (&[16909060, i32::MAX], "1557555D555F5575"),
        (&[16909060, 0], "00020008000A0020"),
        (&[16909060, -1], "5557555D555F5575"),
        (&[16909060, 1], "00020008000A0021"),
        (&[16909060, 1431655765], "11131119111B1131"),
        (&[16909060, -1431655766], "4446444C444E4464"),
        (&[16909060, 1044735], "0002005D550A5575"),
        (&[16909060, -1044736], "55575508005F0020"),
        (&[16909060, 16909060], "0003000C000F0030"),
        (&[-194441980, -713859073], "FB313D8711F75575"),
        (&[1465097161, 551116650], "262A734B93DFB4C6"),
        (&[-2044496387, -1402509360], "C4781C1F3E37FBA2"),
        (&[-42021569, 936478219], "AFB77BA9E0B60AEF"),
        (&[-371001683, -1494708188], "EC96FC48EB288CB2"),
        (&[-11981971, -1746055248], "EBBF74D318DF6DA2"),
        (&[1396948856, -1908031548], "625E301BCFFF7A90"),
        (&[-964581762, -717158876], "F1399003883C2EB8"),
        (&[-1802157460, 629602743], "8631C23675496DB5"),
        (&[47397773, -796979038], "5108B75F0B8AC4A6"),
        (&[2119901077, 859130386], "2FAD279B12EE8326"),
        (&[-681693820, -2140680892], "E22A36BD58F39030"),
        (&[2129441368, -1087931688], "6FFDACB59F4C73C0"),
        (&[458445216, -393048690], "56CA630E6253C854"),
        (&[1762056806, -117967399], "7DC2553DF7997969"),
        (&[1232985102, -1191240484], "65C27FF7A3F051F8"),
        (&[-650702380, -866216185], "F2D21B7E4317A235"),
        (&[-1061812808, 807767500], "A5008E394073DAD0"),
        (&[-293689653, 887702561], "ADB87EE99838A48B"),
        (&[1552189062, -1186747436], "67E19025C46DD138"),
    ];

    fn invoke(args: Vec<ColumnarValue>, number_rows: usize) -> Result<ColumnarValue> {
        DeltaInterleaveBits::new().invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows,
            return_field: Arc::new(Field::new("result", DataType::Binary, false)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn int_array(values: Vec<Option<i32>>) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(Int32Array::from(values)))
    }

    /// Invokes the function on array arguments and returns each row as upper-case hex.
    fn hex_rows(columns: Vec<Vec<Option<i32>>>) -> Vec<String> {
        let rows = columns[0].len();
        let args = columns.into_iter().map(int_array).collect();
        let ColumnarValue::Array(result) = invoke(args, rows).unwrap() else {
            panic!("expected an array result");
        };
        assert_eq!(result.null_count(), 0);
        to_hex_rows(&result)
    }

    fn to_hex_rows(array: &ArrayRef) -> Vec<String> {
        array
            .as_binary::<i32>()
            .iter()
            .map(|bytes| to_hex(bytes.expect("result is never null")))
            .collect()
    }

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02X}")).collect()
    }

    #[test]
    fn oracle_two_inputs() {
        let rows = hex_rows(vec![
            vec![Some(0x000ff0ff)],
            vec![Some(0xfff00f00_u32 as i32)],
        ]);
        assert_eq!(rows, ["555555AAAA55AAAA"]);
    }

    #[test]
    fn null_counts_as_zero() {
        let rows = hex_rows(vec![vec![Some(-1), None], vec![None, Some(-1)]]);
        assert_eq!(rows, ["AAAAAAAAAAAAAAAA", "5555555555555555"]);
    }

    #[test]
    fn null_slot_ignores_its_value() {
        // The value buffer holds -1 under the null slot, unlike the 0 a builder writes there.
        let under_null = || {
            ColumnarValue::Array(Arc::new(Int32Array::new(
                ScalarBuffer::from(vec![-1, -1]),
                Some(NullBuffer::from(vec![true, false])),
            )))
        };
        let minus_one = || int_array(vec![Some(-1), Some(-1)]);
        let cases = [
            (
                vec![minus_one(), under_null()],
                ["FFFFFFFFFFFFFFFF", "AAAAAAAAAAAAAAAA"],
            ),
            (
                vec![under_null(), minus_one()],
                ["FFFFFFFFFFFFFFFF", "5555555555555555"],
            ),
            (vec![under_null()], ["FFFFFFFF", "00000000"]),
        ];
        for (args, expected) in cases {
            let ColumnarValue::Array(result) = invoke(args, 2).unwrap() else {
                panic!("expected an array result");
            };
            assert_eq!(to_hex_rows(&result), expected);
        }
    }

    #[test]
    fn one_input_is_big_endian() {
        let rows = hex_rows(vec![vec![Some(i32::MIN), Some(-1), Some(0x01020304)]]);
        assert_eq!(rows, ["80000000", "FFFFFFFF", "01020304"]);
    }

    #[test]
    fn empty_batches() {
        let empty = || int_array(vec![]);
        let scalar = || ColumnarValue::Scalar(ScalarValue::Int32(Some(-1)));
        for args in [
            vec![empty()],
            vec![empty(), empty()],
            vec![empty(), scalar()],
            vec![scalar(), empty()],
        ] {
            let ColumnarValue::Array(result) = invoke(args, 0).unwrap() else {
                panic!("expected an array result");
            };
            assert_eq!(result.data_type(), &DataType::Binary);
            assert_eq!(result.len(), 0);
            assert_eq!(result.null_count(), 0);
        }
    }

    #[test]
    fn scalar_broadcasts() {
        let column = vec![Some(1), Some(2), None];
        let minus_one = || ColumnarValue::Scalar(ScalarValue::Int32(Some(-1)));
        let cases = [
            (
                vec![int_array(column.clone()), minus_one()],
                vec![column.clone(), vec![Some(-1); 3]],
            ),
            (
                vec![minus_one(), int_array(column.clone())],
                vec![vec![Some(-1); 3], column.clone()],
            ),
        ];
        for (args, expanded) in cases {
            let ColumnarValue::Array(result) = invoke(args, column.len()).unwrap() else {
                panic!("expected an array result");
            };
            assert_eq!(to_hex_rows(&result), hex_rows(expanded));
        }
    }

    #[test]
    fn all_scalar_args() {
        let cases: [Vec<Option<i32>>; 5] = [
            vec![Some(0x000ff0ff), Some(0xfff00f00_u32 as i32)],
            vec![None, Some(-1)],
            vec![None, None],
            vec![Some(0x01020304)],
            vec![None],
        ];
        for values in cases {
            let args = values
                .iter()
                .map(|v| ColumnarValue::Scalar(ScalarValue::Int32(*v)))
                .collect();
            let ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) = invoke(args, 1).unwrap()
            else {
                panic!("expected a non-null binary scalar for {values:?}");
            };
            let expected = hex_rows(values.iter().map(|v| vec![*v]).collect());
            assert_eq!(vec![to_hex(&bytes)], expected, "{values:?}");
        }
    }

    #[test]
    fn delta_goldens() {
        for (inputs, expected) in DELTA_GOLDENS {
            let columns = inputs.iter().map(|v| vec![Some(*v)]).collect();
            assert_eq!(hex_rows(columns), [*expected], "{inputs:?}");
        }
    }

    #[test]
    fn rejects_bad_arity() {
        let udf = DeltaInterleaveBits::new();
        for arity in [0, 3] {
            assert!(udf.return_type(&vec![DataType::Int32; arity]).is_err());
            let args = vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(1))); arity];
            assert!(invoke(args, 1).is_err());
        }
    }

    #[test]
    fn rejects_non_int32_input() {
        let udf = DeltaInterleaveBits::new();
        for arg_types in [
            vec![DataType::Int64],
            vec![DataType::Int32, DataType::Int64],
        ] {
            assert!(udf.return_type(&arg_types).is_err(), "{arg_types:?}");
        }
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(1)))];
        assert!(invoke(args, 1).is_err());
    }
}
