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

// https://github.com/openjdk/jdk/blob/master/src/java.base/share/classes/jdk/internal/math/DoubleToDecimal.java
//
// For full details about this code see the following references:
//
// [1] Giulietti, "The Schubfach way to render doubles",
//     https://drive.google.com/file/d/1gp5xv4CAa78SVgCeWfGqqI4FfYYYuNFb
//
// [2] IEEE Computer Society, "IEEE Standard for Floating-Point Arithmetic"
//
// Divisions are avoided altogether for the benefit of those architectures
// that do not provide specific machine instructions or where they are slow.
// This is discussed in section 10 of [1].

// The precision in bits
const P: i32 = f64::MANTISSA_DIGITS as i32;
// Exponent width in bits
const W: i32 = size_of::<f64>() as i32 * 8 - 1 - (P - 1);
// Minimum value of the exponent: -(2^(W-1)) - P + 3
const Q_MIN: i32 = (-1 << (W - 1)) - P + 3;
// Threshold to detect tiny values, as in section 8.2.1 of [1]
const C_TINY: i64 = 3;
// The minimum k, as in section 8 of [1]
const K_MIN: i32 = -324;
// Minimum value of the significand of a normal value: 2^(P-1)
const C_MIN: i64 = 1 << (P - 1);
// Mask to extract the biased exponent
const BQ_MASK: i32 = (1 << W) - 1;
// Mask to extract the fraction bits
const T_MASK: u64 = (1 << (P - 1)) - 1;
// Used in rop()
const MASK_63: i64 = 0x7fff_ffff_ffff_ffff;

pub fn to_decimal(v: f64) -> Option<(i64, i32)> {
    // For full details see references [2] and [1].
    //
    // For finite v != 0, determine integers c and q such that
    //     |v| = c 2^q    and
    //     Q_MIN <= q <= Q_MAX    and
    //         either    2^(P-1) <= c < 2^P                 (normal)
    //         or        0 < c < 2^(P-1)  and  q = Q_MIN    (subnormal)

    let bits = v.to_bits();
    let t = (bits & T_MASK) as i64;
    let bq = (bits >> (P - 1)) as i32 & BQ_MASK;
    if bq < BQ_MASK {
        if bq != 0 {
            // normal value. Here mq = -q
            let mq = -Q_MIN + 1 - bq;
            let c = C_MIN | t;
            // The fast path discussed in section 8.3 of [1]
            if 0 < mq && mq < P {
                let f = c >> mq;
                if f << mq == c {
                    return Some((f, 0));
                }
            }
            return Some(finite_positive_to_decimal(-mq, c, 0));
        }
        if t != 0 {
            // subnormal value
            return Some(if t < C_TINY {
                finite_positive_to_decimal(Q_MIN, 10 * t, -1)
            } else {
                finite_positive_to_decimal(Q_MIN, t, 0)
            });
        }
        return Some((0, 0));
    }
    None
}

fn finite_positive_to_decimal(q: i32, c: i64, dk: i32) -> (i64, i32) {
    // The skeleton corresponds to figure 7 of [1].
    // The efficient computations are those summarized in figure 9.
    //
    // Here's a correspondence between variable names and names in [1],
    // expressed as approximate LaTeX source code and informally.
    // Other names are identical.
    // cb:     \bar{c}     "c-bar"
    // cbr:    \bar{c}_r   "c-bar-r"
    // cbl:    \bar{c}_l   "c-bar-l"
    //
    // vb:     \bar{v}     "v-bar"
    // vbr:    \bar{v}_r   "v-bar-r"
    // vbl:    \bar{v}_l   "v-bar-l"
    //
    // rop:    r_o'        "r-o-prime"

    let out = c & 1;
    let cb = c << 2;
    let cbr = cb + 2;
    // floor_log10_pow2(e) = floor(log_10(2^e))
    // floor_log10_three_quarters_pow2(e) = floor(log_10(3/4 2^e))
    // floor_log2_pow10(e) = floor(log_2(10^e))
    let (cbl, k) = if c != C_MIN || q == Q_MIN {
        // regular spacing
        (cb - 2, floor_log10_pow2(q))
    } else {
        // irregular spacing
        (cb - 1, floor_log10_three_quarters_pow2(q))
    };
    let h = q + floor_log2_pow10(-k) + 2;

    // g1 and g0 are as in section 9.8.3 of [1], so g = g1 2^63 + g0
    let g1 = g1(k);
    let g0 = g0(k);

    let vb = rop(g1, g0, cb << h);
    let vbl = rop(g1, g0, cbl << h);
    let vbr = rop(g1, g0, cbr << h);

    let s = vb >> 2;
    if s >= 100 {
        // For n = 17, m = 1 the table in section 10 of [1] shows
        //     s' = floor(s / 10) = floor(s 115_292_150_460_684_698 / 2^60)
        //        = floor(s 115_292_150_460_684_698 2^4 / 2^64)
        //
        // sp10 = 10 s'
        // tp10 = 10 t'
        // upin    iff    u' = sp10 10^k in Rv
        // wpin    iff    w' = tp10 10^k in Rv
        // See section 9.3 of [1].

        let sp10 =
            10 * ((s as i128 * (115_292_150_460_684_698_i64 << 4) as i128) as u128 >> 64) as i64;
        let tp10 = sp10 + 10;
        let upin = vbl + out <= sp10 << 2;
        let wpin = (tp10 << 2) + out <= vbr;
        if upin != wpin {
            return (if upin { sp10 } else { tp10 }, k);
        }
    }

    // 10 <= s < 100    or    s >= 100  and  u', w' not in Rv
    // uin    iff    u = s 10^k in Rv
    // win    iff    w = t 10^k in Rv
    // See section 9.3 of [1].
    let t = s + 1;
    let uin = vbl + out <= s << 2;
    let win = (t << 2) + out <= vbr;
    if uin != win {
        return (if uin { s } else { t }, k + dk);
    }

    // Both u and w lie in Rv: determine the one closest to v.
    // See section 9.3 of [1].
    let cmp = vb - ((s + t) << 1);
    (
        if cmp < 0 || cmp == 0 && (s & 1) == 0 {
            s
        } else {
            t
        },
        k + dk,
    )
}

/// Computes rop(cp g 2^-127), where g = g1 2^63 + g0
/// See section 9.9 and figure 8 of [1].
fn rop(g1: i64, g0: i64, cp: i64) -> i64 {
    let x1 = ((g0 as i128 * cp as i128) as u128 >> 64) as i64;
    let y0 = g1.wrapping_mul(cp);
    let y1 = ((g1 as i128 * cp as i128) as u128 >> 64) as i64;
    let z = ((y0 as u64 >> 1) as i64).wrapping_add(x1);
    let vbp = y1.wrapping_add((z as u64 >> 63) as i64);
    vbp | (((z & MASK_63).wrapping_add(MASK_63)) as u64 >> 63) as i64
}

// math utils

// C_10 = floor(log_10(2) * 2^Q_10), A_10 = floor(log_10(3/4) * 2^Q_10)
const Q_10: i32 = 41;
const C_10: i64 = 661_971_961_083;
const A_10: i64 = -274_743_187_321;

// C_2 = floor(log_2(10) * 2^Q_2)
const Q_2: i32 = 38;
const C_2: i64 = 913_124_641_741;

/// Returns the unique integer k such that 10^k <= 2^e < 10^(k+1).
///
/// The result is correct when |e| <= 6_432_162.
/// Otherwise the result is undefined.
///
/// # Arguments
///
/// * `e` - The exponent of 2, which should meet |e| <= 6_432_162 for safe results.
///
/// # Returns
///
/// * `i32` - floor(log_10(2^e)).
fn floor_log10_pow2(e: i32) -> i32 {
    ((e as i64 * C_10) >> Q_10) as i32
}

/// Returns the unique integer k such that 10^k <= 3/4 * 2^e < 10^(k+1).
///
/// The result is correct when -3_606_689 < e <= 3_150_619.
/// Otherwise the result is undefined.
///
/// # Arguments
///
/// * `e` - The exponent of 2, which should meet -3_606_689 <= e < 3_150_619 for safe results.
///
/// # Returns
///
/// * `i32` - floor(log_10(3/4 2^e))
fn floor_log10_three_quarters_pow2(e: i32) -> i32 {
    ((e as i64 * C_10 + A_10) >> Q_10) as i32
}

/// Returns the unique integer k such that 2^k <= 10^e < 2^(k+1).
///
/// The result is correct when |e| <= 1_838_394.
/// Otherwise the result is undefined.
///
/// # Arguments
///
/// * `e` - The exponent of 10, which should meet |e| < 1_838_394 for safe results.
///
/// # Returns
///
/// * `i32` - floor(log_2(10^e))
fn floor_log2_pow10(e: i32) -> i32 {
    ((e as i64 * C_2) >> Q_2) as i32
}

/// Let 10^-k = B 2^r, for the unique pair of integer r and real B meeting 2^125 <= B < 2^126.
/// Further, let g = floor(B) + 1. Split g into the higher 63 bits g1 and the lower 63 bits g0.
/// Thus, g1 = floor(g * 2^-63) and g0 = g - g1 * 2^63.
///
/// This method returns g1 while `g0(i32)` returns g0.
///
/// If needed, the exponent r can be computed as r = `floor_log2_pow10(-k)` - 125.
///
/// # Arguments
///
/// * `k` - The exponent of 10, which must meet `K_MIN` <= k <= `K_MAX`.
///
/// # Returns
///
/// * `i64` - g1 as described above.
fn g1(k: i32) -> i64 {
    G[((k - K_MIN) << 1) as usize]
}

/// Returns g0 as described in `g1(i32)`
///
/// # Arguments
///
/// * `k` - The exponent of 10, which must meet `K_MIN` <= k <= `K_MAX`.
///
/// # Returns
///
/// * `i64` - g0 as described in `g1(k)`
fn g0(k: i32) -> i64 {
    G[((k - K_MIN) << 1 | 1) as usize]
}

// The precomputed values for g1(i32) and g0(i32).
// The first entry must be for an exponent of K_MIN or less.
// The last entry must be for an exponent of K_MAX or more.
const G: [i64; 1234] = [
    0x4F0C_EDC9_5A71_8DD4,
    0x5B01_E8B0_9AA0_D1B5, // -324
    0x7E7B_160E_F71C_1621,
    0x119C_A780_F767_B5EE, // -323
    0x652F_44D8_C5B0_11B4,
    0x0E16_EC67_2C52_F7F2, // -322
    0x50F2_9D7A_37C0_0E29,
    0x5812_56B8_F042_5FF5, // -321
    0x40C2_1794_F966_71BA,
    0x79A8_4560_C035_1991, // -320
    0x679C_F287_F570_B5F7,
    0x75DA_089A_CD21_C281, // -319
    0x52E3_F539_9126_F7F9,
    0x44AE_6D48_A41B_0201, // -318
    0x424F_F761_40EB_F994,
    0x36F1_F106_E9AF_34CD, // -317
    0x6A19_8BCE_CE46_5C20,
    0x57E9_81A4_A918_547B, // -316
    0x54E1_3CA5_71D1_E34D,
    0x2CBA_CE1D_5413_76C9, // -315
    0x43E7_63B7_8E41_82A4,
    0x23C8_A4E4_4342_C56E, // -314
    0x6CA5_6C58_E39C_043A,
    0x060D_D4A0_6B9E_08B0, // -313
    0x56EA_BD13_E949_9CFB,
    0x1E71_76E6_BC7E_6D59, // -312
    0x4588_9743_2107_B0C8,
    0x7EC1_2BEB_C9FE_BDE1, // -311
    0x6F40_F205_01A5_E7A7,
    0x7E01_DFDF_A997_9635, // -310
    0x5900_C19D_9AEB_1FB9,
    0x4B34_B319_5479_44F7, // -309
    0x4733_CE17_AF22_7FC7,
    0x55C3_C27A_A9FA_9D93, // -308
    0x71EC_7CF2_B1D0_CC72,
    0x5606_03F7_765D_C8EA, // -307
    0x5B23_9728_8E40_A38E,
    0x7804_CFF9_2B7E_3A55, // -306
    0x48E9_45BA_0B66_E93F,
    0x1337_0CC7_55FE_9511, // -305
    0x74A8_6F90_123E_41FE,
    0x51F1_AE0B_BCCA_881B, // -304
    0x5D53_8C73_41CB_67FE,
    0x74C1_5809_63D5_39AF, // -303
    0x4AA9_3D29_016F_8665,
    0x43CD_E007_8310_FAF3, // -302
    0x7775_2EA8_024C_0A3C,
    0x0616_333F_381B_2B1E, // -301
    0x5F90_F220_01D6_6E96,
    0x3811_C298_F9AF_55B1, // -300
    0x4C73_F4E6_67DE_BEDE,
    0x600E_3547_2E25_DE28, // -299
    0x7A53_2170_A631_3164,
    0x3349_EED8_49D6_303F, // -298
    0x61DC_1AC0_84F4_2783,
    0x42A1_8BE0_3B11_C033, // -297
    0x4E49_AF00_6A5C_EC69,
    0x1BB4_6FE6_95A7_CCF5, // -296
    0x7D42_B19A_43C7_E0A8,
    0x2C53_E63D_BC3F_AE55, // -295
    0x6435_5AE1_CFD3_1A20,
    0x2376_51CA_FCFF_BEAA, // -294
    0x502A_AF1B_0CA8_E1B3,
    0x35F8_416F_30CC_9888, // -293
    0x4022_25AF_3D53_E7C2,
    0x5E60_3458_F3D6_E06D, // -292
    0x669D_0918_621F_D937,
    0x4A33_86F4_B957_CD7B, // -291
    0x5217_3A79_E819_7A92,
    0x6E8F_9F2A_2DDF_D796, // -290
    0x41AC_2EC7_ECE1_2EDB,
    0x720C_7F54_F17F_DFAB, // -289
    0x6913_7E0C_AE35_17C6,
    0x1CE0_CBBB_1BFF_CC45, // -288
    0x540F_980A_24F7_4638,
    0x171A_3C95_AFFF_D69E, // -287
    0x433F_ACD4_EA5F_6B60,
    0x127B_63AA_F333_1218, // -286
    0x6B99_1487_DD65_7899,
    0x6A5F_05DE_51EB_5026, // -285
    0x5614_106C_B11D_FA14,
    0x5518_D17E_A7EF_7352, // -284
    0x44DC_D9F0_8DB1_94DD,
    0x2A7A_4132_1FF2_C2A8, // -283
    0x6E2E_2980_E2B5_BAFB,
    0x5D90_6850_331E_043F, // -282
    0x5824_EE00_B55E_2F2F,
    0x6473_86A6_8F4B_3699, // -281
    0x4683_F19A_2AB1_BF59,
    0x36C2_D21E_D908_F87B, // -280
    0x70D3_1C29_DDE9_3228,
    0x579E_1CFE_280E_5A5D, // -279
    0x5A42_7CEE_4B20_F4ED,
    0x2C7E_7D98_200B_7B7E, // -278
    0x4835_30BE_A280_C3F1,
    0x09FE_CAE0_19A2_C932, // -277
    0x7388_4DFD_D0CE_064E,
    0x4331_4499_C29E_0EB6, // -276
    0x5C6D_0B31_73D8_050B,
    0x4F5A_9D47_CEE4_D891, // -275
    0x49F0_D5C1_2979_9DA2,
    0x72AE_E439_7250_AD41, // -274
    0x764E_22CE_A8C2_95D1,
    0x377E_39F5_83B4_4868, // -273
    0x5EA4_E8A5_53CE_DE41,
    0x12CB_6191_3629_D387, // -272
    0x4BB7_2084_430B_E500,
    0x756F_8140_F821_7605, // -271
    0x7925_00D3_9E79_6E67,
    0x6F18_CECE_59CF_233C, // -270
    0x60EA_670F_B1FA_BEB9,
    0x3F47_0BD8_47D8_E8FD, // -269
    0x4D88_5272_F4C8_9894,
    0x329F_3CAD_0647_20CA, // -268
    0x7C0D_50B7_EE0D_C0ED,
    0x3765_2DE1_A3A5_0143, // -267
    0x633D_DA2C_BE71_6724,
    0x2C50_F181_4FB7_3436, // -266
    0x4F64_AE8A_31F4_5283,
    0x3D0D_8E01_0C92_902B, // -265
    0x7F07_7DA9_E986_EA6B,
    0x7B48_E334_E0EA_8045, // -264
    0x659F_97BB_2138_BB89,
    0x4907_1C2A_4D88_669D, // -263
    0x514C_7962_80FA_2FA1,
    0x20D2_7CEE_A46D_1EE4, // -262
    0x4109_FAB5_33FB_594D,
    0x670E_CA58_838A_7F1D, // -261
    0x680F_F788_532B_C216,
    0x0B4A_DD5A_6C10_CB62, // -260
    0x533F_F939_DC23_01AB,
    0x22A2_4AAE_BCDA_3C4E, // -259
    0x4299_942E_49B5_9AEF,
    0x354E_A225_63E1_C9D8, // -258
    0x6A8F_537D_42BC_2B18,
    0x554A_9D08_9FCF_A95A, // -257
    0x553F_75FD_CEFC_EF46,
    0x776E_E406_E63F_BAAE, // -256
    0x4432_C4CB_0BFD_8C38,
    0x5F8B_E99F_1E99_6225, // -255
    0x6D1E_07AB_4662_79F4,
    0x3279_75CB_6428_9D08, // -254
    0x574B_3955_D1E8_6190,
    0x2861_2B09_1CED_4A6D, // -253
    0x45D5_C777_DB20_4E0D,
    0x06B4_226D_B0BD_D524, // -252
    0x6FBC_7259_5E9A_167B,
    0x2453_6A49_1AC9_5506, // -251
    0x5963_8EAD_E548_11FC,
    0x1D0F_883A_7BD4_4405, // -250
    0x4782_D88B_1DD3_4196,
    0x4A72_D361_FCA9_D004, // -249
    0x726A_F411_C952_028A,
    0x43EA_EBCF_FAA9_4CD3, // -248
    0x5B88_C341_6DDB_353B,
    0x4FEF_230C_C887_70A9, // -247
    0x493A_35CD_F17C_2A96,
    0x0CBF_4F3D_6D39_26EE, // -246
    0x7529_EFAF_E8C6_AA89,
    0x6132_1862_485B_717C, // -245
    0x5DBB_2626_53D2_2207,
    0x675B_46B5_06AF_8DFD, // -244
    0x4AFC_1E85_0FDB_4E6C,
    0x52AF_6BC4_0559_3E64, // -243
    0x77F9_CA6E_7FC5_4A47,
    0x377F_12D3_3BC1_FD6D, // -242
    0x5FFB_0858_6637_6E9F,
    0x45FF_4242_9634_CABD, // -241
    0x4CC8_D379_EB5F_8BB2,
    0x6B32_9B68_782A_3BCB, // -240
    0x7ADA_EBF6_4565_AC51,
    0x2B84_2BDA_59DD_2C77, // -239
    0x6248_BCC5_0451_56A7,
    0x3C69_BCAE_AE4A_89F9, // -238
    0x4EA0_9704_0374_4552,
    0x6387_CA25_583B_A194, // -237
    0x7DCD_BE6C_D253_A21E,
    0x05A6_103B_C05F_68ED, // -236
    0x64A4_9857_0EA9_4E7E,
    0x37B8_0CFC_99E5_ED8A, // -235
    0x5083_AD12_7221_0B98,
    0x2C93_3D96_E184_BE08, // -234
    0x4069_5741_F4E7_3C79,
    0x7075_CADF_1AD0_9807, // -233
    0x670E_F203_2171_FA5C,
    0x4D89_4498_2AE7_59A4, // -232
    0x5272_5B35_B45B_2EB0,
    0x3E07_6A13_5585_E150, // -231
    0x41F5_15C4_9048_F226,
    0x64D2_BB42_AAD1_810D, // -230
    0x6988_22D4_1A0E_503E,
    0x07B7_9204_4482_6815, // -229
    0x546C_E8A9_AE71_D9CB,
    0x1FC6_0E69_D068_5344, // -228
    0x438A_53BA_F1F4_AE3C,
    0x196B_3EBB_0D20_429D, // -227
    0x6C10_85F7_E987_7D2D,
    0x0F11_FDF8_1500_6A94, // -226
    0x5673_9E5F_EE05_FDBD,
    0x58DB_3193_4400_5543, // -225
    0x4529_4B7F_F19E_6497,
    0x60AF_5ADC_3666_AA9C, // -224
    0x6EA8_78CC_B5CA_3A8C,
    0x344B_C493_8A3D_DDC7, // -223
    0x5886_C70A_2B08_2ED6,
    0x5D09_6A0F_A1CB_17D2, // -222
    0x46D2_38D4_EF39_BF12,
    0x173A_BB3F_B4A2_7975, // -221
    0x7150_5AEE_4B8F_981D,
    0x0B91_2B99_2103_F588, // -220
    0x5AA6_AF25_093F_ACE4,
    0x0940_EFAD_B403_2AD3, // -219
    0x4885_58EA_6DCC_8A50,
    0x0767_2624_9002_88A9, // -218
    0x7408_8E43_E2E0_DD4C,
    0x723E_A36D_B337_410E, // -217
    0x5CD3_A503_1BE7_1770,
    0x5B65_4F8A_F5C5_CDA5, // -216
    0x4A42_EA68_E31F_45F3,
    0x62B7_72D5_916B_0AEB, // -215
    0x76D1_770E_3832_0986,
    0x0458_B7BC_1BDE_77DD, // -214
    0x5F0D_F8D8_2CF4_D46B,
    0x1D13_C630_164B_9318, // -213
    0x4C0B_2D79_BD90_A9EF,
    0x30DC_9E8C_DEA2_DC13, // -212
    0x79AB_7BF5_FC1A_A97F,
    0x0160_FDAE_3104_9351, // -211
    0x6155_FCC4_C9AE_EDFF,
    0x1AB3_FE24_F403_A90E, // -210
    0x4DDE_63D0_A158_BE65,
    0x6229_981D_9002_EDA5, // -209
    0x7C97_061A_9BC1_30A2,
    0x69DC_2695_B337_E2A1, // -208
    0x63AC_04E2_1634_26E8,
    0x54B0_1EDE_28F9_821B, // -207
    0x4FBC_D0B4_DE90_1F20,
    0x43C0_18B1_BA61_34E2, // -206
    0x7F94_8121_6419_CB67,
    0x1F99_C11C_5D68_549D, // -205
    0x6610_674D_E9AE_3C52,
    0x4C7B_00E3_7DED_107E, // -204
    0x51A6_B90B_2158_3042,
    0x09FC_00B5_FE57_4065, // -203
    0x4152_2DA2_8113_59CE,
    0x3B30_0091_9845_CD1D, // -202
    0x6883_7C37_34EB_C2E3,
    0x784C_CDB5_C06F_AE95, // -201
    0x539C_635F_5D89_68B6,
    0x2D0A_3E2B_0059_5877, // -200
    0x42E3_82B2_B13A_BA2B,
    0x3DA1_CB55_99E1_1393, // -199
    0x6B05_9DEA_B52A_C378,
    0x629C_7888_F634_EC1E, // -198
    0x559E_17EE_F755_692D,
    0x3549_FA07_2B5D_89B1, // -197
    0x447E_798B_F911_20F1,
    0x1107_FB38_EF7E_07C1, // -196
    0x6D97_28DF_F4E8_34B5,
    0x01A6_5EC1_7F30_0C68, // -195
    0x57AC_20B3_2A53_5D5D,
    0x4E1E_B234_65C0_09ED, // -194
    0x4623_4D5C_21DC_4AB1,
    0x24E5_5B5D_1E33_3B24, // -193
    0x7038_7BC6_9C93_AAB5,
    0x216E_F894_FD1E_C506, // -192
    0x59C6_C96B_B076_222A,
    0x4DF2_6077_30E5_6A6C, // -191
    0x47D2_3ABC_8D2B_4E88,
    0x3E5B_805F_5A51_21F0, // -190
    0x72E9_F794_1512_1740,
    0x63C5_9A32_2A1B_697F, // -189
    0x5BEE_5FA9_AA74_DF67,
    0x0304_7B5B_54E2_BACC, // -188
    0x498B_7FBA_EEC3_E5EC,
    0x0269_FC49_10B5_623D, // -187
    0x75AB_FF91_7E06_3CAC,
    0x6A43_2D41_B455_69FB, // -186
    0x5E23_32DA_CB38_308A,
    0x21CF_5767_C377_87FC, // -185
    0x4B4F_5BE2_3C2C_F3A1,
    0x67D9_12B9_692C_6CCA, // -184
    0x787E_F969_F9E1_85CF,
    0x595B_5128_A847_1476, // -183
    0x6065_9454_C7E7_9E3F,
    0x6115_DA86_ED05_A9F8, // -182
    0x4D1E_1043_D31F_B1CC,
    0x4DAB_1538_BD9E_2193, // -181
    0x7B63_4D39_51CC_4FAD,
    0x62AB_5527_95C9_CF52, // -180
    0x62B5_D761_0E3D_0C8B,
    0x0222_AA86_116E_3F75, // -179
    0x4EF7_DF80_D830_D6D5,
    0x4E82_2204_DABE_992A, // -178
    0x7E59_659A_F381_57BC,
    0x1736_9CD4_9130_F510, // -177
    0x6514_5148_C2CD_DFC9,
    0x5F5E_E3DD_40F3_F740, // -176
    0x50DD_0DD3_CF0B_196E,
    0x1918_B64A_9A5C_C5CD, // -175
    0x40B0_D7DC_A5A2_7ABE,
    0x4746_F83B_AEB0_9E3E, // -174
    0x6781_5961_0903_F797,
    0x253E_59F9_1780_FD2F, // -173
    0x52CD_E11A_6D9C_C612,
    0x50FE_AE60_DF9A_6426, // -172
    0x423E_4DAE_BE17_04DB,
    0x5A65_584D_7FAE_B685, // -171
    0x69FD_4917_968B_3AF9,
    0x10A2_26E2_65E4_573B, // -170
    0x54CA_A0DF_ABA2_9594,
    0x0D4E_8581_EB1D_1295, // -169
    0x43D5_4D7F_BC82_1143,
    0x243E_D134_BC17_4211, // -168
    0x6C88_7BFF_9403_4ED2,
    0x06CA_E854_6025_3682, // -167
    0x56D3_9666_1002_A574,
    0x6BD5_86A9_E684_2B9B, // -166
    0x4576_11EB_4002_1DF7,
    0x0977_9EEE_5203_5616, // -165
    0x6F23_4FDE_CCD0_2FF1,
    0x5BF2_97E3_B66B_BCEF, // -164
    0x58E9_0CB2_3D73_598E,
    0x165B_ACB6_2B89_63F3, // -163
    0x4720_D6F4_FDF5_E13E,
    0x4516_23C4_EFA1_1CC2, // -162
    0x71CE_24BB_2FEF_CECA,
    0x3B56_9FA1_7F68_2E03, // -161
    0x5B0B_5095_BFF3_0BD5,
    0x15DE_E61A_CC53_5803, // -160
    0x48D5_DA11_665C_0977,
    0x2B18_B815_7042_ACCF, // -159
    0x7489_5CE8_A3C6_758B,
    0x5E8D_F355_806A_AE18, // -158
    0x5D3A_B0BA_1C9E_C46F,
    0x653E_5C44_66BB_BE7A, // -157
    0x4A95_5A2E_7D4B_D059,
    0x3765_169D_1EFC_9861, // -156
    0x7755_5D17_2EDF_B3C2,
    0x256E_8A94_FE60_F3CF, // -155
    0x5F77_7DAC_257F_C301,
    0x6ABE_D543_FEB3_F63F, // -154
    0x4C5F_97BC_EACC_9C01,
    0x3BCB_DDCF_FEF6_5E99, // -153
    0x7A32_8C61_77AD_C668,
    0x5FAC_9619_97F0_975B, // -152
    0x61C2_09E7_92F1_6B86,
    0x7FBD_44E1_465A_12AF, // -151
    0x4E34_D4B9_425A_BC6B,
    0x7FCA_9D81_0514_DBBF, // -150
    0x7D21_545B_9D5D_FA46,
    0x32DD_C8CE_6E87_C5FF, // -149
    0x641A_A9E2_E44B_2E9E,
    0x5BE4_A0A5_2539_6B32, // -148
    0x5015_54B5_836F_587E,
    0x7CB6_E6EA_842D_EF5C, // -147
    0x4011_1091_35F2_AD32,
    0x3092_5255_368B_25E3, // -146
    0x6681_B41B_8984_4850,
    0x4DB6_EA21_F0DE_A304, // -145
    0x5201_5CE2_D469_D373,
    0x57C5_881B_2718_826A, // -144
    0x419A_B0B5_76BB_0F8F,
    0x5FD1_39AF_527A_01EF, // -143
    0x68F7_8122_5791_B27F,
    0x4C81_F5E5_50C3_364A, // -142
    0x53F9_341B_7941_5B99,
    0x239B_2B1D_DA35_C508, // -141
    0x432D_C349_2DCD_E2E1,
    0x02E2_88E4_AE91_6A6D, // -140
    0x6B7C_6BA8_4949_6B01,
    0x516A_74A1_174F_10AE, // -139
    0x55FD_22ED_076D_EF34,
    0x4121_F6E7_45D8_DA25, // -138
    0x44CA_8257_3924_BF5D,
    0x1A81_9252_9E47_14EB, // -137
    0x6E10_D08B_8EA1_322E,
    0x5D9C_1D50_FD3E_87DD, // -136
    0x580D_73A2_D880_F4F2,
    0x17B0_1773_FDCB_9FE4, // -135
    0x4671_294F_139A_5D8E,
    0x4626_7929_97D6_1984, // -134
    0x70B5_0EE4_EC2A_2F4A,
    0x3D0A_5B75_BFBC_F59F, // -133
    0x5A2A_7250_BCEE_8C3B,
    0x4A6E_AF91_6630_C47F, // -132
    0x4821_F50D_63F2_09C9,
    0x21F2_260D_EB5A_36CC, // -131
    0x7369_8815_6CB6_760E,
    0x6983_7016_455D_247A, // -130
    0x5C54_6CDD_F091_F80B,
    0x6E02_C011_D117_5062, // -129
    0x49DD_23E4_C074_C66F,
    0x719B_CCDB_0DAC_404E, // -128
    0x762E_9FD4_6721_3D7F,
    0x68F9_47C4_E2AD_33B0, // -127
    0x5E8B_B310_5280_FDFF,
    0x6D94_396A_4EF0_F627, // -126
    0x4BA2_F5A6_A867_3199,
    0x3E10_2DEE_A58D_91B9, // -125
    0x7904_BC3D_DA3E_B5C2,
    0x3019_E317_6F48_E927, // -124
    0x60D0_9697_E1CB_C49B,
    0x4014_B5AC_5907_20EC, // -123
    0x4D73_ABAC_B4A3_03AF,
    0x4CDD_5E23_7A6C_1A57, // -122
    0x7BEC_45E1_2104_D2B2,
    0x47C8_969F_2A46_908A, // -121
    0x6323_6B1A_80D0_A88E,
    0x6CA0_787F_5505_406F, // -120
    0x4F4F_88E2_00A6_ED3F,
    0x0A19_F9FF_7737_66BF, // -119
    0x7EE5_A7D0_010B_1531,
    0x5CF6_5CCB_F1F2_3DFE, // -118
    0x6584_8640_00D5_AA8E,
    0x172B_7D6F_F4C1_CB32, // -117
    0x5136_D1CC_CD77_BBA4,
    0x78EF_978C_C3CE_3C28, // -116
    0x40F8_A7D7_0AC6_2FB7,
    0x13F2_DFA3_CFD8_3020, // -115
    0x67F4_3FBE_77A3_7F8B,
    0x3984_9906_1959_E699, // -114
    0x5329_CC98_5FB5_FFA2,
    0x6136_E0D1_ADE1_8548, // -113
    0x4287_D6E0_4C91_994F,
    0x00F8_B3DA_F181_376D, // -112
    0x6A72_F166_E0E8_F54B,
    0x1B27_862B_1C01_F247, // -111
    0x5528_C11F_1A53_F76F,
    0x2F52_D1BC_1667_F506, // -110
    0x4420_9A7F_4843_2C59,
    0x0C42_4163_451F_F738, // -109
    0x6D00_F732_0D38_46F4,
    0x7A03_9BD2_0833_2526, // -108
    0x5733_F8F4_D760_38C3,
    0x7B36_1641_A028_EA85, // -107
    0x45C3_2D90_AC4C_FA36,
    0x2F5E_7834_8020_BB9E, // -106
    0x6F9E_AF4D_E07B_29F0,
    0x4BCA_59ED_99CD_F8FC, // -105
    0x594B_BF71_8062_87F3,
    0x563B_7B24_7B0B_2D96, // -104
    0x476F_CC5A_CD1B_9FF6,
    0x11C9_2F50_626F_57AC, // -103
    0x724C_7A2A_E1C5_CCBD,
    0x02DB_7EE7_03E5_5912, // -102
    0x5B70_61BB_E7D1_7097,
    0x1BE2_CBEC_031D_E0DC, // -101
    0x4926_B496_530D_F3AC,
    0x164F_0989_9C17_E716, // -100
    0x750A_BA8A_1E7C_B913,
    0x3D4B_4275_C68C_A4F0, //  -99
    0x5DA2_2ED4_E530_940F,
    0x4AA2_9B91_6BA3_B726, //  -98
    0x4AE8_2577_1DC0_7672,
    0x6EE8_7C74_561C_9285, //  -97
    0x77D9_D58B_62CD_8A51,
    0x3173_FA53_BCFA_8408, //  -96
    0x5FE1_77A2_B571_3B74,
    0x278F_FB76_30C8_69A0, //  -95
    0x4CB4_5FB5_5DF4_2F90,
    0x1FA6_62C4_F3D3_87B3, //  -94
    0x7ABA_32BB_C986_B280,
    0x32A3_D13B_1FB8_D91F, //  -93
    0x622E_8EFC_A138_8ECD,
    0x0EE9_742F_4C93_E0E6, //  -92
    0x4E8B_A596_E760_723D,
    0x58BA_C359_0A0F_E71E, //  -91
    0x7DAC_3C24_A567_1D2F,
    0x412A_D228_1019_71C9, //  -90
    0x6489_C9B6_EAB8_E426,
    0x00EF_0E86_7347_8E3B, //  -89
    0x506E_3AF8_BBC7_1CEB,
    0x1A58_D86B_8F6C_71C9, //  -88
    0x4058_2F2D_6305_B0BC,
    0x1513_E056_0C56_C16E, //  -87
    0x66F3_7EAF_04D5_E793,
    0x3B53_0089_AD57_9BE2, //  -86
    0x525C_6558_D0AB_1FA9,
    0x15DC_006E_2446_164F, //  -85
    0x41E3_8447_0D55_B2ED,
    0x5E49_99F1_B69E_783F, //  -84
    0x696C_06D8_1555_EB15,
    0x7D42_8FE9_2430_C065, //  -83
    0x5456_6BE0_1111_88DE,
    0x3102_0CBA_835A_3384, //  -82
    0x4378_564C_DA74_6D7E,
    0x5A68_0A2E_CF7B_5C69, //  -81
    0x6BF3_BD47_C3ED_7BFD,
    0x770C_DD17_B25E_FA42, //  -80
    0x565C_976C_9CBD_FCCB,
    0x1270_B0DF_C1E5_9502, //  -79
    0x4516_DF8A_16FE_63D5,
    0x5B8D_5A4C_9B1E_10CE, //  -78
    0x6E8A_FF43_57FD_6C89,
    0x127B_C3AD_C4FC_E7B0, //  -77
    0x586F_329C_4664_56D4,
    0x0EC9_6957_D0CA_52F3, //  -76
    0x46BF_5BB0_3850_4576,
    0x3F07_8779_73D5_0F29, //  -75
    0x7132_2C4D_26E6_D58A,
    0x31A5_A58F_1FBB_4B75, //  -74
    0x5A8E_89D7_5252_446E,
    0x5AEA_EAD8_E62F_6F91, //  -73
    0x4872_07DF_750E_9D25,
    0x2F22_557A_51BF_8C74, //  -72
    0x73E9_A632_54E4_2EA2,
    0x1836_EF2A_1C65_AD86, //  -71
    0x5CBA_EB5B_771C_F21B,
    0x2CF8_BF54_E384_8AD2, //  -70
    0x4A2F_22AF_927D_8E7C,
    0x23FA_32AA_4F9D_3BDB, //  -69
    0x76B1_D118_EA62_7D93,
    0x5329_EAAA_18FB_92F8, //  -68
    0x5EF4_A747_21E8_6476,
    0x0F54_BBBB_472F_A8C6, //  -67
    0x4BF6_EC38_E7ED_1D2B,
    0x25DD_62FC_38F2_ED6C, //  -66
    0x798B_138E_3FE1_C845,
    0x22FB_D193_8E51_7BDF, //  -65
    0x613C_0FA4_FFE7_D36A,
    0x4F2F_DADC_71DA_C97F, //  -64
    0x4DC9_A61D_9986_42BB,
    0x58F3_157D_27E2_3ACC, //  -63
    0x7C75_D695_C270_6AC5,
    0x74B8_2261_D969_F7AD, //  -62
    0x6391_7877_CEC0_556B,
    0x1093_4EB4_ADEE_5FBE, //  -61
    0x4FA7_9393_0BCD_1122,
    0x4075_D890_8B25_1965, //  -60
    0x7F72_85B8_12E1_B504,
    0x00BC_8DB4_11D4_F56E, //  -59
    0x65F5_37C6_7581_5D9C,
    0x66FD_3E29_A7DD_9125, //  -58
    0x5190_F96B_9134_4AE3,
    0x6BFD_CB54_864A_DA84, //  -57
    0x4140_C789_40F6_A24F,
    0x6FFE_3C43_9EA2_486A, //  -56
    0x6867_A5A8_67F1_03B2,
    0x7FFD_2D38_FDD0_73DC, //  -55
    0x5386_1E20_5327_3628,
    0x6664_242D_97D9_F64A, //  -54
    0x42D1_B1B3_75B8_F820,
    0x51E9_B68A_DFE1_91D5, //  -53
    0x6AE9_1C52_55F4_C034,
    0x1CA9_2411_6635_B621, //  -52
    0x5587_49DB_77F7_0029,
    0x63BA_8341_1E91_5E81, //  -51
    0x446C_3B15_F992_6687,
    0x6962_029A_7EDA_B201, //  -50
    0x6D79_F823_28EA_3DA6,
    0x0F03_375D_97C4_5001, //  -49
    0x5794_C682_8721_CAEB,
    0x259C_2C4A_DFD0_4001, //  -48
    0x4610_9ECE_D281_6F22,
    0x5149_BD08_B30D_0001, //  -47
    0x701A_97B1_50CF_1837,
    0x3542_C80D_EB48_0001, //  -46
    0x59AE_DFC1_0D72_79C5,
    0x7768_A00B_22A0_0001, //  -45
    0x47BF_1967_3DF5_2E37,
    0x7920_8008_E880_0001, //  -44
    0x72CB_5BD8_6321_E38C,
    0x5B67_3341_7400_0001, //  -43
    0x5BD5_E313_8281_82D6,
    0x7C52_8F67_9000_0001, //  -42
    0x4977_E8DC_6867_9BDF,
    0x16A8_72B9_4000_0001, //  -41
    0x758C_A7C7_0D72_92FE,
    0x5773_EAC2_0000_0001, //  -40
    0x5E0A_1FD2_7128_7598,
    0x45F6_5568_0000_0001, //  -39
    0x4B3B_4CA8_5A86_C47A,
    0x04C5_1120_0000_0001, //  -38
    0x785E_E10D_5DA4_6D90,
    0x07A1_B500_0000_0001, //  -37
    0x604B_E73D_E483_8AD9,
    0x52E7_C400_0000_0001, //  -36
    0x4D09_85CB_1D36_08AE,
    0x0F1F_D000_0000_0001, //  -35
    0x7B42_6FAB_61F0_0DE3,
    0x31CC_8000_0000_0001, //  -34
    0x629B_8C89_1B26_7182,
    0x5B0A_0000_0000_0001, //  -33
    0x4EE2_D6D4_15B8_5ACE,
    0x7C08_0000_0000_0001, //  -32
    0x7E37_BE20_22C0_914B,
    0x1340_0000_0000_0001, //  -31
    0x64F9_64E6_8233_A76F,
    0x2900_0000_0000_0001, //  -30
    0x50C7_83EB_9B5C_85F2,
    0x5400_0000_0000_0001, //  -29
    0x409F_9CBC_7C4A_04C2,
    0x1000_0000_0000_0001, //  -28
    0x6765_C793_FA10_079D,
    0x0000_0000_0000_0001, //  -27
    0x52B7_D2DC_C80C_D2E4,
    0x0000_0000_0000_0001, //  -26
    0x422C_A8B0_A00A_4250,
    0x0000_0000_0000_0001, //  -25
    0x69E1_0DE7_6676_D080,
    0x0000_0000_0000_0001, //  -24
    0x54B4_0B1F_852B_DA00,
    0x0000_0000_0000_0001, //  -23
    0x43C3_3C19_3756_4800,
    0x0000_0000_0000_0001, //  -22
    0x6C6B_935B_8BBD_4000,
    0x0000_0000_0000_0001, //  -21
    0x56BC_75E2_D631_0000,
    0x0000_0000_0000_0001, //  -20
    0x4563_9182_44F4_0000,
    0x0000_0000_0000_0001, //  -19
    0x6F05_B59D_3B20_0000,
    0x0000_0000_0000_0001, //  -18
    0x58D1_5E17_6280_0000,
    0x0000_0000_0000_0001, //  -17
    0x470D_E4DF_8200_0000,
    0x0000_0000_0000_0001, //  -16
    0x71AF_D498_D000_0000,
    0x0000_0000_0000_0001, //  -15
    0x5AF3_107A_4000_0000,
    0x0000_0000_0000_0001, //  -14
    0x48C2_7395_0000_0000,
    0x0000_0000_0000_0001, //  -13
    0x746A_5288_0000_0000,
    0x0000_0000_0000_0001, //  -12
    0x5D21_DBA0_0000_0000,
    0x0000_0000_0000_0001, //  -11
    0x4A81_7C80_0000_0000,
    0x0000_0000_0000_0001, //  -10
    0x7735_9400_0000_0000,
    0x0000_0000_0000_0001, //   -9
    0x5F5E_1000_0000_0000,
    0x0000_0000_0000_0001, //   -8
    0x4C4B_4000_0000_0000,
    0x0000_0000_0000_0001, //   -7
    0x7A12_0000_0000_0000,
    0x0000_0000_0000_0001, //   -6
    0x61A8_0000_0000_0000,
    0x0000_0000_0000_0001, //   -5
    0x4E20_0000_0000_0000,
    0x0000_0000_0000_0001, //   -4
    0x7D00_0000_0000_0000,
    0x0000_0000_0000_0001, //   -3
    0x6400_0000_0000_0000,
    0x0000_0000_0000_0001, //   -2
    0x5000_0000_0000_0000,
    0x0000_0000_0000_0001, //   -1
    0x4000_0000_0000_0000,
    0x0000_0000_0000_0001, //    0
    0x6666_6666_6666_6666,
    0x3333_3333_3333_3334, //    1
    0x51EB_851E_B851_EB85,
    0x0F5C_28F5_C28F_5C29, //    2
    0x4189_374B_C6A7_EF9D,
    0x5916_872B_020C_49BB, //    3
    0x68DB_8BAC_710C_B295,
    0x74F0_D844_D013_A92B, //    4
    0x53E2_D623_8DA3_C211,
    0x43F3_E037_0CDC_8755, //    5
    0x431B_DE82_D7B6_34DA,
    0x698F_E692_70B0_6C44, //    6
    0x6B5F_CA6A_F2BD_215E,
    0x0F4C_A41D_811A_46D4, //    7
    0x55E6_3B88_C230_E77E,
    0x3F70_834A_CDAE_9F10, //    8
    0x44B8_2FA0_9B5A_52CB,
    0x4C5A_02A2_3E25_4C0D, //    9
    0x6DF3_7F67_5EF6_EADF,
    0x2D5C_D103_96A2_1347, //   10
    0x57F5_FF85_E592_557F,
    0x3DE3_DA69_454E_75D3, //   11
    0x465E_6604_B7A8_4465,
    0x7E4F_E1ED_D10B_9175, //   12
    0x7097_09A1_25DA_0709,
    0x4A19_697C_81AC_1BEF, //   13
    0x5A12_6E1A_84AE_6C07,
    0x54E1_2130_67BC_E326, //   14
    0x480E_BE7B_9D58_566C,
    0x43E7_4DC0_52FD_8285, //   15
    0x734A_CA5F_6226_F0AD,
    0x530B_AF9A_1E62_6A6D, //   16
    0x5C3B_D519_1B52_5A24,
    0x426F_BFAE_7EB5_21F1, //   17
    0x49C9_7747_490E_AE83,
    0x4EBF_CC8B_9890_E7F4, //   18
    0x760F_253E_DB4A_B0D2,
    0x4ACC_7A78_F41B_0CBA, //   19
    0x5E72_8432_4908_8D75,
    0x223D_2EC7_29AF_3D62, //   20
    0x4B8E_D028_3A6D_3DF7,
    0x34FD_BF05_BAF2_9781, //   21
    0x78E4_8040_5D7B_9658,
    0x54C9_31A2_C4B7_58CF, //   22
    0x60B6_CD00_4AC9_4513,
    0x5D6D_C14F_03C5_E0A5, //   23
    0x4D5F_0A66_A23A_9DA9,
    0x3124_9AA5_9C9E_4D51, //   24
    0x7BCB_43D7_69F7_62A8,
    0x4EA0_F76F_60FD_4882, //   25
    0x6309_0312_BB2C_4EED,
    0x254D_92BF_80CA_A068, //   26
    0x4F3A_68DB_C8F0_3F24,
    0x1DD7_A899_33D5_4D20, //   27
    0x7EC3_DAF9_4180_6506,
    0x62F2_A75B_8622_1500, //   28
    0x6569_7BFA_9ACD_1D9F,
    0x025B_B916_04E8_10CD, //   29
    0x5121_2FFB_AF0A_7E18,
    0x6849_60DE_6A53_40A4, //   30
    0x40E7_5996_25A1_FE7A,
    0x203A_B3E5_21DC_33B6, //   31
    0x67D8_8F56_A29C_CA5D,
    0x19F7_863B_6960_52BD, //   32
    0x5313_A5DE_E87D_6EB0,
    0x7B2C_6B62_BAB3_7564, //   33
    0x4276_1E4B_ED31_255A,
    0x2F56_BC4E_FBC2_C450, //   34
    0x6A56_96DF_E1E8_3BC3,
    0x6557_93B1_92D1_3A1A, //   35
    0x5512_124C_B4B9_C969,
    0x3779_42F4_7574_2E7B, //   36
    0x440E_750A_2A2E_3ABA,
    0x5F94_3590_5DF6_8B96, //   37
    0x6CE3_EE76_A9E3_912A,
    0x65B9_EF4D_6324_1289, //   38
    0x571C_BEC5_54B6_0DBB,
    0x6AFB_25D7_8283_4207, //   39
    0x45B0_989D_DD5E_7163,
    0x08C8_EB12_CECF_6806, //   40
    0x6F80_F42F_C897_1BD1,
    0x5ADB_11B7_B14B_D9A3, //   41
    0x5933_F68C_A078_E30E,
    0x157C_0E2C_8DD6_47B5, //   42
    0x475C_C53D_4D2D_8271,
    0x5DFC_D823_A4AB_6C91, //   43
    0x722E_0862_1515_9D82,
    0x632E_269F_6DDF_141B, //   44
    0x5B58_06B4_DDAA_E468,
    0x4F58_1EE5_F17F_4349, //   45
    0x4913_3890_B155_8386,
    0x72AC_E584_C132_9C3B, //   46
    0x74EB_8DB4_4EEF_38D7,
    0x6AAE_3C07_9B84_2D2A, //   47
    0x5D89_3E29_D8BF_60AC,
    0x5558_3006_1603_5755, //   48
    0x4AD4_31BB_13CC_4D56,
    0x7779_C004_DE69_12AB, //   49
    0x77B9_E92B_52E0_7BBE,
    0x258F_99A1_63DB_5111, //   50
    0x5FC7_EDBC_424D_2FCB,
    0x37A6_1481_1CAF_740D, //   51
    0x4C9F_F163_683D_BFD5,
    0x7951_AA00_E3BF_900B, //   52
    0x7A99_8238_A6C9_32EF,
    0x754F_7667_D2CC_19AB, //   53
    0x6214_682D_523A_8F26,
    0x2AA5_F853_0F09_AE22, //   54
    0x4E76_B9BD_DB62_0C1E,
    0x5551_9375_A5A1_581B, //   55
    0x7D8A_C2C9_5F03_4697,
    0x3BB5_B8BC_3C35_59C5, //   56
    0x646F_023A_B269_0545,
    0x7C91_6096_9691_149E, //   57
    0x5058_CE95_5B87_376B,
    0x16DA_B3AB_ABA7_43B2, //   58
    0x4047_0BAA_AF9F_5F88,
    0x78AE_F622_EFB9_02F5, //   59
    0x66D8_12AA_B298_98DB,
    0x0DE4_BD04_B2C1_9E54, //   60
    0x5246_7555_5BAD_4715,
    0x57EA_30D0_8F01_4B76, //   61
    0x41D1_F777_7C8A_9F44,
    0x4654_F3DA_0C01_092C, //   62
    0x694F_F258_C744_3207,
    0x23BB_1FC3_4668_0EAC, //   63
    0x543F_F513_D29C_F4D2,
    0x4FC8_E635_D1EC_D88A, //   64
    0x4366_5DA9_754A_5D75,
    0x263A_51C4_A7F0_AD3B, //   65
    0x6BD6_FC42_5543_C8BB,
    0x56C3_B607_731A_AEC4, //   66
    0x5645_969B_7769_6D62,
    0x789C_919F_8F48_8BD0, //   67
    0x4504_787C_5F87_8AB5,
    0x46E3_A7B2_D906_D640, //   68
    0x6E6D_8D93_CC0C_1122,
    0x3E39_0C51_5B3E_239A, //   69
    0x5857_A476_3CD6_741B,
    0x4B60_D6A7_7C31_B615, //   70
    0x46AC_8391_CA45_29AF,
    0x55E7_121F_968E_2B44, //   71
    0x7114_05B6_106E_A919,
    0x0971_B698_F0E3_786D, //   72
    0x5A76_6AF8_0D25_5414,
    0x078E_2BAD_8D82_C6BD, //   73
    0x485E_BBF9_A41D_DCDC,
    0x6C71_BC8A_D79B_D231, //   74
    0x73CA_C65C_39C9_6161,
    0x2D82_C744_8C2C_8382, //   75
    0x5CA2_3849_C7D4_4DE7,
    0x3E02_3903_A356_CF9B, //   76
    0x4A1B_603B_0643_7185,
    0x7E68_2D9C_82AB_D949, //   77
    0x7692_3391_A39F_1C09,
    0x4A40_48FA_6AAC_8EDB, //   78
    0x5EDB_5C74_82E5_B007,
    0x5500_3A61_EEF0_7249, //   79
    0x4BE2_B05D_3584_8CD2,
    0x7733_61E7_F259_F507, //   80
    0x796A_B3C8_55A0_E151,
    0x3EB8_9CA6_508F_EE71, //   81
    0x6122_296D_114D_810D,
    0x7EFA_16EB_73A6_585B, //   82
    0x4DB4_EDF0_DAA4_673E,
    0x3261_ABEF_8FB8_46AF, //   83
    0x7C54_AFE7_C43A_3ECA,
    0x1D69_1318_E5F3_A44B, //   84
    0x6376_F31F_D02E_98A1,
    0x6454_0F47_1E5C_836F, //   85
    0x4F92_5C19_7358_7A1B,
    0x0376_729F_4B7D_35F3, //   86
    0x7F50_935B_EBC0_C35E,
    0x38BD_8432_1261_EFEB, //   87
    0x65DA_0F7C_BC9A_35E5,
    0x13CA_D028_0EB4_BFEF, //   88
    0x517B_3F96_FD48_2B1D,
    0x5CA2_4020_0BC3_CCBF, //   89
    0x412F_6612_6439_BC17,
    0x63B5_0019_A303_0A33, //   90
    0x684B_D683_D38F_9359,
    0x1F88_0029_04D1_A9EA, //   91
    0x536F_DECF_DC72_DC47,
    0x32D3_3354_03DA_EE55, //   92
    0x42BF_E573_16C2_49D2,
    0x5BDC_2910_0315_8B77, //   93
    0x6ACC_A251_BE03_A951,
    0x12F9_DB4C_D1BC_1258, //   94
    0x5570_81DA_FE69_5440,
    0x7594_AF70_A7C9_A847, //   95
    0x445A_017B_FEBA_A9CD,
    0x4476_F2C0_863A_ED06, //   96
    0x6D5C_CF2C_CAC4_42E2,
    0x3A57_EACD_A391_7B3C, //   97
    0x577D_728A_3BD0_3581,
    0x7B79_88A4_82DA_C8FD, //   98
    0x45FD_F53B_630C_F79B,
    0x15FA_D3B6_CF15_6D97, //   99
    0x6FFC_BB92_3814_BF5E,
    0x565E_1F8A_E4EF_15BE, //  100
    0x5996_FC74_F9AA_32B2,
    0x11E4_E608_B725_AAFF, //  101
    0x47AB_FD2A_6154_F55B,
    0x27EA_51A0_9284_88CC, //  102
    0x72AC_C843_CEEE_555E,
    0x7310_829A_8407_4146, //  103
    0x5BBD_6D03_0BF1_DDE5,
    0x4273_9BAE_D005_CDD2, //  104
    0x4964_5735_A327_E4B7,
    0x4EC2_E2F2_4004_A4A8, //  105
    0x756D_5855_D1D9_6DF2,
    0x4AD1_6B1D_333A_A10C, //  106
    0x5DF1_1377_DB14_57F5,
    0x2241_227D_C295_4DA3, //  107
    0x4B27_42C6_48DD_132A,
    0x4E9A_81FE_3544_3E1C, //  108
    0x783E_D13D_4161_B844,
    0x175D_9CC9_EED3_9694, //  109
    0x6032_40FD_CDE7_C69C,
    0x7917_B0A1_8BDC_7876, //  110
    0x4CF5_00CB_0B1F_D217,
    0x1412_F3B4_6FE3_9392, //  111
    0x7B21_9ADE_7832_E9BE,
    0x5351_85ED_7FD2_85B6, //  112
    0x6281_48B1_F9C2_5498,
    0x42A7_9E57_9975_37C5, //  113
    0x4ECD_D3C1_949B_76E0,
    0x3552_E512_E12A_9304, //  114
    0x7E16_1F9C_20F8_BE33,
    0x6EEB_081E_3510_EB39, //  115
    0x64DE_7FB0_1A60_9829,
    0x3F22_6CE4_F740_BC2E, //  116
    0x50B1_FFC0_151A_1354,
    0x3281_F0B7_2C33_C9BE, //  117
    0x408E_6633_4414_DC43,
    0x4201_8D5F_568F_D498, //  118
    0x674A_3D1E_D354_939F,
    0x1CCF_4898_8A7F_BA8D, //  119
    0x52A1_CA7F_0F76_DC7F,
    0x30A5_D3AD_3B99_620B, //  120
    0x421B_0865_A5F8_B065,
    0x73B7_DC8A_9614_4E6F, //  121
    0x69C4_DA3C_3CC1_1A3C,
    0x52BF_C744_2353_B0B1, //  122
    0x549D_7B63_63CD_AE96,
    0x7566_3903_4F76_26F4, //  123
    0x43B1_2F82_B63E_2545,
    0x4451_C735_D92B_525D, //  124
    0x6C4E_B26A_BD30_3BA2,
    0x3A1C_71EF_C1DE_EA2E, //  125
    0x56A5_5B88_9759_C94E,
    0x61B0_5B26_34B2_54F2, //  126
    0x4551_1606_DF7B_0772,
    0x1AF3_7C1E_908E_AA5B, //  127
    0x6EE8_233E_325E_7250,
    0x2B1F_2CFD_B417_76F8, //  128
    0x58B9_B5CB_5B7E_C1D9,
    0x6F4C_23FE_29AC_5F2D, //  129
    0x46FA_F7D5_E2CB_CE47,
    0x72A3_4FFE_87BD_18F1, //  130
    0x7191_8C89_6ADF_B073,
    0x0438_7FFD_A5FB_5B1B, //  131
    0x5ADA_D6D4_557F_C05C,
    0x0360_6664_84C9_15AF, //  132
    0x48AF_1243_7799_66B0,
    0x02B3_851D_3707_448C, //  133
    0x744B_506B_F28F_0AB3,
    0x1DEC_082E_BE72_0746, //  134
    0x5D09_0D23_2872_6EF5,
    0x64BC_D358_985B_3905, //  135
    0x4A6D_A41C_205B_8BF7,
    0x6A30_A913_AD15_C738, //  136
    0x7715_D360_33C5_ACBF,
    0x5D1A_A81F_7B56_0B8C, //  137
    0x5F44_A919_C304_8A32,
    0x7DAE_ECE5_FC44_D609, //  138
    0x4C36_EDAE_359D_3B5B,
    0x7E25_8A51_969D_7808, //  139
    0x79F1_7C49_EF61_F893,
    0x16A2_76E8_F0FB_F33F, //  140
    0x618D_FD07_F2B4_C6DC,
    0x121B_9253_F3FC_C299, //  141
    0x4E0B_30D3_2890_9F16,
    0x41AF_A843_2997_0214, //  142
    0x7CDE_B485_0DB4_31BD,
    0x4F7F_739E_A8F1_9CED, //  143
    0x63E5_5D37_3E29_C164,
    0x3F99_294B_BA5A_E3F1, //  144
    0x4FEA_B0F8_FE87_CDE9,
    0x7FAD_BAA2_FB7B_E98D, //  145
    0x7FDD_E7F4_CA72_E30F,
    0x7F7C_5DD1_925F_DC15, //  146
    0x664B_1FF7_085B_E8D9,
    0x4C63_7E41_41E6_49AB, //  147
    0x51D5_B32C_06AF_ED7A,
    0x704F_9834_34B8_3AEF, //  148
    0x4177_C289_9EF3_2462,
    0x26A6_135C_F6F9_C8BF, //  149
    0x68BF_9DA8_FE51_D3D0,
    0x3DD6_8561_8B29_4132, //  150
    0x53CC_7E20_CB74_A973,
    0x4B12_044E_08ED_CDC2, //  151
    0x4309_FE80_A2C3_BAC2,
    0x6F41_9D0B_3A57_D7CE, //  152
    0x6B43_30CD_D139_2AD1,
    0x3202_94DE_C3BF_BFB0, //  153
    0x55CF_5A3E_40FA_88A7,
    0x419B_AA4B_CFCC_995A, //  154
    0x44A5_E1CB_672E_D3B9,
    0x1AE2_EEA3_0CA3_ADE1, //  155
    0x6DD6_3612_3EB1_52C1,
    0x77D1_7DD1_ADD2_AFCF, //  156
    0x57DE_91A8_3227_7567,
    0x7974_64A7_BE42_263F, //  157
    0x464B_A7B9_C1B9_2AB9,
    0x4790_5086_31CE_84FF, //  158
    0x7079_0C5C_6928_445C,
    0x0C1A_1A70_4FB0_D4CC, //  159
    0x59FA_7049_EDB9_D049,
    0x567B_4859_D95A_43D6, //  160
    0x47FB_8D07_F161_736E,
    0x11FC_39E1_7AAE_9CAB, //  161
    0x732C_14D9_8235_857D,
    0x032D_2968_C44A_9445, //  162
    0x5C23_43E1_34F7_9DFD,
    0x4F57_5453_D03B_A9D1, //  163
    0x49B5_CFE7_5D92_E4CA,
    0x72AC_4376_402F_BB0E, //  164
    0x75EF_B30B_C8EB_07AB,
    0x0446_D256_CD19_2B49, //  165
    0x5E59_5C09_6D88_D2EF,
    0x1D05_7512_3DAD_BC3A, //  166
    0x4B7A_B007_8AD3_DBF2,
    0x4A6A_C40E_97BE_302F, //  167
    0x78C4_4CD8_DE1F_C650,
    0x7711_39B0_F2C9_E6B1, //  168
    0x609D_0A47_1819_6B73,
    0x78DA_948D_8F07_EBC1, //  169
    0x4D4A_6E9F_467A_BC5C,
    0x60AE_DD3E_0C06_5634, //  170
    0x7BAA_4A98_70C4_6094,
    0x344A_FB96_79A3_BD20, //  171
    0x62EE_A213_8D69_E6DD,
    0x103B_FC78_614F_CA80, //  172
    0x4F25_4E76_0ABB_1F17,
    0x2696_6393_810C_A200, //  173
    0x7EA2_1723_445E_9825,
    0x2423_D285_9B47_6999, //  174
    0x654E_78E9_037E_E01D,
    0x69B6_4204_7C39_2148, //  175
    0x510B_93ED_9C65_8017,
    0x6E2B_6803_9694_1AA0, //  176
    0x40D6_0FF1_49EA_CCDF,
    0x71BC_5336_1210_154D, //  177
    0x67BC_E64E_DCAA_E166,
    0x1C60_8523_5019_BBAE, //  178
    0x52FD_850B_E3BB_E784,
    0x7D1A_041C_4014_9625, //  179
    0x4264_6A6F_E963_1F9D,
    0x4A7B_367D_0010_781D, //  180
    0x6A3A_43E6_4238_3295,
    0x5D91_F0C8_001A_59C8, //  181
    0x54FB_6985_01C6_8EDE,
    0x17A7_F3D3_3348_47D4, //  182
    0x43FC_546A_67D2_0BE4,
    0x7953_2975_C2A0_3976, //  183
    0x6CC6_ED77_0C83_463B,
    0x0EEB_7589_3766_C256, //  184
    0x5705_8AC5_A39C_382F,
    0x2589_2AD4_2C52_3512, //  185
    0x459E_089E_1C7C_F9BF,
    0x37A0_EF10_2374_F742, //  186
    0x6F63_40FC_FA61_8F98,
    0x5901_7E80_38BB_2536, //  187
    0x591C_33FD_951A_D946,
    0x7A67_9866_93C8_EA91, //  188
    0x4749_C331_4415_7A9F,
    0x151F_AD1E_DCA0_BBA8, //  189
    0x720F_9EB5_39BB_F765,
    0x0832_AE97_C767_92A5, //  190
    0x5B3F_B22A_9496_5F84,
    0x068E_F213_05EC_7551, //  191
    0x48FF_C1BB_AA11_E603,
    0x1ED8_C1A8_D189_F774, //  192
    0x74CC_692C_434F_D66B,
    0x4AF4_690E_1C0F_F253, //  193
    0x5D70_5423_690C_AB89,
    0x225D_20D8_1673_2843, //  194
    0x4AC0_434F_873D_5607,
    0x3517_4D79_AB8F_5369, //  195
    0x779A_054C_0B95_5672,
    0x21BE_E25C_45B2_1F0E, //  196
    0x5FAE_6AA3_3C77_785B,
    0x3498_B516_9E28_18D8, //  197
    0x4C8B_8882_96C5_F9E2,
    0x5D46_F745_4B53_4713, //  198
    0x7A78_DA6A_8AD6_5C9D,
    0x7BA4_BED5_4552_0B52, //  199
    0x61FA_4855_3BDE_B07E,
    0x2FB6_FF11_0441_A2A8, //  200
    0x4E61_D377_6318_8D31,
    0x72F8_CC0D_9D01_4EED, //  201
    0x7D69_5258_9E8D_AEB6,
    0x1E5A_E015_C802_17E1, //  202
    0x6454_41E0_7ED7_BEF8,
    0x1848_B344_A001_ACB4, //  203
    0x5043_67E6_CBDF_CBF9,
    0x603A_2903_B334_8A2A, //  204
    0x4035_ECB8_A319_6FFB,
    0x002E_8736_28F6_D4EE, //  205
    0x66BC_ADF4_3828_B32B,
    0x19E4_0B89_DB24_87E3, //  206
    0x5230_8B29_C686_F5BC,
    0x14B6_6FA1_7C1D_3983, //  207
    0x41C0_6F54_9ED2_5E30,
    0x1091_F2E7_967D_C79C, //  208
    0x6933_E554_3150_96B3,
    0x341C_B7D8_F0C9_3F5F, //  209
    0x5429_8443_5AA6_DEF5,
    0x767D_5FE0_C0A0_FF80, //  210
    0x4354_69CF_7BB8_B25E,
    0x2B97_7FE7_0080_CC66, //  211
    0x6BBA_42E5_92C1_1D63,
    0x5F58_CCA4_CD9A_E0A3, //  212
    0x562E_9BEA_DBCD_B11C,
    0x4C47_0A1D_7148_B3B6, //  213
    0x44F2_1655_7CA4_8DB0,
    0x3D05_A1B1_276D_5C92, //  214
    0x6E50_23BB_FAA0_E2B3,
    0x7B3C_35E8_3F15_60E9, //  215
    0x5840_1C96_621A_4EF6,
    0x2F63_5E53_65AA_B3ED, //  216
    0x4699_B078_4E7B_725E,
    0x591C_4B75_EAEE_F658, //  217
    0x70F5_E726_E3F8_B6FD,
    0x74FA_1256_44B1_8A26, //  218
    0x5A5E_5285_832D_5F31,
    0x43FB_41DE_9D5A_D4EB, //  219
    0x484B_7537_9C24_4C27,
    0x4FFC_34B2_177B_DD89, //  220
    0x73AB_EEBF_603A_1372,
    0x4CC6_BAB6_8BF9_6274, //  221
    0x5C89_8BCC_4CFB_42C2,
    0x0A38_955E_D661_1B90, //  222
    0x4A07_A309_D72F_689B,
    0x21C6_DDE5_784D_AFA7, //  223
    0x7672_9E76_2518_A75E,
    0x693E_2FD5_8D49_190B, //  224
    0x5EC2_185E_8413_B918,
    0x5431_BFDE_0AA0_E0D5, //  225
    0x4BCE_79E5_3676_2DAD,
    0x29C1_664B_3BB3_E711, //  226
    0x794A_5CA1_F0BD_15E2,
    0x0F9B_D6DE_C5EC_A4E8, //  227
    0x6108_4A1B_26FD_AB1B,
    0x2616_457F_04BD_50BA, //  228
    0x4DA0_3B48_EBFE_227C,
    0x1E78_3798_D097_73C8, //  229
    0x7C33_920E_4663_6A60,
    0x30C0_58F4_80F2_52D9, //  230
    0x635C_74D8_384F_884D,
    0x0D66_AD90_6728_4247, //  231
    0x4F7D_2A46_9372_D370,
    0x711E_F140_5286_9B6C, //  232
    0x7F2E_AA0A_8584_8581,
    0x34FE_4ECD_50D7_5F14, //  233
    0x65BE_EE6E_D136_D134,
    0x2A65_0BD7_73DF_7F43, //  234
    0x5165_8B8B_DA92_40F6,
    0x551D_A312_C319_329C, //  235
    0x411E_093C_AEDB_672B,
    0x5DB1_4F42_35AD_C217, //  236
    0x6830_0EC7_7E2B_D845,
    0x7C4E_E536_BC49_368A, //  237
    0x5359_A56C_64EF_E037,
    0x7D0B_EA92_303A_9208, //  238
    0x42AE_1DF0_50BF_E693,
    0x173C_BBA8_2695_41A0, //  239
    0x6AB0_2FE6_E799_70EB,
    0x3EC7_92A6_A422_029A, //  240
    0x5559_BFEB_EC7A_C0BC,
    0x3239_421E_E9B4_CEE1, //  241
    0x4447_CCBC_BD2F_0096,
    0x5B61_01B2_5490_A581, //  242
    0x6D3F_ADFA_C84B_3424,
    0x2BCE_691D_541A_A268, //  243
    0x5766_24C8_A03C_29B6,
    0x563E_BA7D_DCE2_1B87, //  244
    0x45EB_50A0_8030_215E,
    0x7832_2ECB_171B_4939, //  245
    0x6FDE_E767_3380_3564,
    0x59E9_E478_24F8_7527, //  246
    0x597F_1F85_C2CC_F783,
    0x6187_E9F9_B72D_2A86, //  247
    0x4798_E604_9BD7_2C69,
    0x346C_BB2E_2C24_2205, //  248
    0x728E_3CD4_2C8B_7A42,
    0x20AD_F849_E039_D007, //  249
    0x5BA4_FD76_8A09_2E9B,
    0x33BE_603B_19C7_D99F, //  250
    0x4950_CAC5_3B3A_8BAF,
    0x42FE_B362_7B06_47B3, //  251
    0x754E_113B_91F7_45E5,
    0x5197_856A_5E70_72B8, //  252
    0x5DD8_0DC9_4192_9E51,
    0x27AC_6ABB_7EC0_5BC6, //  253
    0x4B13_3E3A_9ADB_B1DA,
    0x52F0_5562_CBCD_1638, //  254
    0x781E_C9F7_5E2C_4FC4,
    0x1E4D_556A_DFAE_89F3, //  255
    0x6018_A192_B1BD_0C9C,
    0x7EA4_4455_7FBE_D4C3, //  256
    0x4CE0_8142_27CA_707D,
    0x4BB6_9D11_32FF_109C, //  257
    0x7B00_CED0_3FAA_4D95,
    0x5F8A_94E8_5198_1A93, //  258
    0x6267_0BD9_CC88_3E11,
    0x32D5_43ED_0E13_4875, //  259
    0x4EB8_D647_D6D3_64DA,
    0x5BDD_CFF0_D80F_6D2B, //  260
    0x7DF4_8A0C_8AEB_D491,
    0x12FC_7FE7_C018_AEAB, //  261
    0x64C3_A1A3_A256_43A7,
    0x28C9_FFEC_99AD_5889, //  262
    0x509C_814F_B511_CFB9,
    0x0707_FFF0_7AF1_13A1, //  263
    0x407D_343F_C40E_3FC7,
    0x1F39_998D_2F27_42E7, //  264
    0x672E_B9FF_A016_CC71,
    0x7EC2_8F48_4B72_04A4, //  265
    0x528B_C7FF_B345_705B,
    0x189B_A5D3_6F8E_6A1D, //  266
    0x4209_6CCC_8F6A_C048,
    0x7A16_1E42_BFA5_21B1, //  267
    0x69A8_AE14_18AA_CD41,
    0x4356_96D1_32A1_CF81, //  268
    0x5486_F1A9_AD55_7101,
    0x1C45_4574_2881_72CE, //  269
    0x439F_27BA_F111_2734,
    0x169D_D129_BA01_28A5, //  270
    0x6C31_D92B_1B4E_A520,
    0x242F_B50F_9001_DAA1, //  271
    0x568E_4755_AF72_1DB3,
    0x368C_90D9_4001_7BB4, //  272
    0x453E_9F77_BF8E_7E29,
    0x120A_0D7A_999A_C95D, //  273
    0x6ECA_98BF_98E3_FD0E,
    0x5010_1590_F5C4_7561, //  274
    0x58A2_13CC_7A4F_FDA5,
    0x2673_4473_F7D0_5DE8, //  275
    0x46E8_0FD6_C83F_FE1D,
    0x6B8F_69F6_5FD9_E4B9, //  276
    0x7173_4C8A_D9FF_FCFC,
    0x45B2_4323_CC8F_D45C, //  277
    0x5AC2_A3A2_47FF_FD96,
    0x6AF5_0283_0A0C_A9E3, //  278
    0x489B_B61B_6CCC_CADF,
    0x08C4_0202_6E70_87E9, //  279
    0x742C_5692_47AE_1164,
    0x746C_D003_E3E7_3FDB, //  280
    0x5CF0_4541_D2F1_A783,
    0x76BD_7336_4FEC_3315, //  281
    0x4A59_D101_758E_1F9C,
    0x5EFD_F5C5_0CBC_F5AB, //  282
    0x76F6_1B35_88E3_65C7,
    0x4B2F_EFA1_ADFB_22AB, //  283
    0x5F2B_48F7_A0B5_EB06,
    0x08F3_261A_F195_B555, //  284
    0x4C22_A0C6_1A2B_226B,
    0x20C2_84E2_5ADE_2AAB, //  285
    0x79D1_013C_F6AB_6A45,
    0x1AD0_D49D_5E30_4444, //  286
    0x6174_00FD_9222_BB6A,
    0x48A7_107D_E4F3_69D0, //  287
    0x4DF6_6731_41B5_62BB,
    0x53B8_D9FE_50C2_BB0D, //  288
    0x7CBD_71E8_6922_3792,
    0x52C1_5CCA_1AD1_2B48, //  289
    0x63CA_C186_BA81_C60E,
    0x7567_7D6E_7BDA_8906, //  290
    0x4FD5_679E_FB9B_04D8,
    0x5DEC_6458_6315_3A6C, //  291
    0x7FBB_D8FE_5F5E_6E27,
    0x497A_3A27_04EE_C3DF, //  292
];
