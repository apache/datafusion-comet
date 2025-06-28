/// Similar to the `read_num_bytes` but read nums from bytes in big-endian order
/// This is used to read bytes from Java's OutputStream which writes bytes in big-endian
/// Reads `$size` of bytes from `$src`, and reinterprets them as type `$ty`, in
/// big-endian order.
/// This is copied and modified datafusion_comet::common::bit.
macro_rules! read_num_be_bytes {
    ($ty:ty, $size:expr, $src:expr) => {{
        debug_assert!($size <= $src.len());
        let mut buffer = [0u8; std::mem::size_of::<$ty>()];
        buffer.as_mut()[..$size].copy_from_slice(&$src[..$size]);
        <$ty>::from_be_bytes(buffer)
    }};
}
