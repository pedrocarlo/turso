use core::{fmt, str};

mod field;
mod macros;

mod sealed {
    pub trait Sealed {}
}

/// Implementation detail used for constructing FieldSet names from raw
/// identifiers. In `info!(..., r#type = "...")` the macro would end up
/// constructing a name equivalent to `FieldName(*b"type")`.
pub struct FieldName<const N: usize>([u8; N]);

impl<const N: usize> FieldName<N> {
    /// Convert `"prefix.r#keyword.suffix"` to `b"prefix.keyword.suffix"`.
    pub const fn new(input: &str) -> Self {
        let input = input.as_bytes();
        let mut output = [0u8; N];
        let mut read = 0;
        let mut write = 0;
        while read < input.len() {
            if read + 1 < input.len() && input[read] == b'r' && input[read + 1] == b'#' {
                read += 2;
            }
            output[write] = input[read];
            read += 1;
            write += 1;
        }
        assert!(write == N);
        Self(output)
    }

    pub const fn as_str(&self) -> &str {
        // SAFETY: Because of the private visibility of self.0, it must have
        // been computed by Self::new. So these bytes are all of the bytes
        // of some original valid UTF-8 string, but with "r#" substrings
        // removed, which cannot have produced invalid UTF-8.
        unsafe { str::from_utf8_unchecked(self.0.as_slice()) }
    }
}

impl FieldName<0> {
    /// For `"prefix.r#keyword.suffix"` compute `"prefix.keyword.suffix".len()`.
    pub const fn len(input: &str) -> usize {
        // Count occurrences of "r#"
        let mut raw = 0;

        let mut i = 0;
        while i < input.len() {
            if input.as_bytes()[i] == b'#' {
                raw += 1;
            }
            i += 1;
        }

        input.len() - 2 * raw
    }
}

impl<const N: usize> fmt::Debug for FieldName<N> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("FieldName")
            .field(&self.as_str())
            .finish()
    }
}
