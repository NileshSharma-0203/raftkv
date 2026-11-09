//! Minimal binary codec. Every decode is bounds-checked, because these bytes
//! come from disks and networks that can lie.

use std::fmt;

const fn crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

static CRC_TABLE: [u32; 256] = crc_table();

/// CRC-32 (IEEE 802.3).
pub fn crc32(data: &[u8]) -> u32 {
    let mut c = 0xFFFF_FFFFu32;
    for &b in data {
        c = CRC_TABLE[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    c ^ 0xFFFF_FFFF
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError(pub &'static str);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "decode error: {}", self.0)
    }
}

impl std::error::Error for DecodeError {}

pub type DResult<T> = Result<T, DecodeError>;

#[derive(Default)]
pub struct Writer {
    pub buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_capacity(n: usize) -> Self {
        Writer { buf: Vec::with_capacity(n) }
    }
    pub fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    pub fn bool(&mut self, v: bool) {
        self.buf.push(v as u8);
    }
    pub fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    pub fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    pub fn bytes(&mut self, v: &[u8]) {
        self.u32(v.len() as u32);
        self.buf.extend_from_slice(v);
    }
    pub fn str(&mut self, v: &str) {
        self.bytes(v.as_bytes());
    }
    pub fn opt_u64(&mut self, v: Option<u64>) {
        match v {
            Some(x) => {
                self.u8(1);
                self.u64(x);
            }
            None => self.u8(0),
        }
    }
    pub fn opt_bytes(&mut self, v: Option<&[u8]>) {
        match v {
            Some(x) => {
                self.u8(1);
                self.bytes(x);
            }
            None => self.u8(0),
        }
    }
}

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }
    fn take(&mut self, n: usize) -> DResult<&'a [u8]> {
        if self.remaining() < n {
            return Err(DecodeError("truncated input"));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    pub fn u8(&mut self) -> DResult<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn bool(&mut self) -> DResult<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(DecodeError("invalid bool")),
        }
    }
    pub fn u32(&mut self) -> DResult<u32> {
        let mut a = [0u8; 4];
        a.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(a))
    }
    pub fn u64(&mut self) -> DResult<u64> {
        let mut a = [0u8; 8];
        a.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(a))
    }
    pub fn bytes(&mut self) -> DResult<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    pub fn vec(&mut self) -> DResult<Vec<u8>> {
        Ok(self.bytes()?.to_vec())
    }
    pub fn string(&mut self) -> DResult<String> {
        String::from_utf8(self.vec()?).map_err(|_| DecodeError("invalid utf-8"))
    }
    pub fn opt_u64(&mut self) -> DResult<Option<u64>> {
        Ok(if self.bool()? { Some(self.u64()?) } else { None })
    }
    pub fn opt_vec(&mut self) -> DResult<Option<Vec<u8>>> {
        Ok(if self.bool()? { Some(self.vec()?) } else { None })
    }
    pub fn finish(&self) -> DResult<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(DecodeError("trailing bytes"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_known_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn roundtrip() {
        let mut w = Writer::new();
        w.u8(7);
        w.bool(true);
        w.u32(99);
        w.u64(u64::MAX);
        w.bytes(b"hello");
        w.opt_u64(None);
        w.opt_bytes(Some(b"x"));
        let mut r = Reader::new(&w.buf);
        assert_eq!(r.u8().unwrap(), 7);
        assert!(r.bool().unwrap());
        assert_eq!(r.u32().unwrap(), 99);
        assert_eq!(r.u64().unwrap(), u64::MAX);
        assert_eq!(r.bytes().unwrap(), b"hello");
        assert_eq!(r.opt_u64().unwrap(), None);
        assert_eq!(r.opt_vec().unwrap(), Some(b"x".to_vec()));
        r.finish().unwrap();
    }

    #[test]
    fn huge_length_prefix_is_rejected() {
        let mut w = Writer::new();
        w.u32(u32::MAX);
        assert!(Reader::new(&w.buf).bytes().is_err());
    }
}
