// Compact record codec shared by the stream/adaptive batchers.
//
// A frame is a packed (unaligned) sequence of records. Each record starts with a
// 4-byte little-endian word: body_len:24 | flags:4 | kind:4, followed by body_len
// bytes of body. All multi-byte fields are little-endian.
//
//   AM / RETURN_AM: am_id u32 | req_id u64 | [sub_id u64] | team_addr u64 | payload
//   DATA:           req_id u64 | [sub_id u64] | darc_len u32 | darcs | payload
//   UNITS:          count u32 | count x (req_id u64 | sub_id varint)
//   PAD:            body ignored (ring-wrap filler)
//
// [sub_id] is omitted when F_SUB0 is set (sub_id == 0, the common case).


use crate::active_messaging::registered_active_message::AmId;
use crate::active_messaging::ReqId;

pub(crate) const REC_HDR_LEN: usize = 4;
pub(crate) const MAX_REC_BODY: usize = (1 << 24) - 1;

pub(crate) const KIND_AM: u8 = 1;
pub(crate) const KIND_RETURN_AM: u8 = 2;
pub(crate) const KIND_DATA: u8 = 3;
pub(crate) const KIND_UNITS: u8 = 4;
pub(crate) const KIND_PAD: u8 = 5;

pub(crate) const F_SUB0: u8 = 1;

#[inline(always)]
fn hdr_word(kind: u8, flags: u8, body_len: usize) -> u32 {
    debug_assert!(body_len <= MAX_REC_BODY);
    (body_len as u32) | ((flags as u32 & 0xf) << 24) | ((kind as u32) << 28)
}

#[inline(always)]
fn sub_flags(req_id: &ReqId) -> u8 {
    if req_id.sub_id == 0 {
        F_SUB0
    } else {
        0
    }
}

#[inline(always)]
fn req_len(req_id: &ReqId) -> usize {
    if req_id.sub_id == 0 {
        8
    } else {
        16
    }
}

#[inline(always)]
fn varint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

/// Write cursor over a record-sized slot (a Vec's spare capacity or a stream ring region).
struct Cur<'a> {
    b: &'a mut [u8],
    i: usize,
}

impl Cur<'_> {
    #[inline(always)]
    fn put(&mut self, s: &[u8]) {
        self.b[self.i..self.i + s.len()].copy_from_slice(s);
        self.i += s.len();
    }
    #[inline(always)]
    fn put_req(&mut self, req_id: &ReqId) {
        self.put(&(req_id.id as u64).to_le_bytes());
        if req_id.sub_id != 0 {
            self.put(&(req_id.sub_id as u64).to_le_bytes());
        }
    }
    #[inline(always)]
    fn put_varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.put(&[(v as u8) | 0x80]);
            v >>= 7;
        }
        self.put(&[v as u8]);
    }
}

/// Total encoded size (header included) of an AM / return-AM record.
#[inline(always)]
pub(crate) fn am_rec_len(req_id: &ReqId, payload_len: usize) -> usize {
    REC_HDR_LEN + 4 + req_len(req_id) + 8 + payload_len
}

/// Total encoded size (header included) of a data record.
#[inline(always)]
pub(crate) fn data_rec_len(req_id: &ReqId, darc_len: usize, payload_len: usize) -> usize {
    REC_HDR_LEN + req_len(req_id) + 4 + darc_len + payload_len
}

/// Total encoded size (header included) of a units record holding `reqs`.
pub(crate) fn units_rec_len<'a>(reqs: impl Iterator<Item = &'a ReqId>) -> usize {
    REC_HDR_LEN
        + 4
        + reqs
            .map(|r| 8 + varint_len(r.sub_id as u64))
            .sum::<usize>()
}

/// Appends a `rec_len`-byte record that `enc` writes in place. `enc` must write every
/// byte and never read them (the spare capacity is not zero-filled: a memset per AM).
#[inline(always)]
pub(crate) fn append(buf: &mut Vec<u8>, rec_len: usize, enc: impl FnOnce(&mut [u8])) {
    buf.reserve(rec_len);
    let start = buf.len();
    unsafe {
        enc(std::slice::from_raw_parts_mut(buf.as_mut_ptr().add(start), rec_len));
        buf.set_len(start + rec_len);
    }
}

/// Encodes an AM (`ret == false`) or return-AM record into `dst`, which must be exactly
/// `am_rec_len` bytes; `fill` writes the payload in place.
#[inline(always)]
pub(crate) fn enc_am(
    dst: &mut [u8],
    ret: bool,
    am_id: AmId,
    req_id: &ReqId,
    team_addr: usize,
    fill: impl FnOnce(&mut [u8]),
) {
    let kind = if ret { KIND_RETURN_AM } else { KIND_AM };
    let body_len = dst.len() - REC_HDR_LEN;
    let mut c = Cur { b: dst, i: 0 };
    c.put(&hdr_word(kind, sub_flags(req_id), body_len).to_le_bytes());
    c.put(&(am_id as u32).to_le_bytes());
    c.put_req(req_id);
    c.put(&(team_addr as u64).to_le_bytes());
    let Cur { b, i } = c;
    fill(&mut b[i..]);
}

/// Encodes a data (remote result) record into `dst` (exactly `data_rec_len` bytes).
#[inline(always)]
pub(crate) fn enc_data(dst: &mut [u8], req_id: &ReqId, darcs: &[u8], payload: &[u8]) {
    let body_len = dst.len() - REC_HDR_LEN;
    let mut c = Cur { b: dst, i: 0 };
    c.put(&hdr_word(KIND_DATA, sub_flags(req_id), body_len).to_le_bytes());
    c.put_req(req_id);
    c.put(&(darcs.len() as u32).to_le_bytes());
    c.put(darcs);
    c.put(payload);
}

/// Encodes one units record for `reqs` (non-empty) into `dst` (exactly `units_rec_len`).
#[inline(always)]
pub(crate) fn enc_units(dst: &mut [u8], reqs: &[ReqId]) {
    debug_assert!(!reqs.is_empty());
    let body_len = dst.len() - REC_HDR_LEN;
    let mut c = Cur { b: dst, i: 0 };
    c.put(&hdr_word(KIND_UNITS, 0, body_len).to_le_bytes());
    c.put(&(reqs.len() as u32).to_le_bytes());
    for r in reqs {
        c.put(&(r.id as u64).to_le_bytes());
        c.put_varint(r.sub_id as u64);
    }
}

/// Header word of a PAD record spanning `rec_len` (>= REC_HDR_LEN) bytes.
#[inline(always)]
pub(crate) fn pad_hdr(rec_len: usize) -> [u8; REC_HDR_LEN] {
    hdr_word(KIND_PAD, 0, rec_len - REC_HDR_LEN).to_le_bytes()
}

/// Appends an AM (`ret == false`) or return-AM (`ret == true`) record.
pub(crate) fn put_am(
    buf: &mut Vec<u8>,
    ret: bool,
    am_id: AmId,
    req_id: &ReqId,
    team_addr: usize,
    payload: &[u8],
) {
    put_am_with(buf, ret, am_id, req_id, team_addr, payload.len(), |dst| {
        dst.copy_from_slice(payload)
    });
}

/// Like `put_am`, but `fill` writes the `payload_len` payload bytes in place, so an AM
/// can be serialized straight into the frame.
#[inline]
pub(crate) fn put_am_with(
    buf: &mut Vec<u8>,
    ret: bool,
    am_id: AmId,
    req_id: &ReqId,
    team_addr: usize,
    payload_len: usize,
    fill: impl FnOnce(&mut [u8]),
) {
    append(buf, am_rec_len(req_id, payload_len), |d| {
        enc_am(d, ret, am_id, req_id, team_addr, fill)
    });
}

/// Appends a data (remote result) record.
#[cfg_attr(not(test), allow(dead_code))] // Vec encoders used by the codec tests
pub(crate) fn put_data(buf: &mut Vec<u8>, req_id: &ReqId, darcs: &[u8], payload: &[u8]) {
    append(buf, data_rec_len(req_id, darcs.len(), payload.len()), |d| {
        enc_data(d, req_id, darcs, payload)
    });
}

/// Appends one units record carrying every req id in `reqs` (must be non-empty).
#[cfg_attr(not(test), allow(dead_code))] // Vec encoders used by the codec tests
pub(crate) fn put_units(buf: &mut Vec<u8>, reqs: &[ReqId]) {
    append(buf, units_rec_len(reqs.iter()), |d| enc_units(d, reqs));
}

#[inline(always)]
fn rd_u32(b: &[u8], i: &mut usize) -> u32 {
    let v = u32::from_le_bytes(b[*i..*i + 4].try_into().unwrap());
    *i += 4;
    v
}

#[inline(always)]
fn rd_u64(b: &[u8], i: &mut usize) -> u64 {
    let v = u64::from_le_bytes(b[*i..*i + 8].try_into().unwrap());
    *i += 8;
    v
}

#[inline(always)]
fn rd_varint(b: &[u8], i: &mut usize) -> u64 {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let byte = b[*i];
        *i += 1;
        v |= ((byte & 0x7f) as u64) << shift;
        if byte < 0x80 {
            return v;
        }
        shift += 7;
    }
}

#[inline(always)]
fn rd_req(b: &[u8], i: &mut usize, flags: u8) -> ReqId {
    let id = rd_u64(b, i) as usize;
    let sub_id = if flags & F_SUB0 != 0 {
        0
    } else {
        rd_u64(b, i) as usize
    };
    ReqId { id, sub_id }
}

pub(crate) enum Record<'a> {
    Am {
        ret: bool,
        am_id: AmId,
        req_id: ReqId,
        team_addr: usize,
        payload: &'a [u8],
    },
    Data {
        req_id: ReqId,
        darcs: &'a [u8],
        payload: &'a [u8],
    },
    Units(UnitsIter<'a>),
    Pad,
}

pub(crate) struct UnitsIter<'a> {
    body: &'a [u8],
    pos: usize,
    remaining: u32,
}

impl Iterator for UnitsIter<'_> {
    type Item = ReqId;
    #[inline]
    fn next(&mut self) -> Option<ReqId> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let id = rd_u64(self.body, &mut self.pos) as usize;
        let sub_id = rd_varint(self.body, &mut self.pos) as usize;
        Some(ReqId { id, sub_id })
    }
}

/// Decodes every record in a frame, in order.
pub(crate) struct RecordIter<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> RecordIter<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        RecordIter { data, pos: 0 }
    }
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = Record<'a>;
    fn next(&mut self) -> Option<Record<'a>> {
        if self.pos + REC_HDR_LEN > self.data.len() {
            debug_assert_eq!(self.pos, self.data.len(), "trailing bytes in stream frame");
            return None;
        }
        let w = rd_u32(self.data, &mut self.pos);
        let body_len = (w & 0x00ff_ffff) as usize;
        let flags = ((w >> 24) & 0xf) as u8;
        let kind = (w >> 28) as u8;
        let body = &self.data[self.pos..self.pos + body_len];
        self.pos += body_len;
        let mut i = 0;
        Some(match kind {
            KIND_AM | KIND_RETURN_AM => {
                let am_id = rd_u32(body, &mut i) as AmId;
                let req_id = rd_req(body, &mut i, flags);
                let team_addr = rd_u64(body, &mut i) as usize;
                Record::Am {
                    ret: kind == KIND_RETURN_AM,
                    am_id,
                    req_id,
                    team_addr,
                    payload: &body[i..],
                }
            }
            KIND_DATA => {
                let req_id = rd_req(body, &mut i, flags);
                let darc_len = rd_u32(body, &mut i) as usize;
                let darcs = &body[i..i + darc_len];
                Record::Data {
                    req_id,
                    darcs,
                    payload: &body[i + darc_len..],
                }
            }
            KIND_UNITS => {
                let remaining = rd_u32(body, &mut i);
                Record::Units(UnitsIter {
                    body,
                    pos: i,
                    remaining,
                })
            }
            KIND_PAD => Record::Pad,
            k => panic!("[LAMELLAR ERROR] unknown stream record kind {k}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut buf = Vec::new();
        let r0 = ReqId { id: 0xdead_beef, sub_id: 0 };
        let r1 = ReqId { id: 7, sub_id: 300 };
        put_am(&mut buf, false, 5, &r0, 0x1000, b"hello");
        put_am(&mut buf, true, 9, &r1, 0x2000, b"");
        put_data(&mut buf, &r1, b"dd", b"payload");
        put_units(&mut buf, &[r0, r1]);
        let exp = am_rec_len(&r0, 5)
            + am_rec_len(&r1, 0)
            + data_rec_len(&r1, 2, 7)
            + units_rec_len([r0, r1].iter());
        assert_eq!(buf.len(), exp);

        let recs: Vec<_> = RecordIter::new(&buf).collect();
        assert_eq!(recs.len(), 4);
        match &recs[0] {
            Record::Am { ret, am_id, req_id, team_addr, payload } => {
                assert!(!ret);
                assert_eq!((*am_id, req_id.id, req_id.sub_id, *team_addr), (5, 0xdead_beef, 0, 0x1000));
                assert_eq!(*payload, b"hello");
            }
            _ => panic!(),
        }
        match &recs[1] {
            Record::Am { ret, am_id, req_id, payload, .. } => {
                assert!(ret);
                assert_eq!((*am_id, req_id.id, req_id.sub_id), (9, 7, 300));
                assert!(payload.is_empty());
            }
            _ => panic!(),
        }
        match &recs[2] {
            Record::Data { req_id, darcs, payload } => {
                assert_eq!((req_id.id, req_id.sub_id), (7, 300));
                assert_eq!((*darcs, *payload), (&b"dd"[..], &b"payload"[..]));
            }
            _ => panic!(),
        }
        match recs.into_iter().nth(3).unwrap() {
            Record::Units(it) => {
                let v: Vec<_> = it.map(|r| (r.id, r.sub_id)).collect();
                assert_eq!(v, vec![(0xdead_beef, 0), (7, 300)]);
            }
            _ => panic!(),
        }
    }
}
