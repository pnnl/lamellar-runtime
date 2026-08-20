/// ------------ Lamellar serialization RDMA ping-pong -----------------
/// Measures the *real* cost of different serialization codecs (bincode,
/// postcard, rkyv) over an actual RDMA line, with zero active-message/batcher
/// machinery involved. Codecs are invoked directly (`bincode::serde::*`,
/// `postcard::*`, `rkyv::*`) rather than through `lamellar::serialize`/
/// `deserialize` -- those wrappers are hardwired to whichever codec is
/// currently wired into `src/lib.rs` (postcard, as of Phase 4), so going
/// through them would only ever test one codec. This harness compares all
/// three within a single build of the current tree; it does not revert or
/// recompile lamellar itself at any earlier state.
///
/// PE0 encodes a value, RDMA-puts the bytes into PE_dst's slot of a
/// `SharedMemoryRegion<u8>`, PE_dst polls a trailing sentinel to detect
/// arrival, decodes, re-encodes, and puts the response back into PE0's slot
/// of a second region. Repeat, timing the round trip and totaling the actual
/// encoded byte counts to get real wire bytes per transfer.
///
/// Only `SmallPod`/`VecPayload` shapes are exercised here (not the
/// Darc/OneSidedMemoryRegion shapes from `benches/serialization.rs`): those
/// carry `MemRegionSendGuard`/`MEMREGION_SEND_CTX` refcount bookkeeping that
/// normally only fires around a real AM send, and this harness deliberately
/// calls codecs directly with none of that in scope -- doing so with a real
/// Darc/memregion field risks refcount mismatches that hang world teardown.
/// Their wire framing is already covered by the `triangle_count` run (see the
/// postcard-varint-length-bug fix); this harness's job is isolating plain-data
/// codec cost.
use lamellar::memregion::prelude::*;
use lamellar::ActiveMessaging;
use serde::{Deserialize, Serialize};
use std::time::Instant;

const WARMUP: usize = 50;
const ITERS: usize = 500;
const SENTINEL_LEN: usize = 8;
const LEN_PREFIX_LEN: usize = 4;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
struct SmallPod {
    a: usize,
    b: u64,
    c: i32,
    d: bool,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
struct VecPayload {
    items: Vec<u64>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
struct Compound {
    id: u64,
    name: String,
    values: Vec<f64>,
    tag: Option<i32>,
    pod: SmallPod,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
struct VecOfStructs {
    items: Vec<SmallPod>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Codec {
    Bincode,
    Postcard,
    Rkyv,
}

impl Codec {
    fn name(&self) -> &'static str {
        match self {
            Codec::Bincode => "bincode",
            Codec::Postcard => "postcard",
            Codec::Rkyv => "rkyv",
        }
    }
}

const CODECS: [Codec; 3] = [Codec::Bincode, Codec::Postcard, Codec::Rkyv];

fn encode_smallpod(codec: Codec, v: &SmallPod) -> Vec<u8> {
    match codec {
        Codec::Bincode => bincode::serde::encode_to_vec(v, bincode::config::legacy()).unwrap(),
        Codec::Postcard => postcard::to_allocvec(v).unwrap(),
        Codec::Rkyv => rkyv::to_bytes::<rkyv::rancor::Error>(v).unwrap().to_vec(),
    }
}

fn decode_smallpod(codec: Codec, bytes: &[u8]) -> SmallPod {
    match codec {
        Codec::Bincode => {
            bincode::serde::decode_from_slice(bytes, bincode::config::legacy())
                .unwrap()
                .0
        }
        Codec::Postcard => postcard::from_bytes(bytes).unwrap(),
        Codec::Rkyv => {
            let archived =
                rkyv::access::<<SmallPod as rkyv::Archive>::Archived, rkyv::rancor::Error>(bytes)
                    .unwrap();
            rkyv::deserialize::<SmallPod, rkyv::rancor::Error>(archived).unwrap()
        }
    }
}

fn encode_vecpayload(codec: Codec, v: &VecPayload) -> Vec<u8> {
    match codec {
        Codec::Bincode => bincode::serde::encode_to_vec(v, bincode::config::legacy()).unwrap(),
        Codec::Postcard => postcard::to_allocvec(v).unwrap(),
        Codec::Rkyv => rkyv::to_bytes::<rkyv::rancor::Error>(v).unwrap().to_vec(),
    }
}

fn decode_vecpayload(codec: Codec, bytes: &[u8]) -> VecPayload {
    match codec {
        Codec::Bincode => {
            bincode::serde::decode_from_slice(bytes, bincode::config::legacy())
                .unwrap()
                .0
        }
        Codec::Postcard => postcard::from_bytes(bytes).unwrap(),
        Codec::Rkyv => {
            let archived =
                rkyv::access::<<VecPayload as rkyv::Archive>::Archived, rkyv::rancor::Error>(bytes)
                    .unwrap();
            rkyv::deserialize::<VecPayload, rkyv::rancor::Error>(archived).unwrap()
        }
    }
}

fn encode_compound(codec: Codec, v: &Compound) -> Vec<u8> {
    match codec {
        Codec::Bincode => bincode::serde::encode_to_vec(v, bincode::config::legacy()).unwrap(),
        Codec::Postcard => postcard::to_allocvec(v).unwrap(),
        Codec::Rkyv => rkyv::to_bytes::<rkyv::rancor::Error>(v).unwrap().to_vec(),
    }
}

fn decode_compound(codec: Codec, bytes: &[u8]) -> Compound {
    match codec {
        Codec::Bincode => {
            bincode::serde::decode_from_slice(bytes, bincode::config::legacy())
                .unwrap()
                .0
        }
        Codec::Postcard => postcard::from_bytes(bytes).unwrap(),
        Codec::Rkyv => {
            let archived =
                rkyv::access::<<Compound as rkyv::Archive>::Archived, rkyv::rancor::Error>(bytes)
                    .unwrap();
            rkyv::deserialize::<Compound, rkyv::rancor::Error>(archived).unwrap()
        }
    }
}

fn encode_vecofstructs(codec: Codec, v: &VecOfStructs) -> Vec<u8> {
    match codec {
        Codec::Bincode => bincode::serde::encode_to_vec(v, bincode::config::legacy()).unwrap(),
        Codec::Postcard => postcard::to_allocvec(v).unwrap(),
        Codec::Rkyv => rkyv::to_bytes::<rkyv::rancor::Error>(v).unwrap().to_vec(),
    }
}

fn decode_vecofstructs(codec: Codec, bytes: &[u8]) -> VecOfStructs {
    match codec {
        Codec::Bincode => {
            bincode::serde::decode_from_slice(bytes, bincode::config::legacy())
                .unwrap()
                .0
        }
        Codec::Postcard => postcard::from_bytes(bytes).unwrap(),
        Codec::Rkyv => {
            let archived = rkyv::access::<
                <VecOfStructs as rkyv::Archive>::Archived,
                rkyv::rancor::Error,
            >(bytes)
            .unwrap();
            rkyv::deserialize::<VecOfStructs, rkyv::rancor::Error>(archived).unwrap()
        }
    }
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx] as f64 / 1000.0
}

fn print_header(label: &str) {
    println!();
    println!("# {label}");
    println!(
        "# {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>12}",
        "min_us", "p50_us", "p95_us", "p99_us", "max_us", "bytes/xfer"
    );
}

fn print_row(latencies: &mut Vec<u64>, bytes_per_xfer: f64) {
    latencies.sort_unstable();
    println!(
        "  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}  {:>12.1}",
        percentile(latencies, 0.0),
        percentile(latencies, 50.0),
        percentile(latencies, 95.0),
        percentile(latencies, 99.0),
        percentile(latencies, 100.0),
        bytes_per_xfer,
    );
}

/// Writes an explicit little-endian `u32` length prefix + `payload` + zero
/// padding out to `max_payload_len` + an 8-byte little-endian sentinel into
/// `region`'s slot on `pe`, in one `put_buffer` call. The sentinel always
/// lands at the fixed offset `LEN_PREFIX_LEN + max_payload_len` regardless of
/// the actual payload length, so `recv`'s poll never has to guess an offset
/// from a possibly-miscalibrated size (see `project_postcard_varint_length_bug`:
/// never assume a re-encode/sample stays the same width -- carry an explicit
/// length field instead).
fn send(
    region: &SharedMemoryRegion<u8>,
    pe: usize,
    payload: &[u8],
    max_payload_len: usize,
    seq: u64,
) {
    assert!(
        payload.len() <= max_payload_len,
        "payload {} exceeds calibrated slot capacity {}",
        payload.len(),
        max_payload_len
    );
    let mut buf = vec![0u8; LEN_PREFIX_LEN + max_payload_len + SENTINEL_LEN];
    buf[..LEN_PREFIX_LEN].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    buf[LEN_PREFIX_LEN..LEN_PREFIX_LEN + payload.len()].copy_from_slice(payload);
    buf[LEN_PREFIX_LEN + max_payload_len..].copy_from_slice(&seq.to_le_bytes());
    unsafe {
        region.put_buffer(pe, 0, buf).block();
    }
}

/// Spins on the local copy of `region`'s fixed-offset sentinel until it
/// equals `seq`, then reads the explicit length prefix and returns exactly
/// that many payload bytes (local-only reads, no RDMA op, no AM). Uses
/// `read_volatile` per byte (same idiom as this codebase's own magic-value
/// polling in `command_queues_*_eager.rs` and the UCX barrier in
/// `ucx_lamellae/fabric.rs`) -- a plain slice read here would let the
/// optimizer treat the memory as loop-invariant (nothing in this thread
/// writes it) and hoist the load, so the poll would never observe the remote
/// RDMA write.
fn recv(region: &SharedMemoryRegion<u8>, seq: u64, max_payload_len: usize) -> Vec<u8> {
    let sentinel_offset = LEN_PREFIX_LEN + max_payload_len;
    loop {
        let mut sentinel = [0u8; SENTINEL_LEN];
        unsafe {
            let base = region.as_slice().as_ptr().add(sentinel_offset);
            for (i, b) in sentinel.iter_mut().enumerate() {
                *b = std::ptr::read_volatile(base.add(i));
            }
        }
        if u64::from_le_bytes(sentinel) == seq {
            let mut len_bytes = [0u8; LEN_PREFIX_LEN];
            unsafe {
                let base = region.as_slice().as_ptr();
                for (i, b) in len_bytes.iter_mut().enumerate() {
                    *b = std::ptr::read_volatile(base.add(i));
                }
            }
            let len = u32::from_le_bytes(len_bytes) as usize;
            let slice = unsafe { region.as_slice() };
            return slice[LEN_PREFIX_LEN..LEN_PREFIX_LEN + len].to_vec();
        }
        std::hint::spin_loop();
    }
}

/// Runs the encode/put/decode/re-encode/put-back loop for one payload shape
/// under one codec. `make`/`mutate` let the caller vary the payload
/// per-iteration (e.g. sweep `VecPayload` length); `encode`/`decode` select
/// the codec under test (direct `bincode`/`postcard`/`rkyv` calls, not
/// `lamellar::serialize`/`deserialize`) while keeping the RDMA protocol
/// identical across all three.
fn run_shape<T, F, M, E, D>(
    world: &lamellar::LamellarWorld,
    label: &str,
    max_payload_len: usize,
    dst: usize,
    make: F,
    mutate: M,
    encode: E,
    decode: D,
) where
    T: Clone,
    F: Fn(usize) -> T,
    M: Fn(&mut T),
    E: Fn(&T) -> Vec<u8>,
    D: Fn(&[u8]) -> T,
{
    let my_pe = world.my_pe();
    let slot_len = LEN_PREFIX_LEN + max_payload_len + SENTINEL_LEN;
    let a_to_b: SharedMemoryRegion<u8> = world.alloc_shared_mem_region(slot_len).block();
    unsafe { a_to_b.as_mut_slice() }
        .iter_mut()
        .for_each(|b| *b = 0);
    let b_to_a: SharedMemoryRegion<u8> = world.alloc_shared_mem_region(slot_len).block();
    unsafe { b_to_a.as_mut_slice() }
        .iter_mut()
        .for_each(|b| *b = 0);
    world.barrier();

    if my_pe == 0 {
        print_header(label);
    }
    world.barrier();

    let mut latencies: Vec<u64> = Vec::with_capacity(ITERS);
    let mut total_bytes: u64 = 0;

    for i in 0..(WARMUP + ITERS) {
        let measuring = i >= WARMUP;
        let seq = i as u64 + 1;

        if my_pe == 0 {
            let t = Instant::now();
            let payload = make(i);
            let bytes = encode(&payload);
            send(&a_to_b, dst, &bytes, max_payload_len, seq);

            let resp_bytes = recv(&b_to_a, seq, max_payload_len);
            let _resp: T = decode(&resp_bytes);

            if measuring {
                latencies.push(t.elapsed().as_nanos() as u64);
                total_bytes += (bytes.len() + resp_bytes.len()) as u64;
            }
        } else if my_pe == dst {
            let bytes = recv(&a_to_b, seq, max_payload_len);
            let mut val: T = decode(&bytes);
            mutate(&mut val);
            let out = encode(&val);
            send(&b_to_a, 0, &out, max_payload_len, seq);
        }
    }
    world.barrier();

    if my_pe == 0 {
        let bytes_per_xfer = total_bytes as f64 / (2.0 * latencies.len() as f64);
        print_row(&mut latencies, bytes_per_xfer);
    }
    world.barrier();
}

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    if num_pes < 2 {
        eprintln!("Need at least 2 PEs");
        return;
    }
    let dst = num_pes - 1;

    if my_pe == 0 {
        println!("# serde RDMA ping-pong (no AMs)  pe0<->pe{dst}  iters={ITERS}");
    }

    let worst_case_smallpod = SmallPod {
        a: usize::MAX,
        b: u64::MAX,
        c: i32::MIN,
        d: true,
    };
    for codec in CODECS {
        run_shape(
            &world,
            &format!(
                "SmallPod {{ a: usize, b: u64, c: i32, d: bool }}  codec={}",
                codec.name()
            ),
            encode_smallpod(codec, &worst_case_smallpod).len(),
            dst,
            |i| SmallPod {
                a: i,
                b: i as u64,
                c: i as i32,
                d: i % 2 == 0,
            },
            |v: &mut SmallPod| v.a = v.a.wrapping_add(1),
            move |v: &SmallPod| encode_smallpod(codec, v),
            move |b: &[u8]| decode_smallpod(codec, b),
        );
    }

    for &len in &[8usize, 64, 256, 1024, 8192, 65536] {
        let worst_case_vec = VecPayload {
            items: vec![u64::MAX; len],
        };
        for codec in CODECS {
            run_shape(
                &world,
                &format!(
                    "VecPayload {{ items: Vec<u64> }}  len={len}  codec={}",
                    codec.name()
                ),
                encode_vecpayload(codec, &worst_case_vec).len(),
                dst,
                move |_| VecPayload {
                    items: (0..len as u64).collect(),
                },
                |v: &mut VecPayload| {
                    if let Some(first) = v.items.first_mut() {
                        *first = first.wrapping_add(1);
                    }
                },
                move |v: &VecPayload| encode_vecpayload(codec, v),
                move |b: &[u8]| decode_vecpayload(codec, b),
            );
        }
    }

    let worst_case_compound = Compound {
        id: u64::MAX,
        name: "x".repeat(32),
        values: vec![f64::MAX; 16],
        tag: Some(i32::MIN),
        pod: worst_case_smallpod,
    };
    for codec in CODECS {
        run_shape(
            &world,
            &format!(
                "Compound {{ id, name: String, values: Vec<f64>, tag: Option<i32>, pod: SmallPod }}  codec={}",
                codec.name()
            ),
            encode_compound(codec, &worst_case_compound).len(),
            dst,
            |i| Compound {
                id: i as u64,
                name: format!("{i:032}"),
                values: (0..16u64).map(|j| (i * 16 + j as usize) as f64).collect(),
                tag: if i % 2 == 0 { Some(i as i32) } else { None },
                pod: SmallPod {
                    a: i,
                    b: i as u64,
                    c: i as i32,
                    d: i % 2 == 0,
                },
            },
            |v: &mut Compound| v.id = v.id.wrapping_add(1),
            move |v: &Compound| encode_compound(codec, v),
            move |b: &[u8]| decode_compound(codec, b),
        );
    }

    for &len in &[64usize, 1024] {
        let worst_case_vos = VecOfStructs {
            items: vec![worst_case_smallpod; len],
        };
        for codec in CODECS {
            run_shape(
                &world,
                &format!(
                    "VecOfStructs {{ items: Vec<SmallPod> }}  len={len}  codec={}",
                    codec.name()
                ),
                encode_vecofstructs(codec, &worst_case_vos).len(),
                dst,
                move |i| VecOfStructs {
                    items: (0..len)
                        .map(|j| SmallPod {
                            a: i + j,
                            b: (i + j) as u64,
                            c: (i + j) as i32,
                            d: (i + j) % 2 == 0,
                        })
                        .collect(),
                },
                |v: &mut VecOfStructs| {
                    if let Some(first) = v.items.first_mut() {
                        first.a = first.a.wrapping_add(1);
                    }
                },
                move |v: &VecOfStructs| encode_vecofstructs(codec, v),
                move |b: &[u8]| decode_vecofstructs(codec, b),
            );
        }
    }
}
