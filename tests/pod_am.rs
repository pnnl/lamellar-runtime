use lamellar::active_messaging::prelude::*;
use lamellar::active_messaging::LamellarSerde;
use lamellar::TryFromBytes;

#[AmData(Pod, Debug, Clone, PartialEq)]
struct SmallPod {
    a: usize,
    b: u64,
    c: i32,
    d: bool,
    _pad: [u8; 3],
}

#[lamellar::am(Pod)]
impl LamellarAM for SmallPod {
    async fn exec(self) {}
}

#[test]
fn pod_am_round_trip() {
    let orig = SmallPod {
        a: 0xdead_beef,
        b: 42,
        c: -7,
        d: true,
        _pad: [0; 3],
    };

    assert_eq!(orig.serialized_size(), std::mem::size_of::<SmallPod>());

    let bytes = orig.serialize();
    assert_eq!(bytes.len(), orig.serialized_size());

    let decoded = <SmallPod as TryFromBytes>::try_read_from_bytes(&bytes)
        .ok()
        .expect("can decode pod bytes");
    assert_eq!(orig, decoded);

    let mut buf = vec![0u8; std::mem::size_of::<SmallPod>()];
    orig.serialize_into(&mut buf);
    assert_eq!(buf, bytes);
}
