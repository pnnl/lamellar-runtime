use lamellar::array::{Distribution, LocalOnlyArray, ReadOnlyArray};
use lamellar::ActiveMessaging;
const ARRAY_LEN: usize = 100;

#[lamellar::AmData(Clone)]
struct ReadOnlyAm {
    data: ReadOnlyArray<usize>,
    orig_pe: usize,
}

#[lamellar::am]
impl LamellarAM for ReadOnlyAm {
    fn exec(self) {
        println!("{:?} {:?}", self.orig_pe, self.data.local_as_slice());
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let _num_pes = world.num_pes();
    let mut local_only_array =
        LocalOnlyArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block);

    for (i, elem) in local_only_array.as_mut_slice().iter_mut().enumerate() {
        *elem = i;
    }

    local_only_array.print();
    let read_only_array = local_only_array.into_read_only();
    for _i in 0..10 {
        world.exec_am_all(ReadOnlyAm {
            data: read_only_array.clone(),
            orig_pe: my_pe,
        });
    }
    println!("about to chance into local only");
    let local_only_array = read_only_array.into_local_only(); //this acts as a barrier until only the calling instance of read_only_array exists.
    local_only_array.print();

    // let mut vector = vec!{0;1000};
    // let vec_slice = vector.as_slice();

    // let mut_vec_slice = vector.as_mut_slice();
}
