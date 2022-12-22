use lamellar::array::prelude::*;

const ARRAY_LEN: usize = 100;
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();

    let array = ReadOnlyArray::<u8>::new(world.team(), ARRAY_LEN, Distribution::Block);
    if my_pe == 0 {
        println!("Block Full Array");
        for pe in 0..array.num_pes(){
            let si = array.first_global_index_for_pe(pe);
            let ei = array.last_global_index_for_pe(pe);
            println!("PE: {pe} Si: {si:?} Ei: {ei:?}");
        }

        let sub_array = array.sub_array(33..66);

        println!("Block Sub Array");
        for pe in 0..sub_array.num_pes(){
            let si = sub_array.first_global_index_for_pe(pe);
            let ei = sub_array.last_global_index_for_pe(pe);
            println!("PE: {pe} Si: {si:?} Ei: {ei:?}");
        }
    }

    let array = ReadOnlyArray::<u8>::new(world.team(), ARRAY_LEN, Distribution::Cyclic);
    if my_pe == 0 {
        println!("Cyclic Full Array");
        for pe in 0..array.num_pes(){
            let si = array.first_global_index_for_pe(pe);
            let ei = array.last_global_index_for_pe(pe);
            println!("PE: {pe} Si: {si:?} Ei: {ei:?}");
        }

        let sub_array = array.sub_array(33..66);
        println!("Cyclic Sub Array");
        for pe in 0..sub_array.num_pes(){
            let si = sub_array.first_global_index_for_pe(pe);
            let ei = sub_array.last_global_index_for_pe(pe);
            println!("PE: {pe} Si: {si:?} Ei: {ei:?}");
        }
    }
}