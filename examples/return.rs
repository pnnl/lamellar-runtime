fn main() {
    let (my_pe, num_pes) = lamellar::init();
    println!("my_pe {:?} num_pes {:?}", my_pe, num_pes);
    lamellar::barrier();
    println!("exec_on_pe returns");
    let mut reqs = vec![];
    for pe in 0..num_pes {
        reqs.push(lamellar::exec_on_pe(
            pe,
            lamellar::FnOnce!([my_pe] move||{
                (my_pe,lamellar::local_pe())
            }),
        ));
    }
    for req in reqs {
        let res = req.get();
        println!("{:?}", res);
    }
    lamellar::barrier();
    println!("--------------------------------------------");

    println!("exec_all returns");
    let req = lamellar::exec_all(lamellar::FnOnce!([my_pe]move||{
        (my_pe,lamellar::local_pe())
    }));
    let res = req.get_all();
    println!("{:?}", res);
    lamellar::barrier();
    println!("--------------------------------------------");

    lamellar::barrier();
    lamellar::finit();
}
