fn main() {
    let (my_pe, num_pes) = lamellar::init();
    println!("my_pe {:?} num_pes {:?}", my_pe, num_pes);

    lamellar::barrier();

    let req = lamellar::exec_all(
        lamellar::FnOnce!([my_pe] move || { //FnOnce! macro from serde_closure
            let orig_pe = my_pe;
            let local_pe = lamellar::local_pe();
            println!("Hello on {:?} from {:?}",local_pe,orig_pe);
            lamellar::exec_on_return(lamellar::FnOnce!([orig_pe,local_pe] move || {
                println!("I'm automatically executed!! I am: {:?} returning from {:?}",orig_pe,local_pe);
            }))
        }),
    );
    lamellar::wait_all();
    lamellar::barrier();
    let result = req.get();
    println!("I return none! {:?}", result);

    let req = lamellar::exec_all(
        lamellar::FnOnce!([my_pe] move || { //FnOnce! macro from serde_closure
            let orig_pe = my_pe;
            let local_pe = lamellar::local_pe();
            println!("Hello on {:?} from {:?}",local_pe,orig_pe);
            lamellar::FnOnce!([orig_pe,local_pe] move || {
                println!("I'm manually executed!! I am: {:?} returning from {:?}",orig_pe,local_pe);
            })
        }),
    );
    lamellar::wait_all();
    lamellar::barrier();
    let result = req.get();
    println!("I return a closure! {:?}", result);
    let c = result.unwrap();
    c();

    lamellar::barrier();
    lamellar::finit();
}
