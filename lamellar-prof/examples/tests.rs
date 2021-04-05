use lamellar_prof::*;

init_prof!();

//#[prof]
fn my_test_fn() {
    prof_start!(inner_test);
    std::thread::yield_now();
    prof_end!(inner_test);
}

#[derive(std::fmt::Debug)]
struct TestStruct<T: std::fmt::Debug, S: std::fmt::Debug> {
    id: T,
    id2: S,
}

//#[prof]
impl<T: std::fmt::Debug, S: std::fmt::Debug> TestStruct<T, S> {
    fn new(id: T, id2: S) -> TestStruct<T, S> {
        TestStruct { id: id, id2: id2 }
    }
    fn test(&self) {
        println!("hello! {:?}", self);
        my_test_fn()
    }
}

//#[prof]
impl<T: std::fmt::Debug, S: std::fmt::Debug> Drop for TestStruct<T, S> {
    fn drop(&mut self) {
        println!("dropping teststruct {:?}", self);
    }
}

#[derive(std::fmt::Debug)]
struct TestStruct1 {
    id: usize,
}

//#[prof]
impl TestStruct1 {
    fn new(id: usize) -> TestStruct1 {
        TestStruct1 { id: id }
    }
    fn test(&self) {
        println!("hello! {:?}", self);
        my_test_fn()
    }
}

//#[prof]
impl Drop for TestStruct1 {
    fn drop(&mut self) {
        println!("dropping teststruct {:?}", self);
    }
}

fn main() {
    my_test_fn();
    let test_struct: TestStruct<usize, String> = TestStruct::new(0, "Hi".to_owned());
    test_struct.test();
    let test_struct1 = TestStruct1::new(0);
    test_struct1.test();
    fini_prof!();
}
