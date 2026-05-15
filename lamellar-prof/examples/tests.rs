use lamellar_prof::*;

init_prof!();

#[prof]
fn my_test_fn() {
    prof_start!(inner_test);
    std::thread::yield_now();
    prof_end!(inner_test);
}

/// Minimal future that yields once (returns Pending on first poll) so that
/// async profiling shows non-zero await_time.
struct YieldNow(bool);
impl std::future::Future for YieldNow {
    type Output = ();
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 {
            std::task::Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}

#[prof]
async fn my_async_fn() {
    YieldNow(false).await;
}

#[prof]
async fn my_async_caller() {
    my_async_fn().await;
    my_async_fn().await;
}

#[derive(std::fmt::Debug)]
struct TestStruct<T: std::fmt::Debug, S: std::fmt::Debug> {
    id: T,
    id2: S,
}

#[prof_all]
impl<T: std::fmt::Debug, S: std::fmt::Debug> TestStruct<T, S> {
    fn new(id: T, id2: S) -> TestStruct<T, S> {
        TestStruct { id, id2 }
    }
    fn test(&self) {
        println!("hello! {:?}", self);
        my_test_fn()
    }
}

#[prof]
impl<T: std::fmt::Debug, S: std::fmt::Debug> Drop for TestStruct<T, S> {
    fn drop(&mut self) {
        println!("dropping teststruct {:?}", self);
    }
}

#[derive(std::fmt::Debug)]
struct TestStruct1 {
    id: usize,
}

#[prof_all]
impl TestStruct1 {
    fn new(id: usize) -> TestStruct1 {
        TestStruct1 { id }
    }
    fn test(&self) {
        println!("hello! {:?}", self);
        my_test_fn()
    }
}

#[prof]
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
    futures::executor::block_on(my_async_caller());
    fini_prof!();
}
