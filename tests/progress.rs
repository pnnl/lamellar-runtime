#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/array/local_only/immutable_borrow.rs");
    t.compile_fail("tests/ui/array/local_only/clone.rs");
}
