//! Compile-fail proof that semantic units and validation boundaries cannot be mixed.

#[test]
fn semantic_type_boundaries_do_not_compile() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/*.rs");
}
