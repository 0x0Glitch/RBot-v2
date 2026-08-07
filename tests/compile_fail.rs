//! Compile-fail proof that semantic units and validation boundaries cannot be mixed.
#![allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]

#[test]
fn semantic_type_boundaries_do_not_compile() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/*.rs");
}
