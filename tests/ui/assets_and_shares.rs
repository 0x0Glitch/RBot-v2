use alloy::primitives::U256;
use morpho_v2_reallocator::domain::{Assets, Shares};

fn main() {
    let _invalid = Assets(U256::from(1)) + Shares(U256::from(1));
}

