use alloy::primitives::U256;
use morpho_v2_reallocator::domain::{AprBps, RatePerSecond};

fn main() {
    let _invalid = AprBps(30) < RatePerSecond(U256::from(1));
}

