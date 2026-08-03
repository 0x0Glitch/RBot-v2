use alloy::primitives::U256;
use morpho_v2_reallocator::domain::{RecordedAllocation, RequestedAssets};

fn update_recorded(_: RecordedAllocation) {}

fn main() {
    update_recorded(RequestedAssets(U256::from(1)));
}

