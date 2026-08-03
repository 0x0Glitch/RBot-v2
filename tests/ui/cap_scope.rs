use alloy::primitives::B256;
use morpho_v2_reallocator::domain::{CapId, CapRef};

fn requires_vault_scope(_: CapRef) {}

fn main() {
    requires_vault_scope(CapId(B256::ZERO));
}

