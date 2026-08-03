use alloy::primitives::Bytes;
use morpho_v2_reallocator::transaction::signer::RoutineSigner;

async fn sign<S: RoutineSigner>(signer: &S) {
    let _ = signer.sign_rebalance(Bytes::new()).await;
}

fn main() {}
