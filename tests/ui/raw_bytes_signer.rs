use alloy::primitives::Bytes;

struct ValidatedSignerPayload(Bytes);

fn sign(_: &ValidatedSignerPayload) {}

fn main() {
    sign(&Bytes::new());
}

