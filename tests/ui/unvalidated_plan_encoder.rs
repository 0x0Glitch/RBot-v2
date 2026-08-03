use morpho_v2_reallocator::domain::V2Plan;

struct ValidatedV2Plan(V2Plan);

fn encode(_: &ValidatedV2Plan) {}

fn main() {
    let raw: V2Plan = todo!();
    encode(&raw);
}

