use morpho_v2_reallocator::{domain::V2Plan, transaction::encoder::encode_validated_plan};

fn main() {
    let raw: V2Plan = todo!();
    encode_validated_plan(&raw);
}
