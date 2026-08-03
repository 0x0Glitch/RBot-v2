//! Deterministic JSON representations for collections unsupported as JSON objects.

pub(crate) mod btree_map {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

    pub(crate) fn serialize<S, K, V>(
        value: &BTreeMap<K, V>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        K: Ord + Serialize,
        V: Serialize,
    {
        value.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D, K, V>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
    where
        D: Deserializer<'de>,
        K: Ord + Deserialize<'de>,
        V: Deserialize<'de>,
    {
        let entries = Vec::<(K, V)>::deserialize(deserializer)?;
        let mut result = BTreeMap::new();
        for (key, value) in entries {
            if result.insert(key, value).is_some() {
                return Err(D::Error::custom("duplicate deterministic map key"));
            }
        }
        Ok(result)
    }
}
