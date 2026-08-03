//! Bounded common-ancestor discovery against durable canonical history.

use crate::domain::BlockRef;
use crate::storage::actor::StorageHandle;

use super::ChainError;
use super::provider::ChainDataProvider;

/// Finds a hash-equal stored/provider common ancestor within an exact height bound.
pub async fn find_common_ancestor<P: ChainDataProvider>(
    provider: &P,
    storage: &StorageHandle,
    chain_id: u64,
    old_head: BlockRef,
    new_head: BlockRef,
    maximum_rewind: u64,
) -> Result<BlockRef, ChainError> {
    let highest_common_height = old_head.number.min(new_head.number);
    let available_steps = highest_common_height.saturating_add(1);
    let attempts = maximum_rewind.saturating_add(1).min(available_steps);
    for distance in 0..attempts {
        let number = highest_common_height.saturating_sub(distance);
        let stored = storage.load_canonical_block(chain_id, number).await?;
        if let Some(stored) = stored {
            let remote = provider.header_by_number(number).await?;
            if stored.hash == remote.hash {
                return Ok(remote);
            }
        }
    }
    Err(ChainError::DeepReorg {
        searched_blocks: attempts,
    })
}
