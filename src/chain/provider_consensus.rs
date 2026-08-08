//! Shared agreement rules for optional reads from independent providers.

use std::future::Future;

use futures::future::join_all;

/// Failure to select one authoritative optional provider view.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum OptionalViewSelectionError<E> {
    /// Two providers returned different non-null values.
    Disagreement,
    /// No provider returned a value and at least one provider failed.
    Unavailable(E),
}

/// Selects one consistent optional value while retaining configured provider error order.
///
/// A non-null value remains usable when another provider returns `None` or fails. Conflicting
/// non-null values fail closed. If no value is available, the first error in input order wins;
/// an all-null (or empty) view is a successful absence.
pub(crate) fn select_consistent_optional_view<T, E>(
    views: impl IntoIterator<Item = Result<Option<T>, E>>,
) -> Result<Option<T>, OptionalViewSelectionError<E>>
where
    T: PartialEq,
{
    let mut observed = None;
    let mut first_error = None;
    for view in views {
        match view {
            Ok(Some(value)) if observed.as_ref().is_some_and(|known| known != &value) => {
                return Err(OptionalViewSelectionError::Disagreement);
            }
            Ok(Some(value)) => observed = Some(value),
            Ok(None) => {}
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        }
    }

    if observed.is_some() || first_error.is_none() {
        return Ok(observed);
    }

    match first_error {
        Some(error) => Err(OptionalViewSelectionError::Unavailable(error)),
        None => Ok(None),
    }
}

/// Queries independent providers concurrently, then selects in configured provider order.
pub(crate) async fn query_consistent_optional_views<P, T, E, F, Fut>(
    providers: impl IntoIterator<Item = P>,
    query: F,
) -> Result<Option<T>, OptionalViewSelectionError<E>>
where
    T: PartialEq,
    F: Fn(P) -> Fut,
    Fut: Future<Output = Result<Option<T>, E>>,
{
    let views = join_all(providers.into_iter().map(query)).await;
    select_consistent_optional_view(views)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::{sync::Barrier, time::timeout};

    use super::{
        OptionalViewSelectionError, query_consistent_optional_views,
        select_consistent_optional_view,
    };

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ProbeError {
        Primary,
        Checkpoint,
    }

    #[test]
    fn optional_view_selection_truth_table_preserves_existing_semantics() {
        assert_eq!(
            select_consistent_optional_view([Ok(Some(7_u64)), Err(ProbeError::Checkpoint),]),
            Ok(Some(7))
        );
        assert_eq!(
            select_consistent_optional_view([
                Err::<Option<u64>, _>(ProbeError::Primary),
                Ok(Some(7)),
            ]),
            Ok(Some(7))
        );
        assert_eq!(
            select_consistent_optional_view([Ok::<Option<u64>, ProbeError>(Some(7)), Ok(Some(7)),]),
            Ok(Some(7))
        );
        assert_eq!(
            select_consistent_optional_view([Ok::<Option<u64>, ProbeError>(Some(7)), Ok(Some(8)),]),
            Err(OptionalViewSelectionError::Disagreement)
        );
        assert_eq!(
            select_consistent_optional_view([Ok::<Option<u64>, ProbeError>(None), Ok(None),]),
            Ok(None)
        );
        assert_eq!(
            select_consistent_optional_view([Ok(None::<u64>), Err(ProbeError::Checkpoint),]),
            Err(OptionalViewSelectionError::Unavailable(
                ProbeError::Checkpoint
            ))
        );
        assert_eq!(
            select_consistent_optional_view([
                Err::<Option<u64>, _>(ProbeError::Primary),
                Err(ProbeError::Checkpoint),
            ]),
            Err(OptionalViewSelectionError::Unavailable(ProbeError::Primary))
        );
        assert_eq!(
            select_consistent_optional_view(std::iter::empty::<Result<Option<u64>, ProbeError>>()),
            Ok(None)
        );
    }

    #[tokio::test]
    async fn delayed_provider_queries_are_concurrent_but_errors_keep_provider_order() {
        let barrier = Arc::new(Barrier::new(2));
        let providers = [
            (Duration::from_millis(40), ProbeError::Primary),
            (Duration::from_millis(1), ProbeError::Checkpoint),
        ];
        let selected = timeout(
            Duration::from_millis(500),
            query_consistent_optional_views(providers, |(delay, error)| {
                let barrier = Arc::clone(&barrier);
                async move {
                    barrier.wait().await;
                    tokio::time::sleep(delay).await;
                    Err::<Option<u64>, _>(error)
                }
            }),
        )
        .await;

        assert!(matches!(
            selected,
            Ok(Err(OptionalViewSelectionError::Unavailable(
                ProbeError::Primary
            )))
        ));
    }
}
