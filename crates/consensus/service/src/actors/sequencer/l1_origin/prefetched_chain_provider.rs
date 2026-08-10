//! Watch-backed chain provider for prefetched sequencer origins.

use std::{future::Future, sync::Arc, time::Duration};

use alloy_consensus::{Header, Receipt, TxEnvelope};
use alloy_primitives::B256;
use async_trait::async_trait;
use base_consensus_derive::{ChainProvider, PipelineError, PipelineErrorKind};
use base_consensus_providers::{AlloyChainProvider, AlloyChainProviderError};
use base_protocol::BlockInfo;
use tokio::sync::watch;

use super::PreparedL1Origin;
use crate::Metrics;

/// Serves matching header and receipt requests from the selected prepared origin.
#[derive(Debug)]
pub struct PrefetchedChainProvider {
    origin: watch::Receiver<Option<PreparedL1Origin>>,
    fallback: AlloyChainProvider,
    fallback_timeout: Duration,
}

impl PrefetchedChainProvider {
    /// Creates a provider with a bounded RPC fallback.
    pub const fn new(
        origin: watch::Receiver<Option<PreparedL1Origin>>,
        fallback: AlloyChainProvider,
        fallback_timeout: Duration,
    ) -> Self {
        Self { origin, fallback, fallback_timeout }
    }

    async fn bounded_fallback<T>(
        timeout_duration: Duration,
        kind: &'static str,
        future: impl Future<Output = Result<T, AlloyChainProviderError>>,
    ) -> Result<T, PrefetchedChainProviderError> {
        tokio::time::timeout(timeout_duration, future).await.map_or_else(
            |_| {
                warn!(
                    target: "l1_origin_selector",
                    kind,
                    timeout_ms = timeout_duration.as_millis(),
                    "Timed out fetching L1 data from fallback provider"
                );
                Metrics::sequencer_l1_origin_fetch_timeouts_total(kind).increment(1);
                Err(PrefetchedChainProviderError::Timeout)
            },
            |result| result.map_err(PrefetchedChainProviderError::Fallback),
        )
    }
}

#[async_trait]
impl ChainProvider for PrefetchedChainProvider {
    type Error = PrefetchedChainProviderError;

    async fn header_by_hash(&mut self, hash: B256) -> Result<Header, Self::Error> {
        let header = self
            .origin
            .borrow()
            .as_ref()
            .filter(|origin| origin.hash == hash)
            .map(|origin| origin.header.clone());
        if let Some(header) = header {
            Metrics::sequencer_l1_origin_buffer_hits_total("header").increment(1);
            return Ok(header);
        }
        Metrics::sequencer_l1_origin_buffer_misses_total("header").increment(1);
        Self::bounded_fallback(self.fallback_timeout, "by_hash", self.fallback.header_by_hash(hash))
            .await
    }

    async fn block_info_by_number(&mut self, number: u64) -> Result<BlockInfo, Self::Error> {
        Self::bounded_fallback(
            self.fallback_timeout,
            "by_number",
            self.fallback.block_info_by_number(number),
        )
        .await
    }

    async fn receipts_by_hash(&mut self, hash: B256) -> Result<Vec<Receipt>, Self::Error> {
        let receipts = self
            .origin
            .borrow()
            .as_ref()
            .filter(|origin| origin.hash == hash)
            .map(|origin| Arc::clone(&origin.receipts));
        if let Some(receipts) = receipts {
            Metrics::sequencer_l1_origin_buffer_hits_total("receipts").increment(1);
            return Ok((*receipts).clone());
        }
        Metrics::sequencer_l1_origin_buffer_misses_total("receipts").increment(1);
        match Self::bounded_fallback(
            self.fallback_timeout,
            "receipts",
            self.fallback.receipts_by_hash(hash),
        )
        .await
        {
            Err(PrefetchedChainProviderError::Fallback(
                AlloyChainProviderError::BlockNotFound(_),
            )) => Err(PrefetchedChainProviderError::ReceiptsUnavailable(hash)),
            result => result,
        }
    }

    async fn block_info_and_transactions_by_hash(
        &mut self,
        hash: B256,
    ) -> Result<(BlockInfo, Vec<TxEnvelope>), Self::Error> {
        Self::bounded_fallback(
            self.fallback_timeout,
            "by_hash",
            self.fallback.block_info_and_transactions_by_hash(hash),
        )
        .await
    }
}

/// Error returned by [`PrefetchedChainProvider`].
#[derive(Debug, thiserror::Error)]
pub enum PrefetchedChainProviderError {
    /// The RPC fallback failed.
    #[error(transparent)]
    Fallback(#[from] AlloyChainProviderError),
    /// The bounded RPC fallback timed out.
    #[error("timed out fetching L1 data from fallback provider")]
    Timeout,
    /// The requested block exists but its receipts are not available yet.
    #[error("receipts unavailable for L1 origin: {0}")]
    ReceiptsUnavailable(B256),
}

impl From<PrefetchedChainProviderError> for PipelineErrorKind {
    fn from(error: PrefetchedChainProviderError) -> Self {
        match error {
            PrefetchedChainProviderError::Fallback(error) => error.into(),
            PrefetchedChainProviderError::Timeout => {
                Self::Temporary(PipelineError::Provider("L1 fallback lookup timed out".to_string()))
            }
            PrefetchedChainProviderError::ReceiptsUnavailable(hash) => Self::Temporary(
                PipelineError::Provider(format!("L1 origin receipts unavailable: {hash}")),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use alloy_provider::RootProvider;

    use super::*;

    fn fallback() -> AlloyChainProvider {
        AlloyChainProvider::new(
            RootProvider::new_http("http://localhost:1".parse().expect("valid URL")),
            1,
        )
    }

    #[tokio::test]
    async fn test_matching_origin_serves_header_and_receipts() {
        let header = Header { number: 7, timestamp: 84, ..Default::default() };
        let hash = header.hash_slow();
        let receipts = Arc::new(vec![Receipt::default()]);
        let (_tx, rx) = watch::channel(Some(PreparedL1Origin {
            hash,
            header: header.clone(),
            receipts: Arc::clone(&receipts),
        }));
        let mut provider = PrefetchedChainProvider::new(rx, fallback(), Duration::from_millis(1));

        assert_eq!(provider.header_by_hash(hash).await.unwrap(), header);
        assert_eq!(provider.receipts_by_hash(hash).await.unwrap(), *receipts);
    }

    #[tokio::test]
    async fn test_fallback_timeout_is_temporary() {
        let error = PrefetchedChainProvider::bounded_fallback(
            Duration::ZERO,
            "by_hash",
            pending::<Result<(), AlloyChainProviderError>>(),
        )
        .await
        .unwrap_err();
        let kind: PipelineErrorKind = error.into();

        assert!(matches!(kind, PipelineErrorKind::Temporary(_)));
    }

    #[test]
    fn test_missing_fallback_receipts_are_temporary() {
        let kind: PipelineErrorKind =
            PrefetchedChainProviderError::ReceiptsUnavailable(B256::ZERO).into();

        assert!(matches!(kind, PipelineErrorKind::Temporary(_)));
    }
}
