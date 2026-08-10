//! L1 provider interfaces and implementations for origin selection.

use std::fmt::Debug;

use alloy_consensus::{Header, Receipt};
use alloy_primitives::B256;
use alloy_provider::{Provider, RootProvider};
use alloy_transport::TransportErrorKind;
use async_trait::async_trait;
use base_protocol::BlockInfo;
use tokio::sync::watch;

use super::{L1OriginSelectorError, PreparedL1Origin};

/// Prepared L1 origin provider interface for the [`super::L1OriginSelector`].
#[async_trait]
pub trait L1OriginSelectorProvider: Debug + Send + Sync + 'static {
    /// Returns the latest observed L1 head hash, used to identify the canonical chain view.
    fn chain_view(&self) -> Option<B256>;

    /// Returns a prepared origin by its hash.
    async fn prepared_by_hash(
        &self,
        hash: B256,
    ) -> Result<Option<PreparedL1Origin>, L1OriginSelectorError>;

    /// Returns a prepared origin by its number.
    async fn prepared_by_number(
        &self,
        number: u64,
    ) -> Result<Option<PreparedL1Origin>, L1OriginSelectorError>;
}

/// A wrapper around the [`RootProvider`] that delays the view of the L1 chain by a configurable
/// amount of blocks.
#[derive(Debug)]
pub struct DelayedL1OriginSelectorProvider {
    /// The inner [`RootProvider`].
    inner: RootProvider,
    /// The L1 head watch channel.
    l1_head: watch::Receiver<Option<BlockInfo>>,
    /// The confirmation depth to delay the view of the L1 chain.
    confirmation_depth: u64,
}

impl DelayedL1OriginSelectorProvider {
    /// Creates a new [`DelayedL1OriginSelectorProvider`].
    pub const fn new(
        inner: RootProvider,
        l1_head: watch::Receiver<Option<BlockInfo>>,
        confirmation_depth: u64,
    ) -> Self {
        Self { inner, l1_head, confirmation_depth }
    }

    async fn header_by_hash(&self, hash: B256) -> Result<Option<Header>, L1OriginSelectorError> {
        Ok(Provider::get_block_by_hash(&self.inner, hash)
            .await?
            .map(|block| block.header.into_consensus()))
    }

    async fn header_by_number(&self, number: u64) -> Result<Option<Header>, L1OriginSelectorError> {
        Ok(Provider::get_block_by_number(&self.inner, number.into())
            .await?
            .map(|block| block.header.into_consensus()))
    }

    async fn receipts_by_hash(
        &self,
        hash: B256,
    ) -> Result<Option<Vec<Receipt>>, L1OriginSelectorError> {
        let Some(receipts) = Provider::get_block_receipts(&self.inner, hash.into()).await? else {
            return Ok(None);
        };
        receipts
            .into_iter()
            .map(|receipt| receipt.inner.into_primitives_receipt().as_receipt().cloned())
            .collect::<Option<Vec<_>>>()
            .map(Some)
            .ok_or_else(|| {
                L1OriginSelectorError::Provider(TransportErrorKind::custom_str(
                    "failed to convert RPC receipts",
                ))
            })
    }
}

#[async_trait]
impl L1OriginSelectorProvider for DelayedL1OriginSelectorProvider {
    fn chain_view(&self) -> Option<B256> {
        self.l1_head.borrow().as_ref().map(|head| head.hash)
    }

    async fn prepared_by_hash(
        &self,
        hash: B256,
    ) -> Result<Option<PreparedL1Origin>, L1OriginSelectorError> {
        // By-hash lookups are not delayed, as they're direct indexes.
        let Some(header) = self.header_by_hash(hash).await? else {
            return Ok(None);
        };
        let returned_hash = header.hash_slow();
        if returned_hash != hash {
            warn!(target: "l1_origin_selector", requested = %hash, returned = %returned_hash, "L1 RPC returned a mismatched header hash");
            return Ok(None);
        }
        let Some(receipts) = self.receipts_by_hash(hash).await? else {
            return Err(L1OriginSelectorError::ReceiptsUnavailable(hash));
        };
        Ok(Some(PreparedL1Origin { hash, header, receipts: receipts.into() }))
    }

    async fn prepared_by_number(
        &self,
        number: u64,
    ) -> Result<Option<PreparedL1Origin>, L1OriginSelectorError> {
        let Some(l1_head) = *self.l1_head.borrow() else {
            // Without an observed head, a by-number result cannot be tied to a canonical chain
            // view or checked against the confirmation delay.
            return Ok(None);
        };

        if number == 0
            || self.confirmation_depth == 0
            || number.saturating_add(self.confirmation_depth) <= l1_head.number
        {
            let Some(header) = self.header_by_number(number).await? else {
                return Ok(None);
            };
            if header.number != number {
                warn!(
                    target: "l1_origin_selector",
                    requested = number,
                    returned = header.number,
                    "L1 RPC returned a header at the wrong block number"
                );
                return Err(L1OriginSelectorError::Provider(TransportErrorKind::custom_str(
                    "L1 RPC returned a header at the wrong block number",
                )));
            }
            let hash = header.hash_slow();
            let Some(receipts) = self.receipts_by_hash(hash).await? else {
                return Err(L1OriginSelectorError::ReceiptsUnavailable(hash));
            };
            Ok(Some(PreparedL1Origin { hash, header, receipts: receipts.into() }))
        } else {
            Ok(None)
        }
    }
}
