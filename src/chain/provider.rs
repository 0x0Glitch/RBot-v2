//! Role-scoped HTTP JSON-RPC providers with no generic public request surface.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::primitives::{Address, B256, Bytes};
use async_trait::async_trait;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use url::Url;

use crate::config::{RpcRole, ValidatedRpcConfig};
use crate::domain::BlockRef;

const RPC_TIMEOUT: Duration = Duration::from_secs(20);
const RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Provider capability role.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ProviderRole {
    /// Canonical head polling.
    Head,
    /// Log retrieval.
    Logs,
    /// Exact state reads.
    Read,
    /// Transaction simulation.
    Simulate,
    /// Signed transaction submission.
    Submit,
    /// Receipt reads.
    Receipt,
    /// Independent correctness checkpoint.
    Checkpoint,
}

impl From<RpcRole> for ProviderRole {
    fn from(value: RpcRole) -> Self {
        match value {
            RpcRole::Head => Self::Head,
            RpcRole::Logs => Self::Logs,
            RpcRole::Read => Self::Read,
            RpcRole::Simulate => Self::Simulate,
            RpcRole::Submit => Self::Submit,
            RpcRole::Receipt => Self::Receipt,
            RpcRole::Checkpoint => Self::Checkpoint,
        }
    }
}

/// Exact provider log response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RpcLog {
    /// Emitting address.
    pub address: Address,
    /// Event topics.
    pub topics: Vec<B256>,
    /// Event data.
    pub data: Bytes,
    /// Canonical block number quantity.
    pub block_number: Option<String>,
    /// Canonical block hash.
    pub block_hash: Option<B256>,
    /// Transaction hash.
    pub transaction_hash: Option<B256>,
    /// Transaction index quantity.
    pub transaction_index: Option<String>,
    /// Log index quantity.
    pub log_index: Option<String>,
    /// Provider removed marker.
    #[serde(default)]
    pub removed: bool,
}

/// Exact provider transaction receipt response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RpcReceipt {
    /// Transaction hash.
    pub transaction_hash: B256,
    /// Block hash.
    pub block_hash: B256,
    /// Block number quantity.
    pub block_number: String,
    /// Transaction index quantity.
    pub transaction_index: String,
    /// Optional status quantity.
    pub status: Option<String>,
    /// Gas-used quantity.
    pub gas_used: String,
    /// Ordered receipt logs.
    pub logs: Vec<RpcLog>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcHeader {
    number: String,
    hash: B256,
    parent_hash: B256,
    timestamp: String,
}

impl RpcHeader {
    fn into_block_ref(self) -> Result<BlockRef, ProviderError> {
        Ok(BlockRef {
            number: parse_quantity("block.number", &self.number)?,
            hash: self.hash,
            parent_hash: self.parent_hash,
            timestamp: parse_quantity("block.timestamp", &self.timestamp)?,
        })
    }
}

/// Startup capability probe inputs with no secret values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityProbe {
    /// Read-only target used for code/storage/call/estimate checks.
    pub read_target: Address,
    /// Approved read calldata for `eth_call` and gas capability checks.
    pub read_calldata: Bytes,
    /// Configured dedicated signer.
    pub signer: Address,
    /// Known transaction hash; null results still establish method support.
    pub known_transaction_hash: B256,
}

/// Results of required startup RPC capability calls.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCapabilities {
    /// Returned chain ID.
    pub chain_id: u64,
    /// Latest head.
    pub latest_head: BlockRef,
    /// Block receipts method is available.
    pub block_receipts: bool,
    /// One-block log query succeeded.
    pub logs: bool,
    /// Read-only call succeeded.
    pub call: bool,
    /// Gas estimate succeeded.
    pub estimate_gas: bool,
    /// Code read succeeded.
    pub code: bool,
    /// Storage read succeeded.
    pub storage: bool,
    /// Transaction-count read succeeded.
    pub transaction_count: bool,
    /// Transaction lookup method succeeded.
    pub transaction_lookup: bool,
    /// Receipt lookup method succeeded.
    pub receipt_lookup: bool,
    /// HyperEVM signer is confirmed not to use big blocks.
    pub signer_uses_big_blocks: bool,
}

/// Provider or JSON-RPC failure. Endpoint URLs and credentials are never included.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ProviderError {
    /// Required role is absent from this provider.
    #[error("provider `{provider}` lacks required role {role:?}")]
    MissingRole {
        /// Stable provider name.
        provider: String,
        /// Required role.
        role: ProviderRole,
    },
    /// Endpoint environment reference is absent or malformed.
    #[error("provider endpoint configuration is invalid")]
    Endpoint,
    /// HTTP transport failed; details are intentionally redacted to protect credential-bearing URLs.
    #[error("provider transport failed")]
    Transport,
    /// JSON-RPC response could not be decoded.
    #[error("provider returned malformed JSON-RPC")]
    MalformedResponse,
    /// JSON-RPC method is unsupported.
    #[error("provider does not support method {method}")]
    MethodUnsupported {
        /// Static method name.
        method: &'static str,
    },
    /// JSON-RPC returned an execution or parameter error.
    #[error("provider RPC method {method} failed with code {code}")]
    Rpc {
        /// Static method name.
        method: &'static str,
        /// JSON-RPC error code.
        code: i64,
    },
    /// A requested block was not returned.
    #[error("provider did not return requested block")]
    MissingBlock,
    /// Hex quantity is malformed or exceeds `u64`.
    #[error("invalid RPC quantity for {field}")]
    Quantity {
        /// Stable field name.
        field: &'static str,
    },
    /// Returned chain differs from configured chain.
    #[error("provider chain ID mismatch: expected {expected}, observed {observed}")]
    ChainMismatch {
        /// Configured chain ID.
        expected: u64,
        /// Returned chain ID.
        observed: u64,
    },
    /// HyperEVM reports the configured signer uses big blocks.
    #[error("configured signer is opted into HyperEVM big blocks")]
    SignerUsesBigBlocks,
}

/// Read-only data surface required by canonical chain ingestion.
#[async_trait]
pub trait ChainDataProvider: Send + Sync {
    /// Stable provider name.
    fn name(&self) -> &str;
    /// Returns whether this provider owns a role.
    fn has_role(&self, role: ProviderRole) -> bool;
    /// EVM chain ID.
    async fn chain_id(&self) -> Result<u64, ProviderError>;
    /// Latest head using exactly `eth_getBlockByNumber("latest", false)`.
    async fn latest_header(&self) -> Result<BlockRef, ProviderError>;
    /// Header at one exact block number.
    async fn header_by_number(&self, number: u64) -> Result<BlockRef, ProviderError>;
    /// All receipts in a block, or an explicit unsupported error.
    async fn block_receipts(&self, number: u64) -> Result<Vec<RpcReceipt>, ProviderError>;
    /// Deterministic bounded log query.
    async fn logs(
        &self,
        from: u64,
        to: u64,
        addresses: &[Address],
    ) -> Result<Vec<RpcLog>, ProviderError>;
    /// One receipt lookup for fallback ingestion and transaction recovery.
    async fn receipt_by_hash(&self, hash: B256) -> Result<Option<RpcReceipt>, ProviderError>;
}

/// Typed, read-only transaction simulation surface used by final preflight.
#[async_trait]
pub trait TransactionSimulationProvider: Send + Sync {
    /// Executes the exact zero-value call from the configured allocator at one canonical block.
    async fn call_at(
        &self,
        from: Address,
        target: Address,
        data: &Bytes,
        block: BlockRef,
    ) -> Result<Bytes, ProviderError>;

    /// Estimates the exact zero-value call from the configured allocator at one block number.
    async fn estimate_gas_at(
        &self,
        from: Address,
        target: Address,
        data: &Bytes,
        block: BlockRef,
    ) -> Result<u64, ProviderError>;

    /// Confirms the dedicated EOA remains on HyperEVM's fast-block lane.
    async fn using_big_blocks(&self, signer: Address) -> Result<bool, ProviderError>;
}

/// Signed-byte-only submission surface.
#[async_trait]
pub trait SignedTransactionSubmitter: Send + Sync {
    /// Broadcasts exact already-durable EIP-2718 bytes.
    async fn submit_signed_bytes(&self, signed: &Bytes) -> Result<B256, ProviderError>;
}

/// Role-scoped HTTP provider.
pub struct HttpProvider {
    name: String,
    endpoint: Url,
    roles: BTreeSet<ProviderRole>,
    client: Client,
    next_id: AtomicU64,
}

impl std::fmt::Debug for HttpProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpProvider")
            .field("name", &self.name)
            .field("roles", &self.roles)
            .finish_non_exhaustive()
    }
}

impl HttpProvider {
    /// Resolves only the endpoint environment reference from validated configuration.
    pub fn from_config(config: &ValidatedRpcConfig) -> Result<Self, ProviderError> {
        let raw = std::env::var(&config.url_env).map_err(|_| ProviderError::Endpoint)?;
        let endpoint = Url::parse(&raw).map_err(|_| ProviderError::Endpoint)?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(ProviderError::Endpoint);
        }
        Ok(Self {
            name: config.name.clone(),
            endpoint,
            roles: config
                .roles
                .iter()
                .copied()
                .map(ProviderRole::from)
                .collect(),
            client: build_client()?,
            next_id: AtomicU64::new(1),
        })
    }

    /// Constructs a provider from an already resolved endpoint, primarily for deterministic tests.
    pub fn new(
        name: String,
        endpoint: Url,
        roles: BTreeSet<ProviderRole>,
    ) -> Result<Self, ProviderError> {
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(ProviderError::Endpoint);
        }
        Ok(Self {
            name,
            endpoint,
            roles,
            client: build_client()?,
            next_id: AtomicU64::new(1),
        })
    }

    /// Runs required startup method probes and enforces chain/signer-lane identity.
    pub async fn probe_capabilities(
        &self,
        expected_chain_id: u64,
        probe: &CapabilityProbe,
    ) -> Result<ProviderCapabilities, ProviderError> {
        let chain_id_quantity: String = self.request_unscoped("eth_chainId", json!([])).await?;
        let chain_id = parse_quantity("eth_chainId", &chain_id_quantity)?;
        if chain_id != expected_chain_id {
            return Err(ProviderError::ChainMismatch {
                expected: expected_chain_id,
                observed: chain_id,
            });
        }
        let latest_header: Option<RpcHeader> = self
            .request_unscoped("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        let latest_head = latest_header
            .ok_or(ProviderError::MissingBlock)?
            .into_block_ref()?;
        let block_receipts = match self
            .request_unscoped::<Vec<RpcReceipt>>(
                "eth_getBlockReceipts",
                json!([format_quantity(latest_head.number)]),
            )
            .await
        {
            Ok(_) => true,
            Err(ProviderError::MethodUnsupported { .. }) => false,
            Err(error) => return Err(error),
        };
        let _: Vec<RpcLog> = self
            .request_unscoped(
                "eth_getLogs",
                json!([{ "fromBlock": format_quantity(latest_head.number), "toBlock": format_quantity(latest_head.number) }]),
            )
            .await?;
        let _: Bytes = self
            .request_unscoped(
                "eth_call",
                json!([{ "to": probe.read_target, "data": probe.read_calldata }, "latest"]),
            )
            .await?;
        let _: String = self
            .request_unscoped(
                "eth_estimateGas",
                json!([{ "from": probe.signer, "to": probe.read_target, "data": probe.read_calldata }, "latest"]),
            )
            .await?;
        let _: Bytes = self
            .request_unscoped("eth_getCode", json!([probe.read_target, "latest"]))
            .await?;
        let _: B256 = self
            .request_unscoped(
                "eth_getStorageAt",
                json!([probe.read_target, B256::ZERO, "latest"]),
            )
            .await?;
        let _: String = self
            .request_unscoped("eth_getTransactionCount", json!([probe.signer, "latest"]))
            .await?;
        let _: Option<Value> = self
            .request_unscoped(
                "eth_getTransactionByHash",
                json!([probe.known_transaction_hash]),
            )
            .await?;
        let _: Option<RpcReceipt> = self
            .request_unscoped(
                "eth_getTransactionReceipt",
                json!([probe.known_transaction_hash]),
            )
            .await?;
        let signer_uses_big_blocks: bool = self
            .request_unscoped("eth_usingBigBlocks", json!([probe.signer]))
            .await?;
        if signer_uses_big_blocks {
            return Err(ProviderError::SignerUsesBigBlocks);
        }
        Ok(ProviderCapabilities {
            chain_id,
            latest_head,
            block_receipts,
            logs: true,
            call: true,
            estimate_gas: true,
            code: true,
            storage: true,
            transaction_count: true,
            transaction_lookup: true,
            receipt_lookup: true,
            signer_uses_big_blocks,
        })
    }

    /// Executes a read-only call at latest state.
    pub async fn read_call(&self, target: Address, data: &Bytes) -> Result<Bytes, ProviderError> {
        self.request(
            ProviderRole::Read,
            "eth_call",
            json!([{ "to": target, "data": data }, "latest"]),
        )
        .await
    }

    /// Estimates one already-scoped transaction at latest state.
    pub async fn estimate_gas(
        &self,
        from: Address,
        target: Address,
        data: &Bytes,
    ) -> Result<u64, ProviderError> {
        let quantity: String = self
            .request(
                ProviderRole::Simulate,
                "eth_estimateGas",
                json!([{ "from": from, "to": target, "data": data }, "latest"]),
            )
            .await?;
        parse_quantity("eth_estimateGas", &quantity)
    }

    /// Reads complete runtime bytecode at latest state.
    pub async fn code_at(&self, target: Address) -> Result<Bytes, ProviderError> {
        self.request(ProviderRole::Read, "eth_getCode", json!([target, "latest"]))
            .await
    }

    /// Reads one storage slot at latest state.
    pub async fn storage_at(&self, target: Address, slot: B256) -> Result<B256, ProviderError> {
        self.request(
            ProviderRole::Read,
            "eth_getStorageAt",
            json!([target, slot, "latest"]),
        )
        .await
    }

    /// Reads the latest signer nonce.
    pub async fn transaction_count(&self, signer: Address) -> Result<u64, ProviderError> {
        let quantity: String = self
            .request(
                ProviderRole::Read,
                "eth_getTransactionCount",
                json!([signer, "latest"]),
            )
            .await?;
        parse_quantity("eth_getTransactionCount", &quantity)
    }

    /// Reads HyperEVM signer block mode.
    pub async fn using_big_blocks(&self, signer: Address) -> Result<bool, ProviderError> {
        self.request(ProviderRole::Read, "eth_usingBigBlocks", json!([signer]))
            .await
    }

    /// Submits already signed EIP-2718 bytes. No generic transaction object is accepted.
    pub async fn send_raw_transaction(&self, signed: &Bytes) -> Result<B256, ProviderError> {
        self.request(
            ProviderRole::Submit,
            "eth_sendRawTransaction",
            json!([signed]),
        )
        .await
    }

    fn require_role(&self, role: ProviderRole) -> Result<(), ProviderError> {
        if self.roles.contains(&role) {
            Ok(())
        } else {
            Err(ProviderError::MissingRole {
                provider: self.name.clone(),
                role,
            })
        }
    }

    fn require_any_role(&self, roles: &[ProviderRole]) -> Result<(), ProviderError> {
        if roles.iter().any(|role| self.roles.contains(role)) {
            Ok(())
        } else {
            Err(ProviderError::MissingRole {
                provider: self.name.clone(),
                role: roles.first().copied().unwrap_or(ProviderRole::Read),
            })
        }
    }

    async fn request<R: DeserializeOwned>(
        &self,
        role: ProviderRole,
        method: &'static str,
        params: Value,
    ) -> Result<R, ProviderError> {
        self.require_role(role)?;
        self.request_unscoped(method, params).await
    }

    async fn request_unscoped<R: DeserializeOwned>(
        &self,
        method: &'static str,
        params: Value,
    ) -> Result<R, ProviderError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request = RpcRequest {
            jsonrpc: "2.0",
            id,
            method,
            params,
        };
        let response = self
            .client
            .post(self.endpoint.clone())
            .json(&request)
            .send()
            .await
            .map_err(|_| ProviderError::Transport)?;
        if !response.status().is_success() {
            return Err(ProviderError::Transport);
        }
        let envelope: RpcResponse = response
            .json()
            .await
            .map_err(|_| ProviderError::MalformedResponse)?;
        if envelope.jsonrpc != "2.0" || envelope.id != id {
            return Err(ProviderError::MalformedResponse);
        }
        if let Some(error) = envelope.error {
            if error.code == -32601 {
                return Err(ProviderError::MethodUnsupported { method });
            }
            return Err(ProviderError::Rpc {
                method,
                code: error.code,
            });
        }
        serde_json::from_value(envelope.result).map_err(|_| ProviderError::MalformedResponse)
    }
}

#[async_trait]
impl ChainDataProvider for HttpProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn has_role(&self, role: ProviderRole) -> bool {
        self.roles.contains(&role)
    }

    async fn chain_id(&self) -> Result<u64, ProviderError> {
        self.require_any_role(&[
            ProviderRole::Head,
            ProviderRole::Read,
            ProviderRole::Checkpoint,
        ])?;
        let quantity: String = self.request_unscoped("eth_chainId", json!([])).await?;
        parse_quantity("eth_chainId", &quantity)
    }

    async fn latest_header(&self) -> Result<BlockRef, ProviderError> {
        self.require_any_role(&[ProviderRole::Head, ProviderRole::Checkpoint])?;
        let header: Option<RpcHeader> = self
            .request_unscoped("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        header.ok_or(ProviderError::MissingBlock)?.into_block_ref()
    }

    async fn header_by_number(&self, number: u64) -> Result<BlockRef, ProviderError> {
        self.require_any_role(&[ProviderRole::Head, ProviderRole::Checkpoint])?;
        let header: Option<RpcHeader> = self
            .request_unscoped(
                "eth_getBlockByNumber",
                json!([format_quantity(number), false]),
            )
            .await?;
        header.ok_or(ProviderError::MissingBlock)?.into_block_ref()
    }

    async fn block_receipts(&self, number: u64) -> Result<Vec<RpcReceipt>, ProviderError> {
        self.require_any_role(&[ProviderRole::Receipt, ProviderRole::Checkpoint])?;
        self.request_unscoped("eth_getBlockReceipts", json!([format_quantity(number)]))
            .await
    }

    async fn logs(
        &self,
        from: u64,
        to: u64,
        addresses: &[Address],
    ) -> Result<Vec<RpcLog>, ProviderError> {
        let mut filter = serde_json::Map::new();
        filter.insert("fromBlock".to_owned(), json!(format_quantity(from)));
        filter.insert("toBlock".to_owned(), json!(format_quantity(to)));
        if !addresses.is_empty() {
            filter.insert("address".to_owned(), json!(addresses));
        }
        self.request(
            ProviderRole::Logs,
            "eth_getLogs",
            Value::Array(vec![Value::Object(filter)]),
        )
        .await
    }

    async fn receipt_by_hash(&self, hash: B256) -> Result<Option<RpcReceipt>, ProviderError> {
        self.require_any_role(&[ProviderRole::Receipt, ProviderRole::Checkpoint])?;
        self.request_unscoped("eth_getTransactionReceipt", json!([hash]))
            .await
    }
}

#[async_trait]
impl TransactionSimulationProvider for HttpProvider {
    async fn call_at(
        &self,
        from: Address,
        target: Address,
        data: &Bytes,
        block: BlockRef,
    ) -> Result<Bytes, ProviderError> {
        self.request(
            ProviderRole::Simulate,
            "eth_call",
            json!([{
                "from": from,
                "to": target,
                "value": "0x0",
                "data": data,
            }, {
                "blockHash": block.hash,
                "requireCanonical": true,
            }]),
        )
        .await
    }

    async fn estimate_gas_at(
        &self,
        from: Address,
        target: Address,
        data: &Bytes,
        block: BlockRef,
    ) -> Result<u64, ProviderError> {
        let quantity: String = self
            .request(
                ProviderRole::Simulate,
                "eth_estimateGas",
                json!([{
                    "from": from,
                    "to": target,
                    "value": "0x0",
                    "data": data,
                }, format_quantity(block.number)]),
            )
            .await?;
        parse_quantity("eth_estimateGas", &quantity)
    }

    async fn using_big_blocks(&self, signer: Address) -> Result<bool, ProviderError> {
        HttpProvider::using_big_blocks(self, signer).await
    }
}

#[async_trait]
impl SignedTransactionSubmitter for HttpProvider {
    async fn submit_signed_bytes(&self, signed: &Bytes) -> Result<B256, ProviderError> {
        self.send_raw_transaction(signed).await
    }
}

#[derive(Serialize)]
struct RpcRequest {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: Value,
}

#[derive(Deserialize)]
struct RpcResponse {
    jsonrpc: String,
    id: u64,
    #[serde(default)]
    result: Value,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
}

fn build_client() -> Result<Client, ProviderError> {
    Client::builder()
        .connect_timeout(RPC_CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT)
        .build()
        .map_err(|_| ProviderError::Transport)
}

/// Parses an EVM hex quantity into `u64` without narrowing.
pub fn parse_quantity(field: &'static str, quantity: &str) -> Result<u64, ProviderError> {
    let digits = quantity
        .strip_prefix("0x")
        .ok_or(ProviderError::Quantity { field })?;
    if digits.is_empty() {
        return Err(ProviderError::Quantity { field });
    }
    u64::from_str_radix(digits, 16).map_err(|_| ProviderError::Quantity { field })
}

/// Formats an EVM hex quantity without leading zeroes.
#[must_use]
pub fn format_quantity(value: u64) -> String {
    format!("0x{value:x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantity_codec_is_canonical_and_checked() {
        assert_eq!(format_quantity(0), "0x0");
        assert_eq!(format_quantity(255), "0xff");
        assert_eq!(parse_quantity("test", "0xff"), Ok(255));
        assert!(parse_quantity("test", "ff").is_err());
        assert!(parse_quantity("test", "0x").is_err());
        assert!(parse_quantity("test", "0x10000000000000000").is_err());
    }
}
