//! HTTP provider capability and role-boundary tests.
#![allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
#![allow(clippy::panic, clippy::unwrap_used)]

use std::collections::BTreeSet;

use alloy::primitives::{Address, B256, Bytes};
use morpho_v2_reallocator::chain::provider::{
    CapabilityProbe, ChainDataProvider, FeeQuoteProvider, HttpProvider, ProviderError,
    ProviderRole, RpcErrorCategory,
};
use morpho_v2_reallocator::config::BlockOpportunityPolicy;
use serde_json::{Value, json};
use url::Url;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate, matchers::method};

#[derive(Clone)]
struct RpcResponder;

impl Respond for RpcResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        let id = body.get("id").cloned().unwrap_or(json!(1));
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let result =
            match method {
                "eth_chainId" => json!("0x3e7"),
                "eth_getBlockByNumber" => json!({
                    "number": "0xa",
                    "hash": B256::repeat_byte(0x0a),
                    "parentHash": B256::repeat_byte(0x09),
                    "timestamp": "0x64",
                    "gasLimit": "0x989680"
                }),
                "eth_getLogs" | "eth_getBlockReceipts" => json!([]),
                "eth_call" => json!("0x"),
                "eth_estimateGas" => json!("0x5208"),
                "eth_getCode" => json!("0x6000"),
                "eth_getStorageAt" => json!(B256::ZERO),
                "eth_getTransactionCount" => json!("0x7"),
                "eth_gasPrice" => json!("0x5f5e100"),
                "eth_maxPriorityFeePerGas" => json!("0x0"),
                "eth_getTransactionByHash" | "eth_getTransactionReceipt" => Value::Null,
                "eth_usingBigBlocks" => json!(false),
                _ => return ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": "missing" }
                })),
            };
        ResponseTemplate::new(200)
            .set_body_json(json!({ "jsonrpc": "2.0", "id": id, "result": result }))
    }
}

#[derive(Clone)]
struct SubmissionErrorResponder {
    message: &'static str,
}

impl Respond for SubmissionErrorResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        let id = body.get("id").cloned().unwrap_or(json!(1));
        ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": -32000, "message": self.message }
        }))
    }
}

fn all_roles() -> BTreeSet<ProviderRole> {
    BTreeSet::from([
        ProviderRole::Head,
        ProviderRole::Logs,
        ProviderRole::Read,
        ProviderRole::Simulate,
        ProviderRole::Submit,
        ProviderRole::Receipt,
    ])
}

#[tokio::test]
async fn capability_probe_covers_required_methods_with_one_latest_header_call()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(RpcResponder)
        .mount(&server)
        .await;
    let provider = HttpProvider::new("test".to_owned(), Url::parse(&server.uri())?, all_roles())?;
    let capabilities = provider
        .probe_capabilities(
            999,
            &CapabilityProbe {
                read_target: Address::with_last_byte(1),
                read_calldata: Bytes::from_static(&[0x12, 0x34, 0x56, 0x78]),
                signer: Address::with_last_byte(2),
                known_transaction_hash: B256::repeat_byte(3),
            },
            BlockOpportunityPolicy::HyperEvmFastBlocks {
                gas_limit: 2_000_000,
            },
        )
        .await?;
    assert_eq!(capabilities.chain_id, 999);
    assert_eq!(capabilities.latest_head.number, 10);
    assert_eq!(capabilities.signer_uses_big_blocks, Some(false));

    let requests = server.received_requests().await.unwrap();
    let methods = requests
        .iter()
        .filter_map(|request| serde_json::from_slice::<Value>(&request.body).ok())
        .filter_map(|body| {
            body.get("method")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();
    for required in [
        "eth_chainId",
        "eth_getBlockByNumber",
        "eth_getBlockReceipts",
        "eth_getLogs",
        "eth_call",
        "eth_estimateGas",
        "eth_getCode",
        "eth_getStorageAt",
        "eth_getTransactionCount",
        "eth_gasPrice",
        "eth_maxPriorityFeePerGas",
        "eth_getTransactionByHash",
        "eth_getTransactionReceipt",
        "eth_usingBigBlocks",
    ] {
        assert!(methods.iter().any(|method| method == required));
    }
    assert_eq!(
        methods
            .iter()
            .filter(|method| method.as_str() == "eth_getBlockByNumber")
            .count(),
        1
    );
    assert!(!methods.iter().any(|method| method == "eth_blockNumber"));
    let quote = provider.fee_quote().await?;
    assert_eq!(
        quote.gas_price,
        alloy::primitives::U256::from(100_000_000_u64)
    );
    assert_eq!(
        quote.max_priority_fee_per_gas,
        alloy::primitives::U256::ZERO
    );
    Ok(())
}

#[tokio::test]
async fn standard_evm_capability_probe_skips_hyperevm_only_rpc()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(RpcResponder)
        .mount(&server)
        .await;
    let provider = HttpProvider::new("test".to_owned(), Url::parse(&server.uri())?, all_roles())?;
    let capabilities = provider
        .probe_capabilities(
            999,
            &CapabilityProbe {
                read_target: Address::with_last_byte(1),
                read_calldata: Bytes::from_static(&[0x12, 0x34, 0x56, 0x78]),
                signer: Address::with_last_byte(2),
                known_transaction_hash: B256::repeat_byte(3),
            },
            BlockOpportunityPolicy::EveryCanonicalBlock,
        )
        .await?;
    assert_eq!(capabilities.signer_uses_big_blocks, None);

    let requests = server.received_requests().await.unwrap();
    let methods = requests
        .iter()
        .filter_map(|request| serde_json::from_slice::<Value>(&request.body).ok())
        .filter_map(|body| {
            body.get("method")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();
    assert!(!methods.iter().any(|method| method == "eth_usingBigBlocks"));
    Ok(())
}

#[tokio::test]
async fn runtime_methods_enforce_roles_and_accept_null_receipts()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(RpcResponder)
        .mount(&server)
        .await;
    let receipt_only = HttpProvider::new(
        "receipt".to_owned(),
        Url::parse(&server.uri())?,
        BTreeSet::from([ProviderRole::Receipt]),
    )?;
    assert_eq!(receipt_only.receipt_by_hash(B256::ZERO).await?, None);
    assert!(matches!(
        receipt_only.latest_header().await,
        Err(ProviderError::MissingRole {
            role: ProviderRole::Head,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn forbidden_optional_block_receipts_are_treated_as_unsupported()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;
    let provider = HttpProvider::new(
        "restricted-provider".to_owned(),
        Url::parse(&server.uri())?,
        BTreeSet::from([ProviderRole::Receipt]),
    )?;
    assert!(matches!(
        provider.block_receipts(1).await,
        Err(ProviderError::MethodUnsupported {
            method: "eth_getBlockReceipts"
        })
    ));
    assert!(matches!(
        provider.block_receipts(2).await,
        Err(ProviderError::MethodUnsupported {
            method: "eth_getBlockReceipts"
        })
    ));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    Ok(())
}

#[tokio::test]
async fn submission_errors_are_sanitized_into_recovery_categories()
-> Result<(), Box<dyn std::error::Error>> {
    for (message, expected) in [
        ("already known", RpcErrorCategory::AlreadyKnown),
        ("nonce too low", RpcErrorCategory::NonceTooLow),
        (
            "replacement transaction underpriced",
            RpcErrorCategory::ReplacementUnderpriced,
        ),
        (
            "insufficient funds for gas * price + value",
            RpcErrorCategory::InsufficientFunds,
        ),
        ("invalid sender", RpcErrorCategory::InvalidSenderOrEncoding),
        ("provider-specific rejection", RpcErrorCategory::Unknown),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(SubmissionErrorResponder { message })
            .mount(&server)
            .await;
        let provider = HttpProvider::new(
            "submission".to_owned(),
            Url::parse(&server.uri())?,
            BTreeSet::from([ProviderRole::Submit]),
        )?;
        let error = match provider
            .send_raw_transaction(&Bytes::from_static(&[0x02, 0x01]))
            .await
        {
            Ok(_) => panic!("mock submission must fail"),
            Err(error) => error,
        };
        assert_eq!(error.rpc_category(), expected);
        assert!(!error.to_string().contains(message));
    }
    Ok(())
}
