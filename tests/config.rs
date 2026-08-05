//! Representative and fail-closed configuration validation tests.
#![allow(clippy::panic)]

use std::path::PathBuf;

use alloy::primitives::B256;
use morpho_v2_reallocator::config::{
    AppConfig, ConfigError, RpcRole, RuntimeMode, SigningConfig, config_revision,
};

fn example_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.json")
}

fn raw_example() -> AppConfig {
    match AppConfig::load(&example_path()) {
        Ok(config) => config,
        Err(error) => panic!("checked-in representative configuration must parse: {error}"),
    }
}

fn assert_field(error: ConfigError, expected: &str) {
    match error {
        ConfigError::Validation { field, .. } => assert_eq!(field, expected),
        other => panic!("expected validation error for {expected}, got {other}"),
    }
}

#[test]
fn representative_configuration_validates_and_hashes_deterministically() {
    let first = match raw_example().validate() {
        Ok(config) => config,
        Err(error) => panic!("representative configuration must validate: {error}"),
    };
    let second = match raw_example().validate() {
        Ok(config) => config,
        Err(error) => panic!("representative configuration must validate twice: {error}"),
    };
    assert_ne!(config_revision(&first), B256::ZERO);
    assert_eq!(config_revision(&first), config_revision(&second));
}

#[test]
fn checked_in_hyperevm_vault_configuration_is_exact_and_complete() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.hyperevm.json");
    let validated = match AppConfig::load(&path).and_then(AppConfig::validate) {
        Ok(config) => config,
        Err(error) => panic!("HyperEVM configuration must validate: {error}"),
    };
    let vault = &validated.app.vaults[0];
    assert_eq!(validated.app.chain.chain_id, 999);
    assert_eq!(validated.app.strategy.immediate_tranche_bps, 5_000);
    assert_eq!(vault.positions.len(), 8);
    assert!(vault.liquidity_adapter.is_some());
}

#[test]
fn one_allocator_may_own_multiple_distinct_vaults() {
    let mut config = raw_example();
    let mut second = config.vault[0].clone();
    second.address = "0x0000000000000000000000000000000000000200".to_owned();
    assert_eq!(second.signer_address, config.vault[0].signer_address);
    config.vault.push(second);
    let validated = match config.validate() {
        Ok(validated) => validated,
        Err(error) => panic!("shared allocator configuration must validate: {error}"),
    };
    assert_eq!(validated.app.vaults.len(), 2);
    assert_eq!(
        validated.app.vaults[0].signer_address,
        validated.app.vaults[1].signer_address
    );
}

#[test]
fn unknown_field_is_rejected() {
    let text = match std::fs::read_to_string(example_path()) {
        Ok(text) => text,
        Err(error) => panic!("cannot read fixture: {error}"),
    };
    let modified = text.replacen(
        "\"schema_version\": 4,",
        "\"schema_version\": 4,\n  \"unknown\": true,",
        1,
    );
    assert!(serde_json::from_str::<AppConfig>(&modified).is_err());
}

#[test]
fn wrong_schema_is_rejected() {
    let mut config = raw_example();
    config.schema_version = 2;
    assert_field(
        match config.validate() {
            Ok(_) => panic!("wrong schema must fail"),
            Err(error) => error,
        },
        "schema_version",
    );
}

#[test]
fn invalid_strategy_bounds_are_rejected() {
    let mut config = raw_example();
    config.strategy.entry_spread_apr_bps = config.strategy.target_spread_apr_bps;
    assert_field(
        match config.validate() {
            Ok(_) => panic!("entry <= target must fail"),
            Err(error) => error,
        },
        "strategy.entry_spread_apr_bps",
    );

    let mut config = raw_example();
    config.strategy.immediate_tranche_bps = 0;
    assert_field(
        match config.validate() {
            Ok(_) => panic!("zero tranche must fail"),
            Err(error) => error,
        },
        "strategy.immediate_tranche_bps",
    );

    let mut config = raw_example();
    config.node.mode = RuntimeMode::Execute;
    config.strategy.extreme_spread_bypass_enabled = true;
    assert_field(
        match config.validate() {
            Ok(_) => panic!("execute bypass must fail"),
            Err(error) => error,
        },
        "node.mode",
    );
}

#[test]
fn gas_and_pending_horizons_are_rejected() {
    let mut config = raw_example();
    config.execution.maximum_signed_transaction_gas = config.chain.fast_block_gas_limit;
    assert_field(
        match config.validate() {
            Ok(_) => panic!("gas at fast limit must fail"),
            Err(error) => error,
        },
        "execution.maximum_signed_transaction_gas",
    );

    let mut config = raw_example();
    config.execution.maximum_inclusion_fast_blocks = 2;
    config.execution.maximum_rate_rebalance_pending_fast_blocks = 3;
    assert_field(
        match config.validate() {
            Ok(_) => panic!("long rate horizon must fail"),
            Err(error) => error,
        },
        "execution.maximum_rate_rebalance_pending_fast_blocks",
    );
}

#[test]
fn provider_roles_and_hyperevm_log_bound_are_rejected() {
    let mut config = raw_example();
    config.chain.rpc[0]
        .roles
        .retain(|role| *role != RpcRole::Receipt);
    assert_field(
        match config.validate() {
            Ok(_) => panic!("primary without receipt role must fail"),
            Err(error) => error,
        },
        "chain.rpc",
    );

    let mut config = raw_example();
    config.chain.rpc[1]
        .roles
        .retain(|role| *role != RpcRole::Checkpoint);
    assert_field(
        match config.validate() {
            Ok(_) => panic!("missing checkpoint must fail"),
            Err(error) => error,
        },
        "chain.rpc",
    );

    let mut config = raw_example();
    config.chain.maximum_log_range = 51;
    assert_field(
        match config.validate() {
            Ok(_) => panic!("official log range above 50 must fail"),
            Err(error) => error,
        },
        "chain",
    );

    let mut config = raw_example();
    config.chain.rpc[0].websocket_url_env = None;
    assert_field(
        match config.validate() {
            Ok(_) => panic!("declared WebSocket support without an endpoint must fail"),
            Err(error) => error,
        },
        "chain.rpc",
    );
}

#[test]
fn vault_headroom_bounds_are_rejected() {
    let mut config = raw_example();
    config.vault[0].deposit_headroom_search_upper_bound_assets = "0".to_owned();
    assert_field(
        match config.validate() {
            Ok(_) => panic!("missing headroom bound must fail"),
            Err(error) => error,
        },
        "vault.deposit_headroom_search_upper_bound_assets",
    );
}

#[test]
fn signer_and_rate_group_invariants_are_rejected() {
    let mut config = raw_example();
    config.signing = SigningConfig::LocalDevelopment {
        private_key_env: " ".to_owned(),
    };
    assert_field(
        match config.validate() {
            Ok(_) => panic!("blank signer environment name must fail"),
            Err(error) => error,
        },
        "signing",
    );

    let mut config = raw_example();
    config.vault[0].approved_allocators.clear();
    assert_field(
        match config.validate() {
            Ok(_) => panic!("unapproved signer must fail"),
            Err(error) => error,
        },
        "vault.signer_address",
    );

    let mut config = raw_example();
    let duplicate = config.vault[0].rate_group[0].clone();
    config.vault[0].rate_group.push(duplicate);
    assert_field(
        match config.validate() {
            Ok(_) => panic!("two groups must fail"),
            Err(error) => error,
        },
        "vault.rate_group",
    );
}

#[test]
fn local_development_execute_is_rejected_on_hyperevm_mainnet() {
    let mut config = raw_example();
    config.node.mode = RuntimeMode::Execute;
    config.signing = SigningConfig::LocalDevelopment {
        private_key_env: "TEST_PRIVATE_KEY".to_owned(),
    };
    assert_field(
        match config.validate() {
            Ok(_) => panic!("mainnet local signer must fail"),
            Err(error) => error,
        },
        "signing",
    );
}

#[test]
fn market_identity_and_hysteresis_are_rejected() {
    let mut config = raw_example();
    config.vault[0].position[0].market_id =
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned();
    assert_field(
        match config.validate() {
            Ok(_) => panic!("wrong market ID must fail"),
            Err(error) => error,
        },
        "vault.position.market_id",
    );

    let mut config = raw_example();
    config.vault[0].position[0].minimum_relevance_entry_assets = "1000000".to_owned();
    assert_field(
        match config.validate() {
            Ok(_) => panic!("invalid hysteresis must fail"),
            Err(error) => error,
        },
        "vault.position.minimum_relevance_entry_assets",
    );
}

#[test]
fn expired_reward_evidence_is_rejected() {
    let mut config = raw_example();
    config.vault[0].position[0].reward_policy =
        morpho_v2_reallocator::domain::RewardPolicy::NoMaterialRewards {
            checked_at_block: 1,
            valid_until_timestamp: 1,
            evidence_hash: B256::repeat_byte(1),
        };
    assert_field(
        match config.validate() {
            Ok(_) => panic!("expired rewards must fail"),
            Err(error) => error,
        },
        "vault.position.reward_policy.valid_until_timestamp",
    );
}
