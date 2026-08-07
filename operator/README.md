# Manual mainnet test actions

These scripts create independent user activity for observing the reallocator.
They are deliberately separate: a vault depositor and a Morpho borrower may use
different encrypted Foundry keystores. No script reads the reallocator's private
key or accepts a raw private key.

Requirements: Foundry `cast`, `jq`, a chain RPC, an encrypted keystore, and a
password file readable only by its owner. Import each test account once:

```bash
cast wallet import vault-depositor --interactive
cast wallet import market-borrower --interactive
chmod 600 /path/to/password-file
```

For every command, select the account and RPC explicitly:

```bash
export RPC_URL='https://your-chain-rpc'
export KEYSTORE="$HOME/.foundry/keystores/vault-depositor"
export PASSWORD_FILE='/secure/path/vault-depositor.password'
export BOT_CONFIG='/home/ubuntu/morpho-v2-reallocator/current/config.hyperevm.json'
```

The scripts reject an RPC whose chain ID differs from `BOT_CONFIG`, simulate the
exact call from the selected account, and only then send it. Amounts are human
token amounts, not base units.

Vault depositor actions:

```bash
./operator/deposit-vault.sh 10
./operator/withdraw-vault.sh 2.5
```

## Python private-key wallet

`vault_funds.py` provides the same vault actions when the depositor key must be
loaded from an environment file. Use a separate user wallet: the script refuses
the configured allocator address so it cannot interfere with the bot's nonce
lane. The key is never accepted as a command-line argument or printed.

Install its pinned dependency in a virtual environment:

```bash
python3 -m venv .venv-vault-funds
.venv-vault-funds/bin/pip install -r operator/requirements-vault-funds.txt
```

Add the RPC and separate depositor key to the repository `.env`, which is
gitignored:

```dotenv
HTTP_RPC_URL=https://your-chain-rpc
VAULT_USER_PRIVATE_KEY=0x...
```

Run without a command for an interactive prompt, or use one explicit action:

```bash
.venv-vault-funds/bin/python operator/vault_funds.py
.venv-vault-funds/bin/python operator/vault_funds.py status
.venv-vault-funds/bin/python operator/vault_funds.py deposit 100
.venv-vault-funds/bin/python operator/vault_funds.py withdraw 25
.venv-vault-funds/bin/python operator/vault_funds.py redeem-all
```

Every state-changing call is simulated first. The tool verifies the RPC chain
ID, vault bytecode, asset bytecode, `vault.asset()`, and token decimals before
signing. It can sign only an exact ERC-20 approval and the configured vault's
deposit, withdrawal, redemption, force-deallocation, or multicall functions.
Unless `--yes` is supplied, it requires typing `YES` before any transaction is
sent.

If an ordinary withdrawal simulation returns `NotEnoughLiquidity()`, the tool
selects only configured Morpho Market V1 adapter positions with live assets,
verifies their market IDs and runtime code, reads each adapter's live
force-deallocation penalty, and simulates one atomic vault multicall containing
the necessary `forceDeallocate` calls followed by the requested `withdraw`.
The confirmation prints the exact penalty before signing. Any failure reverts
the whole multicall; a partially prepared withdrawal is never left behind.

Vault V2 intentionally returns zero from the ERC-4626 `maxDeposit`, `maxMint`,
`maxWithdraw`, and `maxRedeem` views. The Python tool therefore validates owned
shares and simulates the exact state-changing call instead of using those max
views as availability checks.

Borrower actions use one active market ID shown by `list-markets.sh`. The
borrower must own that market's collateral token. Supply collateral before
borrowing; borrowing against the vault's liquidity without collateral is not
possible in Morpho.

```bash
./operator/list-markets.sh

export KEYSTORE="$HOME/.foundry/keystores/market-borrower"
export PASSWORD_FILE='/secure/path/market-borrower.password'
MARKET_ID='0x...'

./operator/supply-collateral.sh "$MARKET_ID" 1
./operator/borrow-market.sh "$MARKET_ID" 5
./operator/repay-market.sh "$MARKET_ID" 5.01
./operator/withdraw-collateral.sh "$MARKET_ID" 1
```

Use a conservative borrow amount. The `borrow-market.sh` simulation rejects an
undercollateralized borrow or a market without enough loan-token liquidity.
Repaying a fixed asset amount may leave interest dust; inspect the position and
repay the remaining amount before withdrawing all collateral.
