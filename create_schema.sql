/**
 * This plugin implementation for PostgreSQL requires the following tables
 */
-- The table storing accounts


CREATE TABLE public.account (
    pubkey BYTEA,
    owner BYTEA,
    lamports BIGINT NOT NULL,
    slot BIGINT NOT NULL,
    executable BOOL NOT NULL,
    rent_epoch BIGINT NOT NULL,
    data BYTEA,
    write_version BIGINT NOT NULL,
    updated_on TIMESTAMP NOT NULL,
    txn_signature BYTEA
);

CREATE INDEX account_txn_signature_slot ON public.account(txn_signature, slot);

-- The table storing slot information
CREATE TABLE public.slot (
    slot BIGINT PRIMARY KEY,
    parent BIGINT,
    status VARCHAR(16) NOT NULL,
    updated_on TIMESTAMP
);

INSERT INTO public.slot(slot, parent, status)
VALUES (0, NULL, 'rooted');

-- Types for Transactions

Create TYPE "TransactionErrorCode" AS ENUM (
    'AccountInUse',
    'AccountLoadedTwice',
    'AccountNotFound',
    'ProgramAccountNotFound',
    'InsufficientFundsForFee',
    'InvalidAccountForFee',
    'AlreadyProcessed',
    'BlockhashNotFound',
    'InstructionError',
    'CallChainTooDeep',
    'MissingSignatureForFee',
    'InvalidAccountIndex',
    'SignatureFailure',
    'InvalidProgramForExecution',
    'SanitizeFailure',
    'ClusterMaintenance',
    'AccountBorrowOutstanding',
    'WouldExceedMaxAccountCostLimit',
    'WouldExceedMaxBlockCostLimit',
    'UnsupportedVersion',
    'InvalidWritableAccount',
    'WouldExceedMaxAccountDataCostLimit',
    'TooManyAccountLocks',
    'AddressLookupTableNotFound',
    'InvalidAddressLookupTableOwner',
    'InvalidAddressLookupTableData',
    'InvalidAddressLookupTableIndex',
    'InvalidRentPayingAccount',
    'WouldExceedMaxVoteCostLimit',
    'WouldExceedAccountDataBlockLimit',
    'WouldExceedAccountDataTotalLimit',
    'DuplicateInstruction',
    'InsufficientFundsForRent'
);

CREATE TYPE "TransactionError" AS (
    error_code "TransactionErrorCode",
    error_detail VARCHAR(256)
);

CREATE TYPE "CompiledInstruction" AS (
    program_id_index SMALLINT,
    accounts SMALLINT[],
    data BYTEA
);

CREATE TYPE "InnerInstructions" AS (
    index SMALLINT,
    instructions "CompiledInstruction"[]
);

CREATE TYPE "TransactionTokenBalance" AS (
    account_index SMALLINT,
    mint VARCHAR(44),
    ui_token_amount DOUBLE PRECISION,
    owner VARCHAR(44)
);

Create TYPE "RewardType" AS ENUM (
    'Fee',
    'Rent',
    'Staking',
    'Voting'
);

CREATE TYPE "Reward" AS (
    pubkey VARCHAR(44),
    lamports BIGINT,
    post_balance BIGINT,
    reward_type "RewardType",
    commission SMALLINT
);

CREATE TYPE "TransactionStatusMeta" AS (
    error "TransactionError",
    fee BIGINT,
    pre_balances BIGINT[],
    post_balances BIGINT[],
    inner_instructions "InnerInstructions"[],
    log_messages TEXT[],
    pre_token_balances "TransactionTokenBalance"[],
    post_token_balances "TransactionTokenBalance"[],
    rewards "Reward"[]
);

CREATE TYPE "TransactionMessageHeader" AS (
    num_required_signatures SMALLINT,
    num_readonly_signed_accounts SMALLINT,
    num_readonly_unsigned_accounts SMALLINT
);

CREATE TYPE "TransactionMessage" AS (
    header "TransactionMessageHeader",
    account_keys BYTEA[],
    recent_blockhash BYTEA,
    instructions "CompiledInstruction"[]
);

CREATE TYPE "TransactionMessageAddressTableLookup" AS (
    account_key BYTEA,
    writable_indexes SMALLINT[],
    readonly_indexes SMALLINT[]
);

CREATE TYPE "TransactionMessageV0" AS (
    header "TransactionMessageHeader",
    account_keys BYTEA[],
    recent_blockhash BYTEA,
    instructions "CompiledInstruction"[],
    address_table_lookups "TransactionMessageAddressTableLookup"[]
);

CREATE TYPE "LoadedAddresses" AS (
    writable BYTEA[],
    readonly BYTEA[]
);

CREATE TYPE "LoadedMessageV0" AS (
    message "TransactionMessageV0",
    loaded_addresses "LoadedAddresses"
);

-- The table storing transactions
CREATE TABLE public.transaction (
    slot BIGINT NOT NULL,
    signature BYTEA NOT NULL,
    is_vote BOOL NOT NULL,
    message_type SMALLINT, -- 0: legacy, 1: v0 message
    legacy_message "TransactionMessage",
    v0_loaded_message "LoadedMessageV0",
    signatures BYTEA[],
    message_hash BYTEA,
    meta "TransactionStatusMeta",
    write_version BIGINT,
    updated_on TIMESTAMP NOT NULL,
    CONSTRAINT transaction_pk PRIMARY KEY (slot, signature)
);

CREATE INDEX transaction_signature ON public.transaction (signature);

-- The table storing block metadata
CREATE TABLE public.block (
    slot BIGINT PRIMARY KEY,
    blockhash VARCHAR(44),
    rewards "Reward"[],
    block_time BIGINT,
    block_height BIGINT,
    updated_on TIMESTAMP NOT NULL
);

-- The table storing spl token owner to account indexes
CREATE TABLE public.spl_token_owner_index (
    owner_key BYTEA NOT NULL,
    account_key BYTEA NOT NULL,
    slot BIGINT NOT NULL
);

CREATE INDEX spl_token_owner_index_owner_key ON public.spl_token_owner_index (owner_key);
CREATE UNIQUE INDEX spl_token_owner_index_owner_pair ON public.spl_token_owner_index (owner_key, account_key);

-- The table storing spl mint to account indexes
CREATE TABLE public.spl_token_mint_index (
    mint_key BYTEA NOT NULL,
    account_key BYTEA NOT NULL,
    slot BIGINT NOT NULL
);

CREATE INDEX spl_token_mint_index_mint_key ON public.spl_token_mint_index (mint_key);
CREATE UNIQUE INDEX spl_token_mint_index_mint_pair ON public.spl_token_mint_index (mint_key, account_key);

-- Table storing older state of all accounts
-- History looks like:
-- >>>>>>>>>>>>>>>>>>>>>>>>>TIME>>>>>>>>>>>>>>>>>>>>>>>>
-- older_account >>> account_audit >>> account
CREATE TABLE public.older_account (
    pubkey BYTEA PRIMARY KEY,
    owner BYTEA,
    lamports BIGINT NOT NULL,
    slot BIGINT NOT NULL,
    executable BOOL NOT NULL,
    rent_epoch BIGINT NOT NULL,
    data BYTEA,
    write_version BIGINT NOT NULL,
    updated_on TIMESTAMP NOT NULL,
    txn_signature BYTEA
);

-- Historical data for accounts
-- This is partitioned table
CREATE TABLE public.account_audit (
    pubkey BYTEA,
    owner BYTEA,
    lamports BIGINT NOT NULL,
    slot BIGINT NOT NULL,
    executable BOOL NOT NULL,
    rent_epoch BIGINT NOT NULL,
    data BYTEA,
    write_version BIGINT NOT NULL,
    updated_on TIMESTAMP NOT NULL,
    txn_signature BYTEA
) PARTITION BY RANGE (slot);

CREATE INDEX account_audit_pubkey_slot_wv ON  public.account_audit (pubkey, slot, write_version);
CREATE INDEX account_audit_txn_signature ON public.account_audit (txn_signature);
CREATE INDEX account_audit_slot ON public.account_audit (slot);
