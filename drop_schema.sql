/**
 * Script for cleaning up the schema for PostgreSQL used for the AccountsDb plugin.
 */

DROP TRIGGER account_update_trigger ON account;
DROP FUNCTION audit_account_update;
DROP TABLE account_audit;
DROP TABLE account CASCADE;
DROP TABLE slot;
DROP TABLE transaction;
DROP TABLE block;
DROP TABLE spl_token_owner_index;
DROP TABLE spl_token_mint_index;
DROP TABLE older_account;

DROP TYPE "TransactionError" CASCADE;
DROP TYPE "TransactionErrorCode" CASCADE;
DROP TYPE "LoadedMessageV0" CASCADE;
DROP TYPE "LoadedAddresses" CASCADE;
DROP TYPE "TransactionMessageV0" CASCADE;
DROP TYPE "TransactionMessage" CASCADE;
DROP TYPE "TransactionMessageHeader" CASCADE;
DROP TYPE "TransactionMessageAddressTableLookup" CASCADE;
DROP TYPE "TransactionStatusMeta" CASCADE;
DROP TYPE "RewardType" CASCADE;
DROP TYPE "Reward" CASCADE;
DROP TYPE "TransactionTokenBalance" CASCADE;
DROP TYPE "InnerInstructions" CASCADE;
DROP TYPE "CompiledInstruction" CASCADE;

DROP FUNCTION update_older_account;
DROP FUNCTION get_account_at_slot;
DROP FUNCTION get_pre_accounts;
DROP FUNCTION get_pre_accounts_root;
DROP FUNCTION get_pre_accounts_branch;
DROP FUNCTION get_pre_accounts_one_slot;
DROP FUNCTION find_slot_on_longest_branch;
