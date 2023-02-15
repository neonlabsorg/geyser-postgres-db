
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION find_slot_on_longest_branch(transaction_slots BIGINT[])
RETURNS BIGINT
AS $find_slot_on_longest_branch$
DECLARE
    current_slot BIGINT := NULL;
    current_slot_status VARCHAR := NULL;
    num_in_txn_slots INT := 0;
BEGIN
    -- start from topmost slot
    SELECT s.slot
    INTO current_slot
    FROM public.slot AS s
    ORDER BY s.slot DESC LIMIT 1;
  
    LOOP
        -- get status of current slot
        SELECT s.status
        INTO current_slot_status
        FROM public.slot AS s
        WHERE s.slot = current_slot;
    
        -- already on rooted slot - stop iteration
        IF current_slot_status = 'rooted' THEN
            RETURN NULL;
        END IF;
    
        -- does current slot contain transaction ?
        SELECT COUNT(*)
        INTO num_in_txn_slots
        FROM unnest(transaction_slots) AS slot
        WHERE slot = current_slot;
    
        -- if yes - it means we found slot with txn
        -- on the longest branch - return it
        IF num_in_txn_slots <> 0 THEN
            RETURN current_slot;
        END IF;
    
        -- If no - go further into the past - select parent slot
        SELECT s.parent
        INTO current_slot
        FROM public.slot AS s
        WHERE s.slot = current_slot;
    END LOOP;
END;
$find_slot_on_longest_branch$ LANGUAGE plpgsql;


-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION find_txn_slot_on_longest_branch(in_txn_signature BYTEA)
RETURNS BIGINT
AS $find_txn_slot_on_longest_branch$
DECLARE
    transaction_slots BIGINT[];
    current_slot BIGINT := NULL;
    current_slot_status VARCHAR := NULL;
    num_in_txn_slots INT := 0;
BEGIN
    -- find all occurencies of transaction in slots
    SELECT array_agg(txn.slot)
    INTO transaction_slots
    FROM public.transaction AS txn
    WHERE position(in_txn_signature in txn.signature) > 0;

    -- try to find slot that was rooted with given transaction
    SELECT txn_slot INTO current_slot
    FROM unnest(transaction_slots) AS txn_slot
         INNER JOIN public.slot AS s
                    ON txn_slot = s.slot
    WHERE s.status = 'rooted'
    LIMIT 1;

    IF current_slot IS NOT NULL THEN
        RETURN current_slot;
    END IF;

    -- start from topmost slot
    SELECT s.slot
    INTO current_slot
    FROM public.slot AS s
    ORDER BY s.slot DESC LIMIT 1;

    LOOP
        -- get status of current slot
        SELECT s.status
        INTO current_slot_status
        FROM public.slot AS s
        WHERE s.slot = current_slot;

        -- already on rooted slot - stop iteration
        IF current_slot_status = 'rooted' THEN
            RETURN NULL;
        END IF;

        -- does current slot contain transaction ?
        SELECT COUNT(*)
        INTO num_in_txn_slots
        FROM unnest(transaction_slots) AS slot
        WHERE slot = current_slot;

        -- if yes - it means we found slot with txn
        -- on the longest branch - return it
        IF num_in_txn_slots <> 0 THEN
            RETURN current_slot;
        END IF;

        -- If no - go further into the past - select parent slot
        SELECT s.parent
        INTO current_slot
        FROM public.slot AS s
        WHERE s.slot = current_slot;
    END LOOP;
END;
$find_txn_slot_on_longest_branch$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
-- Returns pre-accounts data for given transaction on a given slot
CREATE OR REPLACE FUNCTION get_pre_accounts_one_slot(
    current_slot BIGINT,
    max_write_version BIGINT,
    transaction_accounts BYTEA[]
)

RETURNS TABLE (
    lamports BIGINT,
    data BYTEA,
    owner BYTEA,
    executable BOOL,
    rent_epoch BIGINT,
    pubkey BYTEA,
    slot BIGINT,
    write_version BIGINT,
    signature BYTEA
)

AS $get_pre_accounts_one_slot$

BEGIN
    RETURN QUERY
        SELECT DISTINCT ON (acc.pubkey)
            acc.lamports,
            acc.data,
            acc.owner,
            acc.executable,
            acc.rent_epoch,
            acc.pubkey,
            acc.slot,
            acc.write_version,
            acc.txn_signature
        FROM public.account_audit AS acc
        WHERE
            acc.slot = current_slot
            AND acc.write_version < max_write_version
            AND acc.pubkey IN (SELECT * FROM unnest(transaction_accounts))
        ORDER BY
            acc.pubkey,
            acc.write_version DESC;
END;
$get_pre_accounts_one_slot$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_pre_accounts_branch(
    start_slot BIGINT,
    max_write_version BIGINT,
    transaction_accounts BYTEA[]
)
  
RETURNS TABLE (
    lamports BIGINT,
    data BYTEA,
    owner BYTEA,
    executable BOOL,
    rent_epoch BIGINT,
    pubkey BYTEA,
    slot BIGINT,
    write_version BIGINT,
    signature BYTEA
)

AS $get_pre_accounts_branch$

DECLARE
    branch_slots BIGINT[];

BEGIN   
    WITH RECURSIVE parents AS (
        SELECT
            first.slot,
            first.parent,
            first.status
        FROM public.slot AS first
        WHERE first.slot = start_slot and first.status <> 'rooted'
        UNION
            SELECT
                next.slot,
                next.parent,
                next.status
            FROM public.slot AS next
            INNER JOIN parents p ON p.parent = next.slot
            WHERE next.status <> 'rooted'
    )
    SELECT array_agg(prnts.slot)
    INTO branch_slots
    FROM parents AS prnts;
   
    RETURN QUERY
        SELECT DISTINCT ON (slot_results.pubkey)
            slot_results.lamports,
            slot_results.data,
            slot_results.owner,
            slot_results.executable,
            slot_results.rent_epoch,
            slot_results.pubkey,
            slot_results.slot,
            slot_results.write_version,
            slot_results.signature
        FROM
            unnest(branch_slots) AS current_slot,
            get_pre_accounts_one_slot(
                current_slot, 
                max_write_version, 
                transaction_accounts
            ) AS slot_results
        ORDER BY
            slot_results.pubkey,
            slot_results.slot DESC,
            slot_results.write_version DESC;
END;
$get_pre_accounts_branch$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
-- Returns latest versions of accounts with pubkeys included in @transaction_accounts
-- with update events in slots not much than @start_slot
-- and write versions not much than @max_write_version
-- Search is performed over account_audit table (latest written history)
CREATE OR REPLACE FUNCTION get_pre_audit_accounts(
    start_slot BIGINT,
    max_write_version BIGINT,
    transaction_accounts BYTEA[]
)

RETURNS TABLE (
    lamports BIGINT,
    data BYTEA,
    owner BYTEA,
    executable BOOL,
    rent_epoch BIGINT,
    pubkey BYTEA,
    slot BIGINT,
    write_version BIGINT,
    signature BYTEA
)

AS $get_pre_audit_accounts$

BEGIN
    RETURN QUERY
        SELECT DISTINCT ON (acc.pubkey)
            acc.lamports,
            acc.data,
            acc.owner,
            acc.executable,
            acc.rent_epoch,
            acc.pubkey,
            acc.slot,
            acc.write_version,
            acc.txn_signature
        FROM public.account_audit AS acc
        WHERE
            acc.pubkey IN (SELECT * FROM unnest(transaction_accounts))
            AND acc.slot <= start_slot
            AND acc.write_version < max_write_version
        ORDER BY
            acc.pubkey DESC,
            acc.slot DESC,
            acc.write_version DESC;
END;
$get_pre_audit_accounts$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
-- Returns latest versions of accounts with pubkeys included in @transaction_accounts
-- with update events in slots not much than @start_slot
-- and write versions not much than @max_write_version
-- Search is performed over older_account table (oldest versions)
CREATE OR REPLACE FUNCTION get_pre_older_accounts(
    start_slot BIGINT,
    max_write_version BIGINT,
    transaction_accounts BYTEA[]
)

RETURNS TABLE (
    lamports BIGINT,
    data BYTEA,
    owner BYTEA,
    executable BOOL,
    rent_epoch BIGINT,
    pubkey BYTEA,
    slot BIGINT,
    write_version BIGINT,
    signature BYTEA
)

AS $get_pre_older_accounts$

BEGIN
    RETURN QUERY
        SELECT
            old.lamports,
            old.data,
            old.owner,
            old.executable,
            old.rent_epoch,
            old.pubkey,
            old.slot,
            old.write_version,
            old.txn_signature
        FROM public.older_account AS old
        WHERE
            old.pubkey IN (SELECT * FROM unnest(transaction_accounts))
            AND old.slot <= start_slot
            AND old.write_version < max_write_version;
END;
$get_pre_older_accounts$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_pre_accounts_root(
    start_slot BIGINT,
    max_write_version BIGINT,
    transaction_accounts BYTEA[]
)

RETURNS TABLE (
    lamports BIGINT,
    data BYTEA,
    owner BYTEA,
    executable BOOL,
    rent_epoch BIGINT,
    pubkey BYTEA,
    slot BIGINT,
    write_version BIGINT,
    signature BYTEA
)

AS $get_pre_accounts_root$

BEGIN
    RETURN QUERY
        -- root branch consist of historical states in account_audit 
        -- plus olderst available state in older_accounts - collect from both
        -- and take only latest for each account from transaction_accounts
        WITH results AS (
            SELECT * FROM get_pre_audit_accounts(start_slot, max_write_version, transaction_accounts)
            UNION
            SELECT * FROM get_pre_older_accounts(start_slot, max_write_version, transaction_accounts)
        )
        SELECT DISTINCT ON (res.pubkey) * FROM results AS res 
        ORDER BY
            res.pubkey DESC,
            res.slot DESC,
            res.write_version DESC;
END;
$get_pre_accounts_root$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_pre_accounts(
    in_txn_signature BYTEA,
    transaction_accounts BYTEA[]
)
RETURNS TABLE (
    lamports BIGINT,
    data BYTEA,
    owner BYTEA,
    executable BOOL,
    rent_epoch BIGINT,
    pubkey BYTEA,
    slot BIGINT,
    write_version BIGINT,
    signature BYTEA
)

AS $get_pre_accounts$

DECLARE
    current_slot BIGINT;
    max_write_version BIGINT := NULL;
    transaction_slots BIGINT[];
    first_rooted_slot BIGINT;
   
BEGIN                    
    -- Query minimum write version of account update
    SELECT MIN(acc.write_version)
    INTO max_write_version
    FROM public.account_audit AS acc
    WHERE position(in_txn_signature in acc.txn_signature) > 0;
  
    -- find all occurencies of transaction in slots
    SELECT array_agg(txn.slot)
    INTO transaction_slots
    FROM public.transaction AS txn
    WHERE position(in_txn_signature in txn.signature) > 0;
  
    -- Query first rooted slot
    SELECT sl.slot
    INTO first_rooted_slot
    FROM public.slot AS sl
    WHERE sl.status = 'rooted'
    ORDER BY sl.slot DESC
    LIMIT 1;
  
    -- try to find slot that was rooted with given transaction
    SELECT txn_slot INTO current_slot
    FROM unnest(transaction_slots) AS txn_slot
    INNER JOIN public.slot AS s
    ON txn_slot = s.slot
    WHERE s.status = 'rooted'
    LIMIT 1;
  
    IF current_slot IS NULL THEN
        -- No rooted slot found. It means transaction exist on some not finalized branch.
        -- Try to find it on the longest one (search from topmost slot down to first rooted slot)
        SELECT find_slot_on_longest_branch(transaction_slots) INTO current_slot;
        IF current_slot IS NULL THEN
            -- Transaction not found on the longest branch - it exist somewhere on minor forks.
            -- Return empty list of accounts
            RETURN;
        ELSE
            -- Transaction found on the longest branch. 
            RETURN QUERY
                WITH results AS (
                    -- Start searching recent states of accounts in this branch
                    -- down to first rooted slot 
                    -- (this search algorithm iterates over parent slots and is slow).
                    SELECT * FROM get_pre_accounts_branch(
                        current_slot,
                        max_write_version,
                        transaction_accounts
                    )
                    UNION
                    -- Then apply fast search algorithm over rooted slots 
                    -- to obtain the rest of pre-accounts  
                    SELECT * FROM get_pre_accounts_root(
                        first_rooted_slot,
                        max_write_version,
                        transaction_accounts
                    )
                )
                SELECT DISTINCT ON (res.pubkey)
                    res.lamports,
                    res.data,
                    res.owner,
                    res.executable,
                    res.rent_epoch,
                    res.pubkey,
                    res.slot,
                    res.write_version,
                    res.signature
                FROM results AS res
                ORDER BY
                    res.pubkey,
                    res.slot DESC,
                    res.write_version DESC;
        END IF;
    ELSE
        -- Transaction found on the rooted slot.
        RETURN QUERY
            SELECT * FROM get_pre_accounts_root(
                current_slot,
                max_write_version,
                transaction_accounts
            );
    END IF;
END;
$get_pre_accounts$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_account_at_slot_from_audit(
    in_pubkey BYTEA,
    in_slot BIGINT
)

RETURNS TABLE (
    pubkey BYTEA,
	owner BYTEA,
	lamports BIGINT,
	executable BOOL,
	rent_epoch BIGINT,
	data BYTEA,
    slot BIGINT,
    write_version BIGINT,
    signature BYTEA
)

AS $get_account_at_slot_from_audit$

BEGIN
    RETURN QUERY
        SELECT DISTINCT ON (acc.pubkey)
            acc.pubkey,
            acc.owner,
            acc.lamports,
            acc.executable,
            acc.rent_epoch,
            acc.data,
            acc.slot,
            acc.write_version,
            acc.txn_signature
        FROM public.account_audit AS acc
        WHERE
            acc.slot <= in_slot
            AND acc.pubkey = in_pubkey
        ORDER BY 
            acc.pubkey DESC, 
            acc.write_version DESC 
        LIMIT 1;
END;
$get_account_at_slot_from_audit$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_account_at_slot(
    in_pubkey BYTEA,
    in_slot BIGINT
)

RETURNS TABLE (
    pubkey BYTEA,
	owner BYTEA,
	lamports BIGINT,
	executable BOOL,
	rent_epoch BIGINT,
	data BYTEA,
    slot BIGINT,
    write_version BIGINT,
    signature BYTEA
)

AS $get_account_at_slot$

BEGIN
    RETURN QUERY
        WITH results AS (
            SELECT * FROM get_account_at_slot_from_audit(in_pubkey, in_slot)
            UNION
            SELECT
                old.pubkey,
                old.owner,
                old.lamports,
                old.executable,
                old.rent_epoch,
                old.data,
                old.slot,
                old.write_version,
                old.txn_signature
            FROM public.older_account AS old
            WHERE
                old.pubkey = in_pubkey
        )
        SELECT * FROM results AS res
        ORDER BY res.slot DESC, res.write_version DESC 
        LIMIT 1;
END;
$get_account_at_slot$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE update_older_account(
    until_slot BIGINT
)

AS $update_older_account$

BEGIN
    -- add recent states of all accounts from account_audit
    -- before slot until_slot into older_account table
    INSERT INTO public.older_account
    SELECT
        acc1.pubkey,
        acc1.owner,
        acc1.lamports,
        acc1.slot,
        acc1.executable,
        acc1.rent_epoch,
        acc1.data,
        acc1.write_version,
        acc1.updated_on,
        acc1.txn_signature
    FROM public.account_audit AS acc1
    INNER JOIN (
        SELECT
            acc2.pubkey AS pubkey,
            MAX(acc2.slot) AS slot,
            MAX(acc2.write_version) AS write_version
        FROM public.account_audit AS acc2
        WHERE acc2.slot < until_slot
        GROUP BY acc2.pubkey
    ) latest_versions
    ON
        latest_versions.pubkey = acc1.pubkey
        AND latest_versions.slot = acc1.slot
        AND latest_versions.write_version = acc1.write_version
    WHERE
        acc1.slot < until_slot;
END;
$update_older_account$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_recent_update_slot(
    for_pubkey BYTEA,
    until_slot BIGINT
)

RETURNS TABLE (
	slot BIGINT
)

AS $get_recent_update_slot$

BEGIN
    RETURN QUERY
        WITH results AS (
            SELECT acc.slot, acc.write_version 
            FROM public.account_audit AS acc
            WHERE 
                acc.pubkey = for_pubkey 
                AND acc.slot <= until_slot 
            UNION
            SELECT old_acc.slot, old_acc.write_version
            FROM public.older_account AS old_acc
            WHERE
                old_acc.pubkey = for_pubkey
        )
        SELECT res.slot
        FROM results AS res
        ORDER BY res.slot DESC, res.write_version DESC
        LIMIT 1;
END;
$get_recent_update_slot$ LANGUAGE plpgsql;