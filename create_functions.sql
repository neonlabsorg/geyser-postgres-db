
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
-- Returns pre-accounts data for given transaction on a given slot
CREATE OR REPLACE FUNCTION get_latest_accounts_one_slot(
    max_slot BIGINT,
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

AS $get_latest_accounts_one_slot$

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
            AND acc.slot = current_slot
            AND (
                -- sarching in the slot where search started - write version determines order
                acc.slot = max_slot AND acc.write_version < max_write_version
                -- searchin in slots below start slot - write version ignored (for each slot it starts from 0)
                OR acc.slot != max_slot
            )
        ORDER BY
            acc.pubkey,
            acc.slot DESC,
            acc.write_version DESC;
END;
$get_latest_accounts_one_slot$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_latest_branch_accounts(
    max_slot BIGINT,
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

AS $get_latest_branch_accounts$

DECLARE
    branch_slots BIGINT[];

BEGIN 
    -- Find all slots on the given branch starting from max_slot down to first rooted slot
    WITH RECURSIVE parents AS (
        SELECT
            first.slot,
            first.parent,
            first.status
        FROM public.slot AS first
        WHERE first.slot = max_slot and first.status <> 'rooted'
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
   
    -- Find latest states of all accounts from transaction_accounts
    -- on the found branch of non-rooted slots
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
            get_latest_accounts_one_slot(
                max_slot,
                current_slot, 
                max_write_version, 
                transaction_accounts
            ) AS slot_results
        ORDER BY
            slot_results.pubkey,
            slot_results.slot DESC,
            slot_results.write_version DESC;
END;
$get_latest_branch_accounts$ LANGUAGE plpgsql;


-----------------------------------------------------------------------------------------------------------------------
-- Returns latest versions of accounts with pubkeys included in @transaction_accounts
-- with update events in slots not much than max_slot
-- and write versions not much than max_write_version
-- Search is performed over account_audit table (latest written history)
CREATE OR REPLACE FUNCTION get_latest_accounts_audit(
    max_slot BIGINT,
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

AS $get_latest_accounts_audit$

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
            AND (
                -- sarching in the slot where search started - write version determines order
                acc.slot = max_slot AND acc.write_version < max_write_version
                -- searchin in slots below start slot - write version ignored (for each slot it starts from 0)
                OR acc.slot < max_slot
            )
        ORDER BY
            acc.pubkey,
            acc.slot DESC,
            acc.write_version DESC;
END;
$get_latest_accounts_audit$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
-- Returns latest versions of accounts with pubkeys included in @transaction_accounts
-- with update events in slots not much than max_slot
-- and write versions not much than @max_write_version
-- Search is performed over older_account table (oldest versions)
CREATE OR REPLACE FUNCTION get_latest_accounts_older(
    max_slot BIGINT,
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

AS $get_latest_accounts_older$

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
            AND (
                -- sarching in the slot where search started - write version determines order
                old.slot = max_slot AND old.write_version < max_write_version
                -- searchin in slots below start slot - write version ignored (for each slot it starts from 0)
                OR old.slot < max_slot
            );
END;
$get_latest_accounts_older$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_latest_rooted_accounts(
    max_slot BIGINT,
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

AS $get_latest_rooted_accounts$

BEGIN
    RETURN QUERY
        -- root branch consist of historical states in account_audit 
        -- plus olderst available state in older_account - collect from both
        -- and take only latest for each account from transaction_accounts
        WITH results AS (
            SELECT * FROM get_latest_accounts_audit(max_slot, max_write_version, transaction_accounts)
            UNION
            SELECT * FROM get_latest_accounts_older(max_slot, max_write_version, transaction_accounts)
        )
        SELECT DISTINCT ON (res.pubkey) * FROM results AS res 
        ORDER BY
            res.pubkey,
            res.slot DESC,
            res.write_version DESC;
END;
$get_latest_rooted_accounts$ LANGUAGE plpgsql;

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
    LOCK TABLE public.transaction IN ACCESS SHARE MODE;
    LOCK TABLE public.slot IN ACCESS SHARE MODE;
    LOCK TABLE public.account_audit IN ACCESS SHARE MODE;
    LOCK TABLE public.older_account IN ACCESS SHARE MODE;

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

            -- Query first rooted slot
            SELECT sl.slot
            INTO first_rooted_slot
            FROM public.slot AS sl
            WHERE sl.status = 'rooted'
            ORDER BY sl.slot DESC
            LIMIT 1;

            RETURN QUERY
                WITH results AS (
                    -- Start searching recent states of accounts in this branch
                    -- down to first rooted slot 
                    -- (this search algorithm iterates over parent slots and is slow).
                    SELECT * FROM get_latest_branch_accounts(
                        current_slot,
                        max_write_version,
                        transaction_accounts
                    )
                    UNION
                    -- Then apply fast search algorithm over rooted slots 
                    -- to obtain the rest of pre-accounts  
                    SELECT * FROM get_latest_rooted_accounts(
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
            SELECT * FROM get_latest_rooted_accounts(
                current_slot,
                max_write_version,
                transaction_accounts
            );
    END IF;
END;
$get_pre_accounts$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE update_older_account(
    max_slot BIGINT
)

AS $update_older_account$

DECLARE
    min_slot BIGINT;

BEGIN
    -- determine slot to start 
    -- (maximum slot which was already processed and stored in older_account)
    SELECT COALESCE(MAX(older.slot), 0) INTO min_slot
    FROM public.older_account AS older;

    -- add recent states of all accounts from account_audit
    -- before slot max_slot into older_account table
    INSERT INTO public.older_account AS older
    SELECT DISTINCT ON (acc1.pubkey)
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
        SELECT DISTINCT ON (acc2.pubkey)
            acc2.pubkey AS pubkey,
            acc2.slot AS slot,
            acc2.write_version AS write_version
        FROM public.account_audit AS acc2
        WHERE acc2.slot >= min_slot AND acc2.slot < max_slot
        ORDER BY acc2.pubkey, acc2.slot DESC, acc2.write_version DESC
    ) latest_versions
    ON
        latest_versions.pubkey = acc1.pubkey
        AND latest_versions.slot = acc1.slot
        AND latest_versions.write_version = acc1.write_version
    WHERE
        acc1.slot >= min_slot AND acc1.slot < max_slot
    ON CONFLICT (pubkey) DO UPDATE SET 
		slot=excluded.slot, 
		owner=excluded.owner, 
		lamports=excluded.lamports, 
		executable=excluded.executable, 
		rent_epoch=excluded.rent_epoch,
        data=excluded.data, 
		write_version=excluded.write_version, 
		updated_on=excluded.updated_on, 
		txn_signature=excluded.txn_signature
        WHERE 
            older.slot < excluded.slot 
            OR (older.slot = excluded.slot AND older.write_version < excluded.write_version);
END;
$update_older_account$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_recent_update_slot(
    in_pubkey BYTEA,
    max_slot BIGINT
)

RETURNS TABLE (
	slot BIGINT
)

AS $get_recent_update_slot$

BEGIN
    LOCK TABLE public.account_audit IN ACCESS SHARE MODE;
    LOCK TABLE public.older_account IN ACCESS SHARE MODE;

    RETURN QUERY
        WITH results AS (
            SELECT acc.slot, acc.write_version 
            FROM public.account_audit AS acc
            WHERE 
                acc.pubkey = in_pubkey 
                AND acc.slot <= max_slot 
            UNION
            SELECT old_acc.slot, old_acc.write_version
            FROM public.older_account AS old_acc
            WHERE
                old_acc.pubkey = in_pubkey
        )
        SELECT res.slot
        FROM results AS res
        ORDER BY res.slot DESC, res.write_version DESC
        LIMIT 1;
END;
$get_recent_update_slot$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE maintenance_proc()

AS $maintenance_proc$

DECLARE
    retention_slots BIGINT;
    retention_until_slot BIGINT;

BEGIN
    LOCK TABLE public.account_audit IN ACCESS EXCLUSIVE MODE;

    SELECT MAX(retention)
    INTO retention_slots
    FROM partman.part_config
    WHERE parent_table = 'public.account_audit';

    SELECT MAX(slot) - retention_slots 
    INTO retention_until_slot
    FROM public.account_audit;

    CALL update_older_account(retention_until_slot);
    PERFORM FROM partman.run_maintenance(NULL, NULL, FALSE);
END;
$maintenance_proc$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------

CREATE PROCEDURE order_accounts() AS $order_accounts$
    BEGIN
        LOCK TABLE public.account IN ACCESS EXCLUSIVE MODE;

        CREATE TEMPORARY TABLE items_to_move (
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
        )
        ON COMMIT DROP;

        -- match transactions and accounts by transaction signature and slot
        INSERT INTO items_to_move AS mv(
            pubkey,
            owner,
            lamports,
            slot,
            executable,
            rent_epoch,
            data,
            write_version,
            updated_on,
            txn_signature
        )
        SELECT
            acc.pubkey,
            acc.owner,
            acc.lamports,
            acc.slot,
            acc.executable,
            acc.rent_epoch,
            acc.data,
            txn.write_version,
            acc.updated_on,
            acc.txn_signature 
        FROM public.account AS acc
        INNER JOIN public.transaction AS txn
        ON
            acc.txn_signature = txn.signature 
            AND acc.slot = txn.slot
        WHERE
            acc.processed = FALSE;

        -- find accounts with empty txn_signature (move them anyway with write_version set to 0)
        INSERT INTO items_to_move AS mv(
            pubkey,
            owner,
            lamports,
            slot,
            executable,
            rent_epoch,
            data,
            write_version,
            updated_on,
            txn_signature
        )
        SELECT
            acc.pubkey,
            acc.owner,
            acc.lamports,
            acc.slot,
            acc.executable,
            acc.rent_epoch,
            acc.data,
            0,
            acc.updated_on,
            acc.txn_signature
        FROM public.account AS acc
        WHERE 
            acc.processed = FALSE 
            AND acc.txn_signature IS NULL;

        INSERT INTO public.account_audit
        SELECT * FROM items_to_move
        ON CONFLICT ON CONSTRAINT account_audit_pk DO NOTHING;

        UPDATE public.account AS acc
        SET processed = TRUE
        FROM items_to_move AS mv
        WHERE
			acc.processed = FALSE
			AND (
            	acc.txn_signature IS NULL
            	OR acc.txn_signature = mv.txn_signature 
            	AND acc.slot = mv.slot
			);
    END;

$order_accounts$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_account_at_slot_from_audit(
    in_pubkey BYTEA,
    in_slot BIGINT,
    max_slot BIGINT,
    max_write_version BIGINT
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
        SELECT
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
            acc.pubkey = in_pubkey 
            AND (
                -- common case
                -- used to select latest version of account on a moment of a given in_slot 
                max_slot IS NULL AND acc.slot <= in_slot
                -- case for get_pre_accounts
                -- used to select version of account preliminary to some particular transaction
                OR max_slot IS NOT NULL AND max_write_version IS NOT NULL AND (
                    acc.slot = max_slot AND acc.write_version < max_write_version
                    OR acc.slot < max_slot
                ) 
            )
        ORDER BY 
            acc.pubkey,
            acc.slot DESC,
            acc.write_version DESC 
        LIMIT 1;
END;
$get_account_at_slot_from_audit$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_account_at_root(
    in_pubkey BYTEA,
    first_rooted_slot BIGINT,
    max_slot BIGINT,
    max_write_version BIGINT
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

AS $get_account_at_root$

BEGIN
    RETURN QUERY
        WITH results AS (
            SELECT * 
            FROM get_account_at_slot_from_audit(
                in_pubkey, 
                first_rooted_slot, 
                max_slot, 
                max_write_version
            )
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
        WHERE
            -- common case
            -- used to select latest version of account on a moment of a given first_rooted_slot 
            max_slot IS NULL AND res.slot <= first_rooted_slot
            -- case for get_pre_accounts
            -- used to select version of account preliminary to some particular transaction
            OR max_slot IS NOT NULL AND max_write_version IS NOT NULL AND (
                res.slot = max_slot AND res.write_version < max_write_version
                OR res.slot < max_slot
            )
        ORDER BY res.slot DESC, res.write_version DESC 
        LIMIT 1;
END;
$get_account_at_root$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_account_at_single_slot(
    in_pubkey BYTEA,
    in_slot BIGINT,
    max_slot BIGINT,
    max_write_version BIGINT
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

AS $get_account_at_single_slot$

BEGIN
    RETURN QUERY
        SELECT
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
            acc.pubkey = in_pubkey AND acc.slot = in_slot AND (
                max_slot IS NULL
                OR max_slot IS NOT NULL AND max_write_version IS NOT NULL AND (
                    acc.slot = max_slot AND acc.write_version < max_write_version
                    OR acc.slot < max_slot 
                )
            )
        LIMIT 1;
END;
$get_account_at_single_slot$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_account_at_branch(
    in_pubkey BYTEA,
    branch_slots BIGINT[],
    max_slot BIGINT,
    max_write_version BIGINT
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

AS $get_account_at_branch$

BEGIN
    RETURN QUERY
        SELECT
            slot_results.pubkey,
            slot_results.owner,
            slot_results.lamports,
            slot_results.executable,
            slot_results.rent_epoch,
            slot_results.data,
            slot_results.slot,
            slot_results.write_version,
            slot_results.signature
        FROM
            unnest(branch_slots) AS current_slot,
            get_account_at_single_slot(
                in_pubkey,
                current_slot,
                max_slot,
                max_write_version
            ) AS slot_results
        ORDER BY
            slot_results.pubkey,
            slot_results.slot DESC,
            slot_results.write_version DESC
        LIMIT 1;
END;
$get_account_at_branch$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_account_at_slot_impl(
    in_pubkey BYTEA,
    branch_slots BIGINT[],
    first_rooted_slot BIGINT,
    max_slot BIGINT,
    max_write_version BIGINT
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

AS $get_account_at_slot_impl$

BEGIN
    RETURN QUERY
        WITH results AS (
            -- Start searching recent states of accounts in this branch
            -- down to first rooted slot
            SELECT * FROM get_account_at_branch(
                in_pubkey,
                branch_slots,
                max_slot,
                max_write_version
            )
            UNION
            -- Then apply fast search algorithm over rooted slots 
            SELECT * FROM get_account_at_root(
                in_pubkey,
                first_rooted_slot,
                max_slot,
                max_write_version
            )
        )
        SELECT *
        FROM results AS res
        LIMIT 1;
END;
$get_account_at_slot_impl$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_branch_slots(
    start_slot BIGINT
)

RETURNS BIGINT[]

AS $get_branch_slots$

DECLARE
    branch_slots BIGINT[];

BEGIN
    -- Find all slots on the given branch starting from max_slot down to first rooted slot
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

    RETURN branch_slots;
END;
$get_branch_slots$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_first_rooted_slot(
    start_slot BIGINT
)

RETURNS BIGINT

AS $get_first_rooted_slot$

DECLARE
    result BIGINT;

BEGIN
    SELECT sl.slot
    INTO result
    FROM public.slot AS sl
    WHERE sl.slot <= start_slot AND sl.status = 'rooted'
    ORDER BY sl.slot DESC
    LIMIT 1;

    RETURN result;
END;
$get_first_rooted_slot$ LANGUAGE plpgsql;

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

DECLARE 
    branch_slots BIGINT[] = NULL;
    first_rooted_slot BIGINT = NULL;

BEGIN
    SELECT * INTO branch_slots FROM get_branch_slots(in_slot);
    SELECT * INTO first_rooted_slot FROM get_first_rooted_slot(in_slot);

    RETURN QUERY
        SELECT * FROM get_account_at_slot_impl(
            in_pubkey, 
            branch_slots, 
            first_rooted_slot,
            NULL, -- max_slot
            NULL  -- max_write_version
        );
END;
$get_account_at_slot$ LANGUAGE plpgsql;
