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