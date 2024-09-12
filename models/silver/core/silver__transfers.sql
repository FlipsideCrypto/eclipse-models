{{ config(
    materialized = 'incremental',
    unique_key = ["transfers_id"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_core']
) }}

{% if execute %}
    {% if is_incremental() %}
    {% set max_inserted_query %}
    SELECT
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ this }}
    {% endset %}
    {% set max_inserted_timestamp = run_query(max_inserted_query)[0][0] %}
    {% endif %}
{% endif %}

/* TODO figure out what the addresses are for this on eclipse */
{% set native_token_address = "Eth1111111111111111111111111111111111111111" %}
{% set native_wrapped_token_address = "Eth1111111111111111111111111111111111111112" %}

WITH base_transfers_i AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        index,
        null AS inner_index,
        event_type,
        program_id,
        instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        event_type IN (
            'transfer',
            'transferChecked',
            'transferWithSeed',
            'transferCheckedWithFee'
        )
    {% if is_incremental() %}
        AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
        AND _inserted_timestamp::date = '2024-08-30' /* TODO replace with whenever we start getting data in PROD */
    {% endif %}
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        instruction_index AS index,
        inner_index,
        event_type,
        program_id,
        instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events_inner') }}
    WHERE
        event_type IN (
            'transfer',
            'transferChecked',
            'transferWithSeed',
            'transferCheckedWithFee'
        )
    {% if is_incremental() %}
        AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
        AND _inserted_timestamp::date = '2024-08-30' /* TODO replace with whenever we start getting data in PROD */
    {% endif %}
),
base_post_token_balances AS (
    SELECT
        tx_id,
        owner,
        account,
        mint,
        DECIMAL
    FROM
        {{ ref('silver___post_token_balances') }}
    WHERE
    {% if is_incremental() %}
        _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
        _inserted_timestamp::date = '2024-08-30' /* TODO replace with whenever we start getting data in PROD */
    {% endif %}
),
base_pre_token_balances AS (
    SELECT
        tx_id,
        owner,
        account,
        mint,
        DECIMAL
    FROM
        {{ ref('silver___pre_token_balances') }}
    WHERE
    {% if is_incremental() %}
        _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
        _inserted_timestamp::date = '2024-08-30' /* TODO replace with whenever we start getting data in PROD */
    {% endif %}
),
spl_transfers AS (
    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        e.index,
        e.inner_index,
        e.program_id,
        e.succeeded,
        coalesce(
            p.owner,
            e.instruction:parsed:info:authority::string,
            e.instruction:parsed:info:multisigAuthority::string
        ) AS tx_from,
        coalesce(
            p2.owner,
            instruction:parsed:info:destination::string
        ) AS tx_to,
        coalesce(
            e.instruction:parsed:info:tokenAmount:decimals::integer,
            p.decimal,
            p2.decimal,
            p3.decimal,
            p4.decimal,
            9 -- default to lamport decimals
        ) AS decimal,
        coalesce (
            e.instruction:parsed:info:amount::integer,
            e.instruction:parsed:info:tokenAmount:amount::integer
        ) AS amount,
        coalesce(
            p.mint,
            p2.mint,
            p3.mint,
            p4.mint
        ) AS mint,
        instruction:parsed:info:source::string as source_token_account,
        instruction:parsed:info:destination::string as dest_token_account,
        e._inserted_timestamp
    FROM
        base_transfers_i e
    LEFT OUTER JOIN 
        base_pre_token_balances p
        ON e.tx_id = p.tx_id
        AND e.instruction:parsed:info:source::string = p.account
    LEFT OUTER JOIN 
        base_post_token_balances p2
        ON e.tx_id = p2.tx_id
        AND e.instruction:parsed:info:destination::string = p2.account
    LEFT OUTER JOIN 
        base_post_token_balances p3
        ON e.tx_id = p3.tx_id
        AND e.instruction:parsed:info:source::string = p3.account
    LEFT OUTER JOIN 
        base_pre_token_balances p4
        ON e.tx_id = p4.tx_id
        AND e.instruction:parsed:info:destination::string = p4.account
    WHERE
        (
            e.instruction:parsed:info:authority::string IS NOT NULL
            OR e.instruction:parsed:info:multisigAuthority::string IS NOT NULL
        )
),
native_transfers AS (
    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        e.index,
        e.inner_index,
        e.program_id,
        e.succeeded,
        instruction:parsed:info:source::string AS tx_from,
        instruction:parsed:info:destination::string AS tx_to,
        9 AS decimal, -- Default decimal adjustment for Lamports
        instruction:parsed:info:lamports AS amount,
        CASE
            WHEN e.program_id = '11111111111111111111111111111111' THEN '{{ native_token_address }}' 
            ELSE '{{ native_wrapped_token_address }}'
        END AS mint,
        NULL AS source_token_account,
        NULL AS dest_token_account,
        e._inserted_timestamp
    FROM
        base_transfers_i e
    WHERE
        instruction:parsed:info:lamports::string IS NOT NULL
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    tx_from,
    tx_to,
    mint,
    amount,
    decimal,
    source_token_account,
    dest_token_account,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'tx_id', 'index', 'inner_index']
    ) }} AS transfers_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    spl_transfers
UNION
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    tx_from,
    tx_to,
    mint,
    amount,
    decimal,
    source_token_account,
    dest_token_account,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'tx_id', 'index', 'inner_index']
    ) }} AS transfers_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    native_transfers
