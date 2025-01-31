-- depends_on: {{ ref('bronze__transactions') }}

{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
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

WITH pre_final AS (
    SELECT
        b.block_timestamp AS block_timestamp,
        t.block_id,
        t.tx_id,
        t.index,
        t.data:transaction:message:recentBlockhash::string AS recent_block_hash,
        t.data:meta:fee::number AS fee,
        CASE
            WHEN is_null_value(t.data:meta:err) THEN 
                TRUE
            ELSE 
                FALSE
        END AS succeeded,
        t.data:transaction:message:accountKeys::array AS account_keys,
        t.data:meta:preBalances::array AS pre_balances,
        t.data:meta:postBalances::array AS post_balances,
        t.data:meta:preTokenBalances::array AS pre_token_balances,
        t.data:meta:postTokenBalances::array AS post_token_balances,
        t.data:transaction:message:instructions::array AS instructions,
        t.data:meta:innerInstructions::array AS inner_instructions,
        t.data:meta:logMessages::array AS log_messages,
        t.data:transaction:message:addressTableLookups::array as address_table_lookups,
        t.data :meta :computeUnitsConsumed :: NUMBER as units_consumed,
        t.data:version::string as version,
        t.partition_key,
        t._inserted_timestamp
    FROM
        {% if is_incremental() %}
        {{ ref('bronze__transactions') }} t
        {% else %}
        {{ ref('bronze__FR_transactions') }} t
        {% endif %}
    LEFT OUTER JOIN 
        {{ ref('silver__blocks') }} b
        ON b.block_id = t.block_id
    WHERE
        tx_id IS NOT NULL
        AND (
            coalesce(t.data:transaction:message:instructions[0]:programId::STRING,'') <> 'Vote111111111111111111111111111111111111111'
            OR array_size(t.data:transaction:message:instructions) > 1
        )
        {% if is_incremental() %}
        AND t._inserted_timestamp >= '{{ max_inserted_timestamp }}'
        {% else %}
        AND t._inserted_timestamp::date = '2024-09-12'
        {% endif %}
),
{% if is_incremental() %}
prev_null_block_timestamp_txs AS (
    SELECT
        b.block_timestamp,
        t.block_id,
        t.tx_id,
        t.index,
        t.recent_block_hash,
        t.signers,
        t.fee,
        t.succeeded,
        t.account_keys,
        t.pre_balances,
        t.post_balances,
        t.pre_token_balances,
        t.post_token_balances,
        t.instructions,
        t.inner_instructions,
        t.log_messages,
        t.address_table_lookups,
        t.units_consumed,
        t.units_limit,
        t.tx_size,
        t.version,
        t.partition_key,
        GREATEST(
            t._inserted_timestamp,
            b._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ this }} t
    INNER JOIN 
        {{ ref('silver__blocks') }} b
        ON b.block_id = t.block_id
    WHERE
        t.block_timestamp::DATE IS NULL
),
{% endif %}
qualifying_transactions AS (
    SELECT
        tx_id,
        array_agg(i.value:programId::string) WITHIN GROUP (ORDER BY i.index) AS program_ids
    FROM 
        pre_final
    JOIN
        table(flatten(instructions)) i
    WHERE
        (
            coalesce(instructions[0]:programId::string,'') <> 'Vote111111111111111111111111111111111111111'
            /* small amount of txs have non-compute instructions after the vote */
            OR i.value:programId::string NOT IN ('Vote111111111111111111111111111111111111111','ComputeBudget111111111111111111111111111111')
        )
    GROUP BY 1
    HAVING array_size(program_ids) > 0
    UNION ALL
    /* some txs have no instructions at all, this is being filtered out above so we need to make sure we grab these */
    SELECT
        tx_id,
        null
    FROM
        pre_final
    WHERE
        array_size(instructions) = 0
),
combined AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        recent_block_hash,
        silver.udf_ordered_signers(account_keys) AS signers,
        fee,
        succeeded,
        account_keys,
        pre_balances,
        post_balances,
        pre_token_balances,
        post_token_balances,
        instructions,
        inner_instructions,
        log_messages,
        address_table_lookups,
        units_consumed,
        silver.udf_get_compute_units_total(log_messages, instructions) as units_limit,
        silver.udf_get_tx_size(account_keys,instructions,version,address_table_lookups,signers) as tx_size,
        version,
        partition_key,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }} AS transactions_id,
        sysdate() AS inserted_timestamp,
        sysdate() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        pre_final b 
    JOIN
        qualifying_transactions
        USING(tx_id)
    {% if is_incremental() %}
    UNION
    SELECT
        *,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }} AS transactions_id,
        sysdate() AS inserted_timestamp,
        sysdate() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        prev_null_block_timestamp_txs
    {% endif %}
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    index,
    recent_block_hash,
    signers,
    fee,
    succeeded,
    account_keys,
    pre_balances,
    post_balances,
    pre_token_balances,
    post_token_balances,
    instructions,
    inner_instructions,
    log_messages,
    address_table_lookups,
    units_consumed,
    units_limit,
    tx_size,
    version,
    partition_key,
    _inserted_timestamp,
    transactions_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    combined
QUALIFY
    row_number() OVER (PARTITION BY block_id, tx_id ORDER BY _inserted_timestamp DESC) = 1