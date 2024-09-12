-- depends_on: {{ ref('bronze__transactions') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','vote_index'],
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
        i.index::int AS vote_index,
        i.value:parsed:type::string AS event_type,
        i.value::variant AS instruction,
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
    JOIN
        table(flatten(t.data:transaction:message:instructions)) i
    WHERE
        tx_id IS NOT NULL
        AND coalesce(t.data:transaction:message:instructions[0]:programId::string,'') = 'Vote111111111111111111111111111111111111111'
        AND i.value:programId::string = 'Vote111111111111111111111111111111111111111'
        {% if is_incremental() %}
        AND t._inserted_timestamp >= '{{ max_inserted_timestamp }}'
        {% else %}
        AND t._inserted_timestamp::date = '2024-08-30' /* TODO replace with whenever we start getting data in PROD */
        {% endif %}
)
{% if is_incremental() %}
,
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
        t.vote_index,
        t.event_type,
        t.instruction,
        t.version,
        t.partition_key,
        greatest(t._inserted_timestamp,b._inserted_timestamp) as _inserted_timestamp
    FROM
        {{ this }} t
    INNER JOIN 
        {{ ref('silver__blocks') }} b
        ON b.block_id = t.block_id
    WHERE
        t.block_timestamp::DATE IS NULL
)
{% endif %}
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
    vote_index,
    event_type,
    instruction,
    version,
    partition_key,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'vote_index']
    ) }} AS votes_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final b 
QUALIFY
    row_number() OVER (PARTITION BY block_id, tx_id ORDER BY _inserted_timestamp DESC) = 1
{% if is_incremental() %}
UNION
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'vote_index']
    ) }} AS votes_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    prev_null_block_timestamp_txs
{% endif %}