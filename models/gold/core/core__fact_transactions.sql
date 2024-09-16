{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['scheduled_core']
) }}

{% if execute %}
    {% if is_incremental() %}
    {% set max_modified_query %}
    SELECT
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ this }}
    {% endset %}
    {% set max_modified_timestamp = run_query(max_modified_query)[0][0] %}
    {% endif %}
{% endif %}

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
    transactions_id AS fact_transactions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__transactions') }}
WHERE
    block_timestamp IS NOT NULL
{% if is_incremental() %}
    AND modified_timestamp > (SELECT max(modified_timestamp) FROM {{ this }})
{% endif %}
