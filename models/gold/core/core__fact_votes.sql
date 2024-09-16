{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','vote_index'],
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
    vote_index,
    event_type,
    instruction,
    version,
    votes_id AS fact_votes_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__votes') }}
WHERE
    block_timestamp IS NOT NULL
{% if is_incremental() %}
    AND modified_timestamp > (SELECT max(modified_timestamp) FROM {{ this }})
{% endif %}
