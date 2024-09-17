{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','vote_index'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id)'),
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
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM
    {{ ref('silver__votes') }}
WHERE
    block_timestamp IS NOT NULL
{% if is_incremental() %}
    AND modified_timestamp > '{{ max_modified_timestamp }}'
{% endif %}
