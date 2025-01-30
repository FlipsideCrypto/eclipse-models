{{ config(
    materialized = 'incremental',
    unique_key = ["fact_transfers_id"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, program_id, tx_from, tx_to, mint, index, inner_index, fact_transfers_id)'),
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
    transfers_id AS fact_transfers_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM
    {{ ref('silver__transfers') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > '{{ max_modified_timestamp }}'
{% endif %}
