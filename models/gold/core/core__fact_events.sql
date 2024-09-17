{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','tx_id','index'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','program_id'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, program_id, event_type, inner_instruction_program_ids)'),
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
    signers,
    succeeded,
    index,
    program_id,
    event_type,
    instruction,
    inner_instruction,
    inner_instruction_program_ids,
    events_id AS fact_events_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__events') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > '{{ max_modified_timestamp }}'
{% endif %}
