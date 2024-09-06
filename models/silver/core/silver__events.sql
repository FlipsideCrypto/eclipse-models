{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','tx_id','index'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE','program_id'],
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

WITH base_transactions AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        signers,
        instructions,
        inner_instructions,
        _inserted_timestamp
    FROM
        {{ ref('silver__transactions') }} 
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
    WHERE
        _inserted_timestamp::date = '2024-08-30' /* TODO replace with whenever we start getting data in PROD */
    {% endif %}
),
base_instructions AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        signers,
        i.index::integer AS index,
        i.value:parsed:type::string AS event_type,
        i.value:programId::string AS program_id,
        i.value AS instruction,
        _inserted_timestamp
    FROM
        base_transactions t,
        table(flatten(instructions)) i
),
base_inner_instructions AS (
    SELECT
        block_id,
        tx_id,
        ii.value:index::integer AS mapped_instruction_index,
        ii.value AS inner_instruction,
        silver.udf_get_all_inner_instruction_program_ids(ii.value) AS inner_instruction_program_ids,
    FROM
        base_transactions t,
        table(flatten(inner_instructions)) ii
)
SELECT
    i.block_timestamp,
    i.block_id,
    i.tx_id,
    i.signers,
    i.succeeded,
    i.index,
    i.program_id AS program_id,
    i.event_type AS event_type,
    i.instruction,
    ii.inner_instruction,
    ii.inner_instruction_program_ids,
    i._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['i.block_id', 'i.tx_id', 'i.index']
    ) }} AS events_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_instructions i
LEFT OUTER JOIN 
    base_inner_instructions ii
    ON ii.block_id = i.block_id
    AND ii.tx_id = i.tx_id
    AND ii.mapped_instruction_index = i.index
