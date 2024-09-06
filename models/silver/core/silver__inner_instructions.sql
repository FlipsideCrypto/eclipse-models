{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','tx_id','instruction_index','index'],
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

WITH pre_final AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        e.index AS instruction_index,
        ii.index::integer AS index,
        succeeded,
        signers,
        e.program_id AS instruction_program_id,
        ii.value:programId::string AS program_id,
        ii.value:parsed:type::string AS event_type,
        ii.value AS instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }} e,
        table(flatten(inner_instruction:instructions)) ii 
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
    WHERE
        _inserted_timestamp::date = '2024-08-30' /* TODO replace with whenever we start getting data in PROD */
    {% endif %}
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    signers,
    succeeded,
    instruction_index,
    index,
    instruction_program_id,
    program_id,
    event_type,
    instruction,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'tx_id', 'instruction_index', 'index']
    ) }} AS inner_instructions_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
