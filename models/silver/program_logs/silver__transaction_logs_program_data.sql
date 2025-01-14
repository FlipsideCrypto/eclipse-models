{{ config(
    materialized = 'incremental',
    unique_key = "transaction_logs_program_data_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, program_id)'),
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_core'],
) }}

WITH base AS (
    SELECT 
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        signers,
        silver.udf_get_logs_program_data(log_messages) AS program_data_logs,
        _inserted_timestamp
    FROM 
        {{ ref('silver__transactions') }}
    WHERE 
        succeeded
        AND log_messages IS NOT NULL
        {% if is_incremental() %}
        AND _inserted_timestamp >= (SELECT max(_inserted_timestamp) FROM {{ this }}) 
        {% endif %}
)
SELECT 
    t.block_timestamp,
    t.block_id,
    t.tx_id,
    t.succeeded,
    t.signers,
    l.value:index::int AS index,
    l.value:inner_index::int AS inner_index,
    l.index AS log_index,
    l.value:program_id::string AS program_id,
    l.value:event_type::string AS event_type,
    l.value:data::string AS data,
    l.value:error::string AS _udf_error,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','index','log_index']
    ) }} AS transaction_logs_program_data_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    base t
JOIN 
    table(flatten(program_data_logs)) l