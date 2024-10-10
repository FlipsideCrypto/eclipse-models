{{ config(
    materialized = 'incremental',
    unique_key = ["burn_actions_id"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
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

with base as (
        SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        index,
        null as inner_index,
        program_id,
        event_type,
        instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }} 
    where event_type IN (
       'burn',
        'burnChecked'
    )
    {% if is_incremental() %}
        AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
        AND _inserted_timestamp::date = '2024-09-12'
    {% endif %}
    union ALL
        SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        instruction_index as index,
        inner_index,
        program_id,
        event_type,
        instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events_inner') }} 
    where event_type IN (
       'burn',
        'burnChecked'
    )
    {% if is_incremental() %}
        AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
        AND _inserted_timestamp::date = '2024-09-12'
    {% endif %}
)

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    instruction :parsed :info :mint :: STRING AS mint,
    instruction :parsed :info :account :: STRING as token_account, 
    COALESCE(
        instruction :parsed :info :amount :: INTEGER,
        instruction :parsed :info :tokenAmount: amount :: INTEGER
    ) AS burn_amount,
    COALESCE(
        instruction :parsed :info :authority :: string,
        instruction :parsed :info :multisigAuthority :: string
    ) AS burn_authority,
    instruction :parsed :info :signers :: string AS signers,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'mint', 'index', 'inner_index']
    ) }} AS burn_actions_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base