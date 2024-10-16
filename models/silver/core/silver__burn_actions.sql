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

WITH base AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INDEX,
        NULL AS inner_index,
        program_id,
        event_type,
        instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        event_type IN ('burn','burnChecked')
    {% if is_incremental() %}
        AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
        AND _inserted_timestamp :: DATE = '2024-09-12'
    {% endif %}
    UNION ALL
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        instruction_index AS INDEX,
        inner_index,
        program_id,
        event_type,
        instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events_inner') }}
    WHERE
        event_type IN (
            'burn',
            'burnChecked'
        )
    {% if is_incremental() %}
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
        AND _inserted_timestamp :: DATE = '2024-09-12'
    {% endif %}
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    INDEX,
    inner_index,
    event_type,
    instruction :parsed :info :mint :: STRING AS mint,
    instruction :parsed :info :account :: STRING AS token_account,
    COALESCE(
        instruction :parsed :info :amount :: INTEGER,
        instruction :parsed :info :tokenAmount: amount :: INTEGER
    ) AS burn_amount,
    COALESCE(
        instruction :parsed :info :authority :: STRING,
        instruction :parsed :info :multisigAuthority :: STRING
    ) AS burn_authority,
    instruction :parsed :info :signers :: STRING AS signers,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'mint', 'index', 'inner_index']
    ) }} AS burn_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
