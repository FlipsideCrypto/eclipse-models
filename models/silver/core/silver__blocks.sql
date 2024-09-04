{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::date'],
  full_refresh = false,
  tags = ['scheduled_core'],
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
        block_id,
        data:result:blockTime::timestamp_ntz AS block_timestamp,
        'mainnet' AS network,
        'eclipse' AS chain_id,
        data:result:blockHeight AS block_height,
        data:result:blockhash::string AS block_hash,
        data:result:parentSlot AS previous_block_id,
        data:result:previousBlockhash::string AS previous_block_hash,
        _inserted_timestamp
    FROM
        {{ ref('bronze__blocks') }}
    WHERE
        block_id IS NOT NULL
        AND error IS NULL
        {% if is_incremental() %}
        AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
        {% endif %}
    QUALIFY
        row_number() OVER (PARTITION BY block_id ORDER BY _inserted_timestamp DESC) = 1
)
SELECT
    block_id,
    block_timestamp,
    network,
    chain_id,
    block_height,
    block_hash,
    previous_block_id,
    previous_block_hash,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id']
    ) }} AS blocks_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
