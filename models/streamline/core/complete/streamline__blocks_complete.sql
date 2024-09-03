-- depends_on: {{ ref('bronze__blocks') }}
-- depends_on: {{ ref('bronze__FR_blocks') }}

{{ config (
    materialized = "incremental",
    unique_key = 'block_id',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = "ROUND(block_id, -5)",
) }}

SELECT
    block_id,
    error,
    partition_key,
    _inserted_timestamp,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
FROM
{% if is_incremental() %}
    {{ ref('bronze__blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__FR_blocks') }}
{% endif %}
QUALIFY
    row_number() OVER (PARTITION BY block_id ORDER BY _inserted_timestamp DESC) = 1