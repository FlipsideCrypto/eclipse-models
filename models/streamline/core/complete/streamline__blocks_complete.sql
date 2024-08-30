-- depends_on: {{ ref('bronze__blocks') }}
-- depends_on: {{ ref('bronze__FR_blocks') }}

{{ config (
    materialized = "incremental",
    unique_key = 'block_id',
    cluster_by = "ROUND(block_id, -5)",
) }}

SELECT
    block_id,
    error,
    _partition_by_block_id,
    _inserted_timestamp
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