-- depends_on: {{ ref('bronze__transactions') }}
-- depends_on: {{ ref('bronze__FR_transactions') }}
-- depends_on: {{ ref('streamline__blocks') }}

{{ config (
    materialized = "incremental",
    unique_key = 'block_id',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = "ROUND(block_id, -4)",
) }}

{% if execute %}
    {% set min_partition_key_query %}
        SELECT round(min(block_id),-4)::int AS min_partition_key
        FROM (
            SELECT
                block_id
            FROM
                {{ ref("streamline__blocks") }}
            WHERE
                /* Find the earliest block available from the node provider */
                block_id >= 6572203
            EXCEPT
            SELECT
                block_id
            FROM
                {{ this }}
        )
    {% endset %}
    {% set min_partition_key = run_query(min_partition_key_query)[0][0] %}
{% endif %}

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
    {{ ref('bronze__transactions') }}
WHERE
    partition_key >= {{ min_partition_key }}
    AND _inserted_timestamp >= (
        SELECT
            coalesce(max(_inserted_timestamp), '1970-01-01' :: DATE) AS max_inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__FR_transactions') }}
{% endif %}
QUALIFY
    row_number() OVER (PARTITION BY block_id ORDER BY _inserted_timestamp DESC) = 1