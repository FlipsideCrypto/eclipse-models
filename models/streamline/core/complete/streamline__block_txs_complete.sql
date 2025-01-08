-- depends_on: {{ ref('bronze__transactions') }}
-- depends_on: {{ ref('bronze__FR_transactions') }}
{{ config (
    materialized = "incremental",
    unique_key = 'block_id',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = "ROUND(block_id, -5)",
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_pk_query %}

SELECT
    COALESCE(MAX(partition_key), 0) - 1000000 AS partition_key
FROM
    {{ this }}

    {% endset %}
    {% set max_pk = run_query(max_pk_query) [0] [0] %}
    {% set max_inserted_query %}
SELECT
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_inserted_timestamp = run_query(max_inserted_query) [0] [0] %}
{% endif %}
{% endif %}

{% set base_query %}
CREATE
OR REPLACE temporary TABLE streamline.blocks_txs__intermediate_tmp AS
SELECT
    block_id,
    error,
    partition_key,
    _inserted_timestamp,
FROM

{% if is_incremental() %}
{{ ref('bronze__transactions') }}
WHERE
    _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    AND partition_key >= {{ max_pk }}
{% else %}
    {{ ref('bronze__FR_transactions') }}
{% endif %}

{% endset %}
{% do run_query(base_query) %}
SELECT
    block_id,
    error,
    partition_key,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
FROM
    streamline.blocks_txs__intermediate_tmp qualify ROW_NUMBER() over (
        PARTITION BY block_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
