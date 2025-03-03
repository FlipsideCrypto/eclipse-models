{{ config (
    materialized = 'view'
) }}

{% set model = "block_txs_2" %}
{{ streamline_external_table_query_v2(
    model,
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_name = "partition_key",
    unique_key = "block_id",
    other_cols=""
) }}