{{ config (
    materialized = 'view'
) }}

{% set model = "block_txs" %}
{{ streamline_external_table_FR_query_v2(
    model,
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_name = "_partition_by_block_id",
    unique_key = "block_id",
    other_cols="data:error::variant AS error, data:transaction:signatures[0]::string AS tx_id, value:array_index AS index"
) }}