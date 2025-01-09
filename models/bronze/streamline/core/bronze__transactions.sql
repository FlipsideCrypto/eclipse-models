{{ config (
    materialized = 'view'
) }}

WITH meta AS (
    SELECT
        LAST_MODIFIED::timestamp_ntz AS _inserted_timestamp,
        file_name,
        CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER) AS partition_key
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                start_time => DATEADD('hour', -4, CURRENT_TIMESTAMP()),
                table_name => '{{ source( "bronze_streamline", "block_txs") }}')
            ) A
)
SELECT
    block_id,
    data:error::variant AS error, 
    data:transaction:signatures[0]::string AS tx_id, 
    value:array_index AS index,
    DATA,
    _inserted_timestamp,
    s.partition_key,
    s.value AS VALUE
FROM
    {{ source(
        "bronze_streamline",
        "block_txs"
    ) }}
    s
JOIN 
    meta b
    ON b.file_name = metadata$filename
    AND b.partition_key = s.partition_key
WHERE
    b.partition_key = s.partition_key
    AND (
        data:error:code IS NULL
        OR data:error:message::string LIKE '%Slot % was skipped, or missing in long-term storage%'
        OR data:error:message::string LIKE 'Slot % was skipped, or missing due to ledger jump to recent snapshot'
    )