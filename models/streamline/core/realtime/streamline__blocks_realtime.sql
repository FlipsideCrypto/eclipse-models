{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"blocks",
        "sql_limit" :"10000",
        "producer_batch_size" :"10000",
        "worker_batch_size" :"10000",
        "sql_source" :"{{this.identifier}}" }
    )
) }}

WITH blocks AS (
    SELECT
        block_id
    FROM
        {{ ref("streamline__blocks") }}
    EXCEPT
    SELECT
        block_id
    FROM
        {{ ref('streamline__complete_blocks') }}
)
SELECT
    ROUND(
        block_number,
        -5
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        'https://mainnetbeta-rpc.eclipse.xyz',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_id,
            'jsonrpc',
            '2.0',
            'method',
            'getBlock',
            'params',
            ARRAY_CONSTRUCT(
                block_id,
                OBJECT_CONSTRUCT(
                    'encoding',
                    'jsonParsed',
                    'rewards',
                    False,
                    'transactionDetails',
                    'none',
                    'maxSupportedTransactionVersion',
                    0
                )
            )
        )
    ) AS request
FROM
    blocks
ORDER BY
    block_id