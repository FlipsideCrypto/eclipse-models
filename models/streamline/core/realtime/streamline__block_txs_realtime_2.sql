-- depends_on: {{ ref('streamline__node_min_block_available') }}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"block_txs_2",
        "sql_limit" :"20000",
        "producer_batch_size" :"20000",
        "worker_batch_size" :"500",
        "async_concurrent_requests": "10",
        "exploded_key": tojson(["result.transactions"]),
        "include_top_level_json": tojson(["result.blockTime"]),
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "block_id", }
    )
) }}

-- TO-DO: use min_block_id OR remove it and use static cutoff like solana

{% if execute %}
    {% set min_block_query %}

    SELECT
        MIN(block_id)
    FROM
        {{ ref('streamline__node_min_block_available') }}

        {% endset %}
        {% set min_block_id = run_query(min_block_query) [0] [0] %}
    {% endif %}

WITH blocks AS (
    SELECT
        block_id
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_id >= 49901565 /* TODO: cutoff block_id in PROD after deploy */
    EXCEPT
    SELECT
        block_id
    FROM
        {{ ref('streamline__block_txs_complete') }}
    WHERE
        block_id <= 49901565 /* TODO: cutoff block_id in PROD after deploy */
    EXCEPT
    SELECT 
        block_id
    FROM
        {{ ref('streamline__block_txs_complete_2') }}
)
SELECT
    block_id,
    ROUND(
        block_id,
        -4
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}',
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
                    FALSE,
                    'transactionDetails',
                    'full',
                    'maxSupportedTransactionVersion',
                    0
                )
            )
        ),
        'Vault/prod/eclipse/mainnet'
    ) AS request
FROM
    blocks
