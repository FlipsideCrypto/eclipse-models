  -- depends_on: {{ ref('streamline__node_min_block_available') }}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"blocks",
        "sql_limit" :"200000",
        "producer_batch_size" :"200000",
        "worker_batch_size" :"20000",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "block_id" }
    )
) }}

{% if execute %}
    {% set min_block_query %}
    SELECT min(block_id) FROM {{ ref('streamline__node_min_block_available') }}
    {% endset %}

    {% set min_block_id = run_query(min_block_query)[0][0] %}
{% endif %}

WITH blocks AS (
    SELECT
        block_id
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        /* Find the earliest block available from the node provider */
        block_id >= {{ min_block_id }}
    EXCEPT
    SELECT
        block_id
    FROM
        {{ ref('streamline__blocks_complete') }}
)
SELECT
    block_id,
    ROUND(
        block_id,
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