-- depends_on: {{ ref('streamline__node_min_block_available') }}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"block_txs",
        "sql_limit" :"20000",
        "producer_batch_size" :"20000",
        "worker_batch_size" :"500",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "block_id",
        "exploded_key": tojson(["result.transactions"]),
        "async_concurrent_requests" :"10" }
    )
) }}

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
            /* Find the earliest block available from the node provider */
            block_id >= 6572203
        EXCEPT
        SELECT
            block_id
        FROM
            {{ ref('streamline__block_txs_complete') }}
    )
SELECT
    block_id,
    ROUND(
        block_id,
        -4
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}/token/{Authentication}',
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
        'Vault/prod/eclipse/private/mainnet'
    ) AS request
FROM
    blocks
