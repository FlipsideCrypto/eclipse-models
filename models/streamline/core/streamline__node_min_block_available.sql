{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

{% set block_to_check = 0 %}

WITH node_response AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'POST',
            'https://eclipse.lgns.net:443',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),
            OBJECT_CONSTRUCT(
                'id',
                {{ block_to_check }},
                'jsonrpc',
                '2.0',
                'method',
                'getBlock',
                'params',
                ARRAY_CONSTRUCT(
                    {{ block_to_check }},
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
        ) AS data
)
SELECT 
    data:data:error:code::int AS error_code,
    data:data:error:message::string AS error_message,
    CASE
        WHEN error_code = -32001 THEN
            split_part(error_message,'available block: ',2)::int
        ELSE
            {{ block_to_check }}
    END AS block_id
FROM 
    node_response