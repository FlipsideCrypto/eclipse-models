{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

{% set block_to_check = 0 %}
WITH node_response AS (

    SELECT
        {{ target.database }}.live.udf_api(
            'POST',
            '{Service}/token/{Authentication}',
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
                        FALSE,
                        'transactionDetails',
                        'none',
                        'maxSupportedTransactionVersion',
                        0
                    )
                )
            ),
            'Vault/prod/eclipse/private/mainnet'
        ) AS DATA
)
SELECT
    DATA :data :error :code :: INT AS error_code,
    DATA :data :error :message :: STRING AS error_message,
    CASE
        WHEN error_code = -32001 THEN SPLIT_PART(
            error_message,
            'available block: ',
            2
        ) :: INT
        ELSE {{ block_to_check }}
    END AS block_id
FROM
    node_response
