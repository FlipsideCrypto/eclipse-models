{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    0 AS block_id
UNION
SELECT
    _id AS block_id
FROM
    {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
WHERE
    _id <= (
        SELECT
            MAX(block_id)
        FROM
            {{ ref('streamline__chainhead') }}
    )