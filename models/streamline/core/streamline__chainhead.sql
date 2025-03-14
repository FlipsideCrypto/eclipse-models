{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}/token/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state',
            'livequery'
        ),
        OBJECT_CONSTRUCT(
            'id',
            0,
            'jsonrpc',
            '2.0',
            'method',
            'getSlot',
            'params',
            []
        ),
        'Vault/prod/eclipse/private/mainnet'
    ) :data :result :: INT - 50 AS block_id
