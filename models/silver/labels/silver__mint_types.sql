{{ config(
    materialized = 'incremental',
    unique_key = ["mint_types_id"],
    tags = ['scheduled_non_core'],
) }}

{% if execute %}
    {% if is_incremental() %}
    {% set max_inserted_query %}
    SELECT
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ this }}
    {% endset %}
    {% set max_inserted_timestamp = run_query(max_inserted_query)[0][0] %}
    {% endif %}
{% endif %}

WITH initialization AS (
    SELECT
        *,
        CASE
            WHEN DECIMAL = 0 THEN 'nft'
            WHEN DECIMAL > 0 THEN 'token'
            ELSE NULL
        END AS mint_type
    FROM
        {{ ref('silver__mint_actions') }}
    WHERE
        event_type IN (
            'initializeMint',
            'initializeMint2'
        )
        AND succeeded
    {% if is_incremental() %}
        AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
),
base_metaplex_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        NULL AS inner_index,
        program_id,
        instruction :accounts AS accounts,
        ARRAY_SIZE(accounts) AS num_accounts
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
        AND succeeded
    {% if is_incremental() %}
        AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
    UNION
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        instruction_index AS INDEX,
        inner_index,
        program_id,
        instruction :accounts AS accounts,
        ARRAY_SIZE(accounts) AS num_accounts
    FROM
        {{ ref('silver__events_inner') }}
    WHERE
        program_id = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
        AND succeeded
    {% if is_incremental() %}
        AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
),
metaplex_mint_events AS (
    SELECT
        *,
        CASE
            WHEN num_accounts = 9
            AND accounts [6] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND accounts [7] = '11111111111111111111111111111111'
            AND accounts [8] = 'SysvarRent111111111111111111111111111111111' THEN 'Create Master Edition'
            WHEN num_accounts = 8
            AND accounts [6] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND accounts [7] = '11111111111111111111111111111111' THEN 'Create Master Edition V3'
            WHEN num_accounts = 11
            AND accounts [9] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND accounts [10] = '11111111111111111111111111111111'
            AND accounts [11] = 'SysvarRent111111111111111111111111111111111' THEN 'Create Master Edition Deprecated'
            WHEN num_accounts = 9
            AND accounts [6] = '11111111111111111111111111111111'
            AND accounts [7] = 'Sysvar1nstructions1111111111111111111111111'
            AND accounts [8] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' THEN 'Create'
            WHEN num_accounts IN (
                13,
                14
            )
            AND accounts [11] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND accounts [12] :: STRING = '11111111111111111111111111111111' THEN 'Edition'
            WHEN num_accounts = 6
            AND accounts [5] = '11111111111111111111111111111111' THEN 'Create Metadata Account V3'
            WHEN num_accounts = 7
            AND accounts [5] = '11111111111111111111111111111111'
            AND accounts [6] = 'SysvarRent111111111111111111111111111111111' THEN 'Create Metadata Account'
        END AS metaplex_event_type,
        CASE
            WHEN metaplex_event_type = 'Edition' THEN accounts [3] :: STRING
            WHEN metaplex_event_type = 'Create' THEN accounts [2] :: STRING
            WHEN metaplex_event_type IS NOT NULL THEN accounts [1] :: STRING
            ELSE NULL
        END AS mint
    FROM
        base_metaplex_events
    WHERE
        metaplex_event_type IS NOT NULL
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY mint
            ORDER BY
                CASE
                    WHEN metaplex_event_type IN (
                        'Create Master Edition Deprecated',
                        'Create Master Edition V3',
                        'Create Master Edition'
                    ) THEN 1
                    WHEN metaplex_event_type = 'Edition' THEN 2
                    WHEN metaplex_event_type IN (
                        'Create Metadata account',
                        'Create',
                        'Create Metadata Account V3'
                    ) THEN 3
                    ELSE 4
                END
        ) AS rn
    FROM
        metaplex_mint_events
),
nonfungibles AS (
    SELECT
        A.tx_id,
        A.mint,
        A.decimal,
        A.mint_type,
        CASE
            WHEN b.metaplex_event_type = 'Edition'
            AND A.decimal = 0 THEN 'Edition'
            WHEN b.metaplex_event_type IN (
                'Create Master Edition Deprecated',
                'Create Master Edition V3',
                'Create Master Edition'
            )
            AND A.decimal = 0 THEN 'NonFungible'
            WHEN b.metaplex_event_type IN ('Create')
            AND accounts [1] <> 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
            AND A.decimal = 0 THEN 'NonFungible'
        END AS mint_standard_type,
        A._inserted_timestamp
    FROM
        initialization A
        LEFT JOIN ranked b
        ON A.mint = b.mint
    WHERE
        b.rn = 1
        AND mint_standard_type IS NOT NULL
),
fungibles_and_others AS (
    SELECT
        A.mint,
        A.decimal,
        A.mint_type,
        CASE
            WHEN b.metaplex_event_type IN ('Create')
            AND accounts [1] = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
            AND A.decimal = 0 THEN 'FungibleAsset'
            WHEN b.metaplex_event_type IN (
                'Create Metadata Account',
                'Create',
                'Create Metadata Account V3'
            )
            AND A.decimal = 0 THEN 'FungibleAsset'
            WHEN b.metaplex_event_type IN (
                'Create Metadata Account',
                'Create',
                'Create Metadata Account V3'
            )
            AND A.decimal > 0 THEN 'Fungible'
        END AS mint_standard_type,
        A._inserted_timestamp
    FROM
        initialization A
        LEFT JOIN ranked b
        ON A.mint = b.mint
        AND b.rn = 1
    WHERE
        A.mint NOT IN (
            SELECT
                mint
            FROM
                nonfungibles
        )
        AND A.mint_type IS NOT NULL qualify(ROW_NUMBER() over (PARTITION BY A.mint
    ORDER BY
        A._inserted_timestamp DESC)) = 1
),
prefinal AS (
    SELECT
        mint,
        DECIMAL,
        mint_type,
        mint_standard_type,
        _inserted_timestamp
    FROM
        nonfungibles
    UNION ALL
    SELECT
        mint,
        DECIMAL,
        mint_type,
        mint_standard_type,
        _inserted_timestamp
    FROM
        fungibles_and_others
)
SELECT
    mint,
    DECIMAL,
    mint_type,
    mint_standard_type,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['mint']
    ) }} AS mint_types_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    prefinal
