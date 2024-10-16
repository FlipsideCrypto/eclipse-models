{{ config(
    materialized = 'incremental',
    unique_key = ["mint_actions_id"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
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

WITH base AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INDEX,
        NULL AS inner_index,
        program_id,
        event_type,
        instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        event_type IN (
            'mintTo',
            'initializeMint',
            'mintToChecked',
            'initializeMint2',
            'initializeConfidentialTransferMint',
            'initializeNonTransferableMint'
        )
    {% if is_incremental() %}
        AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
        AND _inserted_timestamp :: DATE = '2024-09-12'
    {% endif %}
    UNION ALL
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        instruction_index AS INDEX,
        inner_index,
        program_id,
        event_type,
        instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events_inner') }}
    WHERE
        event_type IN (
            'mintTo',
            'initializeMint',
            'mintToChecked',
            'initializeMint2',
            'initializeConfidentialTransferMint',
            'initializeNonTransferableMint'
        )
    {% if is_incremental() %}
        AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
        AND _inserted_timestamp :: DATE = '2024-09-12'
    {% endif %}
),
prefinal AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        INDEX,
        inner_index,
        event_type,
        instruction :parsed :info :mint :: STRING AS mint,
        instruction :parsed :info :account :: STRING AS token_account,
        instruction :parsed :info :decimals :: INTEGER AS DECIMAL,
        COALESCE(
            instruction :parsed :info :amount :: INTEGER,
            instruction :parsed :info :tokenAmount: amount :: INTEGER
        ) AS mint_amount,
        COALESCE(
            instruction :parsed :info :mintAuthority :: STRING,
            instruction :parsed :info :multisigMintAuthority :: STRING,
            instruction :parsed :info :authority :: STRING
        ) AS mint_authority,
        instruction :parsed :info :signers :: STRING AS signers,
        _inserted_timestamp
    FROM
        base
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    INDEX,
    inner_index,
    event_type,
    mint,
    token_account,
    DECIMAL,
    mint_amount,
    CASE
        WHEN event_type IN (
            'initializeConfidentialTransferMint',
            'initializeNonTransferableMint'
        ) THEN FIRST_VALUE(mint_authority) ignore nulls over (
            PARTITION BY tx_id
            ORDER BY
                CASE
                    WHEN mint_authority IS NOT NULL THEN 0
                    ELSE 1
                END,
                INDEX
        )
        ELSE mint_authority
    END AS mint_authority,
    signers,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'mint', 'index', 'inner_index']
    ) }} AS mint_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    prefinal
