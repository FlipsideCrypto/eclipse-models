{{ config(
    materialized = 'incremental',
    unique_key = ["token_mint_actions_id"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
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

WITH base_mint_actions AS (
    SELECT
        *
    FROM
        {{ ref('silver__mint_actions') }}
    {% if is_incremental() %}
        WHERE _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.succeeded,
    A.index,
    COALESCE(A.inner_index, -1) as inner_index,
    A.event_type,
    A.mint,
    A.mint_amount,
    A.mint_authority,
    A.token_account,
    A.signers,
    b.decimal,
    b.mint_standard_type,
     A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['A.tx_id', 'A.index', 'inner_index', 'A.mint']
    ) }} AS token_mint_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_mint_actions A
    INNER JOIN {{ ref('silver__mint_types') }} b
    ON A.mint = b.mint
WHERE
    b.mint_type = 'token'
