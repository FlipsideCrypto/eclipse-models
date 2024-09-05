{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','tx_id','index'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_core']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    b.index,
    b.value:accountIndex::integer AS account_index,
    t.account_keys[account_index]:pubkey::string AS account,
    b.value:mint::string AS mint,
    b.value:owner::string AS owner,
    b.value:uiTokenAmount:amount::integer AS amount,
    b.value:uiTokenAmount:decimals AS decimal,
    coalesce(b.value:uiTokenAmount:uiAmount::float,0) AS ui_amount,
    b.value:uiTokenAmount:uiAmountString::string AS ui_amount_string,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id','tx_id','b.index']
    ) }} AS _pre_token_balance_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__transactions') }} t,
    table(flatten(pre_token_balances)) b
{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    _inserted_timestamp::date = '2024-08-30' /* TODO replace with whenever we start getting data in PROD */
{% endif %}