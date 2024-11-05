{{ config(
    materialized = 'incremental',
    unique_key = ['address', 'blockchain'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
    post_hook = [enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(address)'), "DELETE FROM {{ this }} WHERE _is_deleted = TRUE;",],
    tags = ['scheduled_non_core']
) }}

SELECT
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    _is_deleted,
    labels_combined_id as labels_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'labels_combined'
    ) }}
WHERE
    blockchain = 'eclipse'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        max(
            modified_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}