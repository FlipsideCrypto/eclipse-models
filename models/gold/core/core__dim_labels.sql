{{ config(
    materialized = 'incremental',
    unique_key = ['address'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = [enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(address)'), "DELETE FROM {{ this }} WHERE address in (select address from {{ ref('silver__labels') }} where _is_deleted = TRUE);",],
    tags = ['scheduled_non_core']
) }}

SELECT
    blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    project_name AS label,
    address_name,
    labels_combined_id AS dim_labels_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__labels') }}

{% if is_incremental() %}
WHERE 
    modified_timestamp >= (
        SELECT
            max(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
