{{ config(
    materialized = 'view',
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
    labels_combined_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ source(
        'crosschain_silver',
        'labels_combined'
    ) }}
WHERE
    blockchain = 'eclipse'