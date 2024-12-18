{{ config(
    materialized = 'incremental',
    tags = ['scheduled_non_core'],
    full_refresh = false
) }}

WITH lq AS (

    SELECT
        {{ target.database }}.live.udf_api(
            '{Service}/main/info',
            'Vault/prod/eclipse/solar_dex'
        ) :data :data AS response
)
SELECT
    SYSDATE() AS recorded_at,
    'sooGfQwJ6enHfLTPfasFZtFR7DgobkJD77maDNEqGkD' AS program_id,
    response :tvl :: FLOAT AS tvl,
    response :users :: INT AS users,
    response :volume24 :: FLOAT AS volume_24,
    {{ dbt_utils.generate_surrogate_key(
        ['recorded_at']
    ) }} AS fact_solar_dex_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    lq
WHERE
    response IS NOT NULL
