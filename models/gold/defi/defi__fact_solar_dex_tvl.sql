{{ config(
    materialized = 'incremental',
    unique_key = ["block_date"],
    tags = ['scheduled_non_core']
) }}

WITH lq AS (

    SELECT
        {{ target.database }}.live.udf_api('https://api.solarstudios.co/main/info') :data :data AS response
)
SELECT
    SYSDATE() :: DATE AS block_date,
    response :tvl :: FLOAT AS tvl,
    response :users :: INT AS users,
    response :volume24 :: FLOAT AS volume_24,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date']
    ) }} AS fact_solar_dex_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    lq
