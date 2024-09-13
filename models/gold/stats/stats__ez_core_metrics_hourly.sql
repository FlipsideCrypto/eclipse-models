{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'block_timestamp_hour',
        cluster_by = ['block_timestamp_hour'],
        meta ={ 
            'database_tags': { 
                'table': { 
                    'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
                }
            }
        },
        tags = ['scheduled_non_core','exclude_change_tracking']
    ) 
}}

SELECT
    block_timestamp_hour,
    block_number_min,
    block_number_max,
    block_count,
    transaction_count,
    transaction_count_success,
    transaction_count_failed,
    unique_signers_count,
    total_fees AS total_fees_native,
    ROUND(
        (
            total_fees / pow(
                10,
                9
            )
        ) * p.price,
        2
    ) AS total_fees_usd,
    core_metrics_hourly_id AS ez_core_metrics_hourly_id,
    s.inserted_timestamp AS inserted_timestamp,
    s.modified_timestamp AS modified_timestamp
FROM
    {{ ref('silver__core_metrics_hourly') }} s
LEFT JOIN 
    {{ ref('silver__complete_native_prices') }} p
    ON s.block_timestamp_hour = p.hour
{% if is_incremental() %}
WHERE
    s.modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}