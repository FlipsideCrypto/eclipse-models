{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    full_refresh = false,
    tags = ['observability']
) }}

/* CURRENTLY UNUSED UNTIL WE HAVE A WAY TO VERIFY TX COUNTS PER BLOCK */
WITH base as (
    SELECT 
        block_id,
        tx_id,
        _inserted_timestamp
    FROM 
        {{ ref('silver__transactions') }}
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp > (SELECT max(_inserted_timestamp) FROM {{ this }})
    {% else %}
    WHERE 
        block_id >= 6572203 --TODO current min block available
    {% endif %}
    UNION ALL 
    SELECT 
        block_id,
        tx_id,
        _inserted_timestamp
    FROM 
        {{ ref('silver__votes') }}
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp > (SELECT max(_inserted_timestamp) FROM {{ this }})
    {% else %}
    WHERE 
        block_id >= 6572203 --TODO current min block available
    {% endif %}
)
SELECT 
    block_id,
    count(DISTINCT tx_id) AS transaction_count,
    max(_inserted_timestamp) AS _inserted_timestamp
FROM 
    base 
GROUP BY 1