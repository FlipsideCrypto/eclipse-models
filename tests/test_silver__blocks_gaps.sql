WITH tmp AS (
    SELECT
        block_id,
        previous_block_id,
        block_timestamp
    FROM
        {{ ref('silver__blocks') }}
),
missing AS (
    SELECT
        t1.previous_block_id AS missing_block_id,
        t1.block_timestamp
    FROM
        tmp t1
    LEFT OUTER JOIN 
        tmp t2
        ON t1.previous_block_id = t2.block_id
    WHERE
        t2.block_id IS NULL
        AND t1.previous_block_id IS NOT NULL
),
gaps AS (
    SELECT
        (
            SELECT
                max(block_id)
            FROM
                {{ ref('silver__blocks') }}
            WHERE
                block_id > 0
                AND block_id < missing_block_id
        ) AS tmp__gap_start_block_id,
        missing_block_id AS gap_end_block_id,
    FROM
        missing
    WHERE
        block_timestamp::date < current_date
)
SELECT
    coalesce(tmp__gap_start_block_id, 0) AS gap_start_block_id,
    gap_end_block_id,
    gap_end_block_id - gap_start_block_id AS diff
FROM
    gaps
WHERE
    gap_end_block_id <> 6572202 /* we know that blocks 0 to 6572202 is currently unavailable */