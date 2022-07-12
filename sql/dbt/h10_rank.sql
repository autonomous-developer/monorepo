{{
  config(
    materialized = 'incremental',
    unique_key = 'unique_key',
    partition_by = {
      "field": "wc_date",
      "data_type": "date",
      "granularity": "day"
    },
    incremental_strategy = 'insert_overwrite'
  )
}}

WITH date_dimension AS (
    SELECT
        week_number,
        year,
        MIN(full_date) AS wc_date,
        MAX(full_date) AS we_date
    FROM
        {{ ref('date_dimension') }}
    {{ dbt_utils.group_by(2) }}
),

raw_cerebro AS (
    SELECT
        EXTRACT(YEAR FROM cerebro_rank.timestamp_ingested) AS year,
        EXTRACT(WEEK FROM cerebro_rank.timestamp_ingested) AS week,
        date_dimension.wc_date,
        date_dimension.we_date,
        cerebro_rank.asin,
        cerebro_rank.phrase,
        cerebro_rank.rank AS h10_rank
    FROM
        {{ source('h10','cerebro_rank') }} AS cerebro_rank
    INNER JOIN
        {{ source('niches', 'master') }} AS niche_master
        ON niche_master.niche_id = cerebro_rank.niche_id
    LEFT JOIN
        date_dimension
        ON EXTRACT(YEAR FROM cerebro_rank.timestamp_ingested) = date_dimension.year
            AND EXTRACT(WEEK FROM cerebro_rank.timestamp_ingested) = date_dimension.week_number
    WHERE
        niche_master.marketplace_id = 'ATVPDKIKX0DER'
    {% if is_incremental() %}
        AND DATE(cerebro_rank.timestamp_ingested) >= CURRENT_DATE()
    {% endif %}
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY
                EXTRACT(YEAR FROM cerebro_rank.timestamp_ingested),
                EXTRACT(WEEK FROM cerebro_rank.timestamp_ingested),
                cerebro_rank.asin,
                cerebro_rank.phrase
            ORDER BY
                cerebro_rank.timestamp_ingested DESC
        ) = 1
)

-- avg_rank: We take average of rank over last 4 weeks for an asin-phrase if current week is `NULL`
SELECT
    CONCAT(wc_date, asin, phrase) AS unique_key,
    year,
    week,
    wc_date,
    we_date,
    asin,
    phrase,
    SAFE_CAST(h10_rank AS INT64) AS h10_rank
FROM
    raw_cerebro
