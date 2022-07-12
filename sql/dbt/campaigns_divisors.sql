--noqa: disable=L026,L031,L034

{{
  config(
    materialized = 'table',
    partition_by = {
      "field": "report_date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

-- Getting Campaigns and their creative ASINs from Campaigns API
-- Using Date here since Campaigns can have different creative ASINs on different dates
WITH ads_api AS (
    SELECT
        CAST(params.report_date AS DATE) AS report_date,
        c.campaignid AS campaign_id,
        2 AS data_source_priority,
        c.creative.asins[SAFE_ORDINAL(1)] AS creative_asin1,
        c.creative.asins[SAFE_ORDINAL(2)] AS creative_asin2,
        c.creative.asins[SAFE_ORDINAL(3)] AS creative_asin3
    FROM
        {{ source("advertising_api", "campaigns") }}
    CROSS JOIN
        UNNEST(payload) AS p
    CROSS JOIN
        UNNEST(p.campaigns) AS c
    QUALIFY
        ROW_NUMBER() OVER(PARTITION BY c.campaignid, params.report_date) = 1
),

asin_brand AS (
    SELECT
        child_asin AS asin,
        brand
    FROM
        {{ ref("amazon_product_referential_master") }}
    QUALIFY
        ROW_NUMBER() OVER(PARTITION BY child_asin ORDER BY last_updated_date DESC) = 1
),

-- All campaigns from OB data
campaigns AS (
    SELECT
        campaign_id,
        campaign_name,
        report_date
    FROM
        (
            SELECT
                campaign_id,
                campaign_name,
                report_date
            FROM
                {{ ref("amzadvertising_sp_ad_groups") }}
            UNION ALL
            SELECT
                campaign_id,
                campaign_name,
                report_date
            FROM
                {{ ref("amzadvertising_sd_ad_groups") }}
            UNION ALL
            SELECT
                campaign_id,
                campaign_name,
                report_date
            FROM
                {{ ref("amzadvertising_hsa_ad_groups") }}
            UNION ALL
            SELECT
                campaign_id,
                campaign_name,
                report_date
            FROM
                {{ ref("amzadvertising_hsa_ad_groups_video") }}
        )
    QUALIFY
        ROW_NUMBER() OVER(PARTITION BY campaign_id, report_date) = 1
),

-- CTE to get campaign_id, ASIN mapping
-- based on ASINs present in Campaign's Name
asin_campaign1 AS (
    SELECT
        campaigns.report_date,
        campaigns.campaign_id,
        campaigns.campaign_name,
        asin_brand.asin,
        ROW_NUMBER() OVER(PARTITION BY campaigns.report_date, campaigns.campaign_id) AS row_num
    FROM
        campaigns
    LEFT JOIN
        asin_brand
        ON campaigns.campaign_name LIKE CONCAT('%', asin_brand.asin, '%')
    {{ dbt_utils.group_by(4) }}
),

-- CTE that selects all rows for row_num = 2
-- from asin_campaign1
asin_campaign2 AS (
    SELECT
        report_date,
        campaign_id,
        asin
    FROM
        asin_campaign1
    WHERE
        row_num = 2
),

-- CTE that selects all rows for row_num = 3
-- from asin_campaign1
asin_campaign3 AS (
    SELECT
        report_date,
        campaign_id,
        asin
    FROM
        asin_campaign1
    WHERE
        row_num = 3
),

asin_campaign AS (
    SELECT
        asin_campaign1.report_date,
        asin_campaign1.campaign_id,
        CASE
            WHEN asin_campaign1.asin IS NULL THEN 3
            ELSE 1
        END AS data_source_priority,
        asin_campaign1.asin AS creative_asin1,
        asin_campaign2.asin AS creative_asin2,
        asin_campaign3.asin AS creative_asin3
    FROM
        asin_campaign1
    LEFT JOIN
        asin_campaign2
        ON asin_campaign1.campaign_id = asin_campaign2.campaign_id
            AND asin_campaign1.report_date = asin_campaign2.report_date
    LEFT JOIN
        asin_campaign3
        ON asin_campaign1.campaign_id = asin_campaign3.campaign_id
            AND asin_campaign1.report_date = asin_campaign3.report_date
    WHERE
        asin_campaign1.row_num = 1
),

-- Combining all Campaigns and their Creative ASINs from OB and API
ads_ob_api AS (
    SELECT
        report_date,
        campaign_id,
        creative_asin1,
        creative_asin2,
        creative_asin3
    FROM
        (
            SELECT
                *
            FROM
                ads_api

            UNION ALL

            SELECT
                *
            FROM
                asin_campaign
        )
    QUALIFY
        ROW_NUMBER() OVER(PARTITION BY report_date, campaign_id ORDER BY data_source_priority) = 1
),

daily_sales AS (
    SELECT
        purchased_date_utc AS sales_date,
        asin,
        SUM(COALESCE(item_price_usd, 0)) AS sales
    FROM
        {{ ref('amazon_pnl_report') }}
    {{ dbt_utils.group_by(2) }}
),

-- Using Ads Bulk Report (Decommissioned) to COALESCE ASINs missing from OB and API
ads_all AS (
    SELECT
        ads_ob_api.report_date,
        ads_ob_api.campaign_id,
        COALESCE(ads_ob_api.creative_asin1, ads_bulk.creative_asin1) AS creative_asin1,
        COALESCE(ads_ob_api.creative_asin2, ads_bulk.creative_asin2) AS creative_asin2,
        COALESCE(ads_ob_api.creative_asin3, ads_bulk.creative_asin3) AS creative_asin3
    FROM
        ads_ob_api
    LEFT JOIN
        (
            SELECT
                campaign_id,
                ANY_VALUE(creative_asins1) AS creative_asin1,
                ANY_VALUE(creative_asins2) AS creative_asin2,
                ANY_VALUE(creative_asins3) AS creative_asin3
            FROM
                {{ source("core", "ads_bulk_operations_v1") }}
            GROUP BY
                campaign_id
        ) AS ads_bulk
        ON ads_ob_api.campaign_id = ads_bulk.campaign_id
),

-- Adding ASIN divisors based on Sales for that date
asin_divisors AS (
    SELECT
        ads_all.report_date,
        ads_all.campaign_id,
        ads_all.creative_asin1,
        SAFE_DIVIDE(IFNULL(ds1.sales, 0),
            IFNULL(ds1.sales, 0) + IFNULL(ds2.sales, 0) + IFNULL(ds3.sales, 0)) AS divisor_1,
        ads_all.creative_asin2,
        SAFE_DIVIDE(IFNULL(ds2.sales, 0),
            IFNULL(ds1.sales, 0) + IFNULL(ds2.sales, 0) + IFNULL(ds3.sales, 0)) AS divisor_2,
        ads_all.creative_asin3,
        SAFE_DIVIDE(IFNULL(ds3.sales, 0),
            IFNULL(ds1.sales, 0) + IFNULL(ds2.sales, 0) + IFNULL(ds3.sales, 0)) AS divisor_3
    FROM
        ads_all
    LEFT JOIN
        daily_sales AS ds1
        ON ads_all.creative_asin1 = ds1.asin
            AND ads_all.report_date = ds1.sales_date
    LEFT JOIN
        daily_sales AS ds2
        ON ads_all.creative_asin2 = ds2.asin
            AND ads_all.report_date = ds2.sales_date
    LEFT JOIN
        daily_sales AS ds3
        ON ads_all.creative_asin3 = ds3.asin
            AND ads_all.report_date = ds3.sales_date
)

SELECT
    report_date,
    campaign_id,
    creative_asin1,
    creative_asin2,
    creative_asin3,
    {% for n in range(3) %}
        CASE
            WHEN
                NOT(creative_asin1 IS NULL OR creative_asin2 IS NULL OR creative_asin3 IS NULL) AND divisor_1 IS NULL AND divisor_2 IS NULL AND divisor_3 IS NULL
                THEN 1/3
            {% if n != 2 %}
            WHEN
                NOT(creative_asin1 IS NULL OR creative_asin2 IS NULL) AND divisor_1 IS NULL AND divisor_2 IS NULL
                THEN 0.5
            {% endif %}
            {% if n == 0 %}
            WHEN
                creative_asin1 IS NOT NULL AND creative_asin2 IS NULL AND creative_asin3 IS NULL AND divisor_1 IS NULL
                THEN 1
            {% endif %}
            ELSE
                IFNULL(divisor_{{n+1}}, 0)
        END AS divisor_{{n+1}}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM
    asin_divisors
WHERE
    creative_asin1 IS NOT NULL
