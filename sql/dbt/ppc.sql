-- noqa: disable= L016
{{ config(
    materialized = 'table',
    partition_by = {
      "field": "week_end",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

-- Incrementality Factor values based on H10 rank (for Branded Search Terms only)
{% set branded_rank_1_4 = 0.1 %}
{% set branded_rank_5_14 = 0.25 %}
{% set branded_rank_15_40 = 0.5 %}
{% set branded_rank_default = 0.75 %}

-- Incrementality Factor values based on H10 rank (for non-Branded Search Terms only)
{% set non_branded_rank_1_4 = 0.2 %}

-- Contribution Margin values basis product/inventory status
{% set status_deplete_liquidate = 0.3 %}
{% set status_overstock = 0.34 %}
{% set status_new = 1 %}

WITH rf_products_latest AS (
    SELECT
        asin,
        title,
        brand,
        amazon_domain
    FROM
        {{ ref('rf_products') }}
    WHERE
        scrap_rank = 1
        AND bsr_tree_rank = 1
),

phrase_search_volume AS (
    SELECT
        year,
        week,
        phrase,
        search_volume
    FROM
        {{ ref('stg_ppc_incrementality_latest_phrase_search_volume') }}
),

unioned AS (
    SELECT
        week,
        year,
        week_start,
        week_end,
        branded_asin,
        'Sponsored Product' AS ad_type,
        'Keyword' AS target_type,
        keyword_text AS keyword_or_target,
        match_type,
        query,
        cost,
        impressions,
        clicks,
        orders,
        sales,
        ctr,
        cpc,
        acos,
        h10_rank,
        COALESCE(fba_fees, 0) AS fba_fees,
        COALESCE(cogs, 0) AS cogs,
        COALESCE(promo, 0) AS promo,
        COALESCE(refunded, 0) AS refunded,
        COALESCE(google_cost, 0) AS google_cost,
        COALESCE(dsp_cost, 0) AS dsp_cost,
        ppc_incrementality_factor
    FROM
        {{ ref('ppc_incrementality_sp_kw_query') }}

    UNION ALL

    SELECT
        week,
        year,
        week_start,
        week_end,
        branded_asin,
        'Sponsored Product' AS ad_type,
        'Target' AS target_type,
        targeting_text AS keyword_or_target,
        '' AS match_type,
        '' AS query,
        cost,
        impressions,
        clicks,
        orders,
        sales,
        ctr,
        cpc,
        acos,
        NULL AS h10_rank,
        COALESCE(fba_fees, 0) AS fba_fees,
        COALESCE(cogs, 0) AS cogs,
        COALESCE(promo, 0) AS promo,
        COALESCE(refunded, 0) AS refunded,
        COALESCE(google_cost, 0) AS google_cost,
        COALESCE(dsp_cost, 0) AS dsp_cost,
        ppc_incrementality_factor
    FROM
        {{ ref('ppc_incrementality_sp_target_ads') }}

    UNION ALL

    SELECT
        week,
        year,
        week_start,
        week_end,
        branded_asin,
        'Sponsored Display' AS ad_type,
        'Target' AS target_type,
        targeting_text AS keyword_or_target,
        '' AS match_type,
        '' AS query,
        cost,
        impressions,
        clicks,
        orders,
        sales,
        ctr,
        cpc,
        acos,
        NULL AS h10_rank,
        COALESCE(fba_fees, 0) AS fba_fees,
        COALESCE(cogs, 0) AS cogs,
        COALESCE(promo, 0) AS promo,
        COALESCE(refunded, 0) AS refunded,
        COALESCE(google_cost, 0) AS google_cost,
        COALESCE(dsp_cost, 0) AS dsp_cost,
        ppc_incrementality_factor
    FROM
        {{ ref('ppc_incrementality_sd_target_ads') }}

    UNION ALL

    SELECT
        week,
        year,
        week_start,
        week_end,
        branded_asin,
        'Sponsored Brand' AS ad_type,
        'Keyword' AS target_type,
        keyword_text AS keyword_or_target,
        match_type,
        '' AS query,
        cost,
        impressions,
        clicks,
        orders,
        sales,
        ctr,
        cpc,
        acos,
        h10_rank,
        COALESCE(fba_fees, 0) AS fba_fees,
        COALESCE(cogs, 0) AS cogs,
        COALESCE(promo, 0) AS promo,
        COALESCE(refunded, 0) AS refunded,
        COALESCE(google_cost, 0) AS google_cost,
        COALESCE(dsp_cost, 0) AS dsp_cost,
        ppc_incrementality_factor
    FROM
        {{ ref('ppc_incrementality_sb_kw_query') }}

    UNION ALL

    SELECT
        week,
        year,
        week_start,
        week_end,
        branded_asin,
        'Sponsored Brand' AS ad_type,
        'Target' AS target_type,
        targeting_text AS keyword_or_target,
        '' AS match_type,
        '' AS query,
        cost,
        impressions,
        clicks,
        orders,
        sales,
        ctr,
        cpc,
        acos,
        NULL AS h10_rank,
        COALESCE(fba_fees, 0) AS fba_fees,
        COALESCE(cogs, 0) AS cogs,
        COALESCE(promo, 0) AS promo,
        COALESCE(refunded, 0) AS refunded,
        COALESCE(google_cost, 0) AS google_cost,
        COALESCE(dsp_cost, 0) AS dsp_cost,
        ppc_incrementality_factor
    FROM
        {{ ref('ppc_incrementality_sb_target_ads') }}

    UNION ALL

    SELECT
        week,
        year,
        week_start,
        week_end,
        branded_asin,
        'Sponsored Brand Video' AS ad_type,
        'Keyword' AS target_type,
        keyword_text AS keyword_or_target,
        match_type,
        '' AS query,
        cost,
        impressions,
        clicks,
        orders,
        sales,
        ctr,
        cpc,
        acos,
        h10_rank,
        COALESCE(fba_fees, 0) AS fba_fees,
        COALESCE(cogs, 0) AS cogs,
        COALESCE(promo, 0) AS promo,
        COALESCE(refunded, 0) AS refunded,
        COALESCE(google_cost, 0) AS google_cost,
        COALESCE(dsp_cost, 0) AS dsp_cost,
        ppc_incrementality_factor
    FROM
        {{ ref('ppc_incrementality_sbv_kw_query') }}
),

wbr_asin_mapping AS (
    SELECT
        EXTRACT(YEAR FROM update_date) AS year,
        EXTRACT(WEEK FROM update_date) AS week,
        child_asin,
        product_status,
        inventory_status
    FROM
        {{ ref('asin_mapping_daily') }}
    QUALIFY
        ROW_NUMBER() OVER(PARTITION BY child_asin, EXTRACT(YEAR FROM update_date), EXTRACT(WEEK FROM update_date) ORDER BY update_date DESC) = 1
),

ppc_incrementality AS (
    SELECT
        *,
        IF(branded_search IS TRUE,
            CASE
                WHEN h10_rank < 5
                    THEN {{ branded_rank_1_4 }}
                WHEN h10_rank < 15
                    THEN {{ branded_rank_5_14 }}
                WHEN h10_rank < 41
                    THEN {{ branded_rank_15_40 }}
                ELSE {{ branded_rank_default }}
            END,
            CASE
                WHEN h10_rank < 5
                    THEN {{ non_branded_rank_1_4 }}
                ELSE
                    ppc_incrementality_factor
            END) AS ppc_incrementality_factor_new,
        CASE
            WHEN LOWER(inventory_status) IN ("liquidate", "deplete")
                THEN {{ status_deplete_liquidate }} + (0.5 * SAFE_DIVIDE(cogs, sales))
            WHEN LOWER(inventory_status) = "overstock"
                THEN {{ status_overstock }}
            WHEN LOWER(product_status) IN ("new")
                THEN {{ status_new }}
            ELSE cm_perc_true
        END AS cm_perc_framework
    FROM
        (
            SELECT
                unioned.*,
                phrase_search_volume.search_volume AS keyword_search_volume,
                wbr_asin_mapping.product_status,
                wbr_asin_mapping.inventory_status,
                UPPER(TRIM(LPAD(TRIM(rf_products_latest.title), 50))) AS title,
                UPPER(rf_products_latest.brand) AS brand,
                CASE
                    WHEN unioned.target_type = 'Keyword' THEN 'Keyword'
                    WHEN unioned.keyword_or_target LIKE '%asin%' THEN 'Asin'
                    WHEN unioned.keyword_or_target LIKE '%category%' THEN 'Category'
                    ELSE unioned.target_type
                END AS target_type_new,
                SAFE_DIVIDE(unioned.fba_fees, unioned.sales - unioned.promo) AS fba_fees_perc,
                SAFE_DIVIDE(unioned.cogs, unioned.sales - unioned.promo) AS cogs_perc,
                SAFE_DIVIDE(unioned.promo, unioned.sales - unioned.promo) AS promo_perc,
                SAFE_DIVIDE(unioned.refunded, unioned.sales - unioned.promo) AS refunded_perc,
                SAFE_DIVIDE(unioned.google_cost, unioned.sales - unioned.promo) AS google_perc,
                SAFE_DIVIDE(unioned.dsp_cost, unioned.sales - unioned.promo) AS dsp_perc,
                SAFE_DIVIDE(unioned.fba_fees, unioned.orders) AS fba_fees_unit,
                SAFE_DIVIDE(unioned.cogs, unioned.orders) AS cogs_unit,
                SAFE_DIVIDE(unioned.promo, unioned.orders) AS promo_unit,
                SAFE_DIVIDE(unioned.refunded, unioned.orders) AS refunded_unit,
                SAFE_DIVIDE(unioned.google_cost, unioned.orders) AS google_unit,
                SAFE_DIVIDE(unioned.dsp_cost, unioned.orders) AS dsp_unit,
                CASE
                    WHEN unioned.sales = 0
                        THEN 0
                    ELSE SAFE_DIVIDE((unioned.sales - unioned.fba_fees - unioned.cogs - unioned.promo - unioned.refunded - unioned.google_cost - unioned.dsp_cost), unioned.sales - unioned.promo)
                END AS cm_perc_true,
                CASE
                    WHEN rf_products_latest.brand IS NULL
                        THEN FALSE
                    WHEN LOWER(unioned.query) LIKE ('%' || LOWER(rf_products_latest.brand) || '%')
                        THEN TRUE
                    WHEN LOWER(unioned.keyword_or_target) LIKE ('%' || LOWER(rf_products_latest.brand) || '%') AND unioned.target_type = 'Keyword'
                        THEN TRUE
                    ELSE FALSE
                END AS branded_search
            FROM
                unioned
            LEFT JOIN
                rf_products_latest
                ON rf_products_latest.asin = unioned.branded_asin
                    AND LOWER(rf_products_latest.amazon_domain) = 'amazon.com'
            LEFT JOIN
                phrase_search_volume
                ON phrase_search_volume.phrase = unioned.keyword_or_target
                    AND phrase_search_volume.week = unioned.week
                    AND phrase_search_volume.year = unioned.year
            LEFT JOIN
                wbr_asin_mapping
                ON unioned.branded_asin = wbr_asin_mapping.child_asin
                    AND unioned.week = wbr_asin_mapping.week
                    AND unioned.year = wbr_asin_mapping.year
            WHERE
                (unioned.cost + unioned.impressions + unioned.clicks + unioned.orders + unioned.sales) > 0
                AND rf_products_latest.brand IS NOT NULL
        )
)

SELECT
    week,
    year,
    week_start,
    week_end,
    branded_asin,
    title,
    brand,
    product_status,
    inventory_status,
    ad_type,
    target_type_new AS target_type,
    keyword_or_target,
    branded_search,
    keyword_search_volume,
    match_type,
    query,
    cost,
    impressions,
    clicks,
    orders,
    sales - promo AS sales,
    ctr,
    cpc,
    acos,
    h10_rank,
    fba_fees,
    cogs,
    promo,
    refunded,
    google_cost,
    dsp_cost,
    ppc_incrementality_factor_new AS ppc_incrementality_factor,
    fba_fees_perc,
    cogs_perc,
    promo_perc,
    refunded_perc,
    google_perc,
    dsp_perc,
    fba_fees_unit,
    cogs_unit,
    promo_unit,
    refunded_unit,
    google_unit,
    dsp_unit,
    cm_perc_true,
    cm_perc_framework,
    CASE
        WHEN cm_perc_true >= cm_perc_framework
            THEN "TRUE"
        WHEN cm_perc_true < cm_perc_framework
            THEN "FRAMEWORK"
    END AS cm_applied,
    CASE
        WHEN sales = 0
            THEN -1 * cost
        ELSE
            (sales * cm_perc_true) - cost
    END AS ebitda_true_cm,
    CASE
        WHEN sales = 0
            THEN -1 * cost
        ELSE
            ((sales * cm_perc_true) * ppc_incrementality_factor_new) - cost
    END AS incremental_ebitda_true_cm,
    CASE
        WHEN sales = 0
            THEN cost * -1
        ELSE
            (sales * GREATEST(cm_perc_true, cm_perc_framework)) - cost
    END AS ebitda_framework_cm,
    CASE
        WHEN sales = 0
            THEN -1 * cost
        ELSE
            ((sales * GREATEST(cm_perc_true, cm_perc_framework)) * ppc_incrementality_factor_new) - cost
    END AS incremental_ebitda_framework_cm
FROM
    ppc_incrementality
