WITH st_monthly_spends AS (
SELECT
  Customer_Search_Term ,
  EXTRACT(MONTH FROM date) AS month,
  EXTRACT(YEAR FROM date) AS year,
  SUM(spend) AS st_monthly_spend,
  SUM(Clicks) AS st_monthly_clicks,
  SUM(_14_Day_Total_Sales) AS st_monthly_sales
FROM
  `prefab-clover-311410.advertising_data.search_terms_merged_data`
GROUP BY 1,2,3
),

kw_monthly_spends AS (
SELECT
  Targeting,
  EXTRACT(MONTH FROM date) AS month,
  EXTRACT(YEAR FROM date) AS year,
  SUM(spend) AS kw_monthly_spend,
  SUM(Clicks) AS kw_monthly_clicks,
  SUM(_14_Day_Total_Sales) AS kw_monthly_sales
FROM
  `prefab-clover-311410.advertising_data.search_terms_merged_data`
GROUP BY 1,2,3
),

date_instances AS (
SELECT
  Date,
  COUNT(*) AS instances
FROM
  `prefab-clover-311410.advertising_data.search_terms_merged_data`
GROUP BY 1
),

adjusted_sales_traffic AS (
SELECT
  sales_traffic.Date,
  Ordered_Product_Sales/instances AS ordered_product_sales_adjusted,
  sessions_total/instances AS sessions_adjusted,
  Units_Ordered/instances AS units_ordered_adjusted,
  Total_Order_Items/instances AS total_order_items_adjusted,
  Units_Refunded/instances AS units_refunded_adjusted,
  Feedback_Received/instances AS feedback_received_adjusted
FROM
  `prefab-clover-311410.advertising_data.amazon_sales_traffic` AS sales_traffic
LEFT JOIN
  date_instances
ON sales_traffic.date = date_instances.date
),

search_terms_merged_data AS (
SELECT
  *,
  EXTRACT(MONTH FROM date) AS month,
  EXTRACT(YEAR FROM date) AS year
FROM
  `prefab-clover-311410.advertising_data.search_terms_merged_data`
),

campaign_mapping AS (
SELECT DISTINCT
  campaign,
  campaign_new
FROM
  `prefab-clover-311410.sql_master_tables.campaign_mapping`
),

adgroup_mapping AS (
SELECT DISTINCT
  ad_group,
  ad_group_new,
  product_type
FROM
  `prefab-clover-311410.sql_master_tables.campaign_mapping`
),

search_terms_data AS (
SELECT
  search_terms_merged_data.* EXCEPT(campaign_name, ad_group_name),
  IFNULL(campaign_new,search_terms_merged_data.campaign_name) AS campaign_name,
  IFNULL(ad_group_new,search_terms_merged_data.ad_group_name) AS ad_group_name,
  IFNULL(adgroup_mapping.product_type,NULL) AS product_type,
  st_monthly_spend,
  st_monthly_clicks,
  st_monthly_sales,
  kw_monthly_spend,
  kw_monthly_clicks,
  kw_monthly_sales,
  amazon_sales_traffic.* EXCEPT(Date),
  adjusted_sales_traffic.* EXCEPT(Date)
FROM
  search_terms_merged_data
LEFT JOIN
  st_monthly_spends
ON search_terms_merged_data.Customer_Search_Term = st_monthly_spends.Customer_Search_Term AND search_terms_merged_data.year = st_monthly_spends.year AND search_terms_merged_data.month = st_monthly_spends.month
LEFT JOIN
  kw_monthly_spends
ON search_terms_merged_data.Targeting = kw_monthly_spends.Targeting AND search_terms_merged_data.year = kw_monthly_spends.year AND search_terms_merged_data.month = kw_monthly_spends.month
LEFT JOIN
  adjusted_sales_traffic
ON search_terms_merged_data.Date = adjusted_sales_traffic.Date
LEFT JOIN
  `prefab-clover-311410.advertising_data.amazon_sales_traffic` AS amazon_sales_traffic
ON search_terms_merged_data.date = amazon_sales_traffic.date
LEFT JOIN
  campaign_mapping
ON search_terms_merged_data.campaign_name = campaign_mapping.campaign
LEFT JOIN
  adgroup_mapping
ON search_terms_merged_data.ad_group_name = adgroup_mapping.ad_group
),

traffic_divisor AS (
SELECT
  EXTRACT(MONTH FROM date) AS month,
  EXTRACT(YEAR FROM date) AS year,
  product_type,
  COUNT(*) AS divisor
FROM
  search_terms_data
GROUP BY 1,2,3
),

traffic AS (
SELECT
  asin_s_t.year,
  asin_s_t.month,
  asin_s_t.product_type,
  ROUND(sessions/divisor,2) AS sessions,
  ROUND(page_views/divisor,2) AS page_views,
  ROUND(units_ordered/divisor,2) AS units_ordered,
  ROUND(ordered_product_sales/divisor,2) AS ordered_product_sales,
  ROUND(total_order_items/divisor,2) AS total_order_items
FROM
  (
  SELECT
    EXTRACT(YEAR FROM Date) AS year,
    EXTRACT(MONTH FROM Date) AS month,
    product_type,
    SUM(sessions_total) AS sessions,
    SUM(page_views_total) AS page_views,
    SUM(units_ordered) AS units_ordered,
    SUM(ordered_product_sales) AS ordered_product_sales,
    SUM(total_order_items) AS total_order_items
  FROM
    `prefab-clover-311410.sql_master_tables.asin_sales_traffic`
  GROUP BY 1,2,3
  ) AS asin_s_t
LEFT JOIN
  traffic_divisor
ON asin_s_t.year = traffic_divisor.year AND asin_s_t.month = traffic_divisor.month AND asin_s_t.product_type = traffic_divisor.product_type
)


SELECT
  search_terms_data.* EXCEPT(month,year),
  traffic.sessions AS monthly_sessions,
  traffic.page_views AS monthly_pageviews,
  traffic.units_ordered AS monthly_units_ordered,
  traffic.ordered_product_sales AS monthly_ordered_product_sales,
  traffic.total_order_items AS monthly_total_units_ordered
FROM
  search_terms_data
LEFT JOIN
  traffic
ON search_terms_data.year = traffic.year AND search_terms_data.month = traffic.month AND search_terms_data.product_type = traffic.product_type
