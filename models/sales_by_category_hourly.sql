CREATE OR REPLACE TABLE `meli-data-platform.meli_analytics.sales_by_category_hourly` AS

SELECT
    category,
    SUM(total_value) AS total_sales
FROM `meli-data-platform.meli_raw.orders_stream`
WHERE DATE(event_time) = CURRENT_DATE()
GROUP BY category