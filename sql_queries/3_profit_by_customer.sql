SELECT customer_id, ROUND(SUM(Total_profit),2) AS total_profit FROM fz_catalog.gold.full_order_info
GROUP BY 1
ORDER BY 2 DESC