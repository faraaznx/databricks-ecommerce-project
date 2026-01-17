SELECT customer_id, year, ROUND(SUM(Total_profit),2) AS total_profit FROM fz_catalog.gold.full_order_info
GROUP BY 1,2
ORDER BY 1, 2 DESC