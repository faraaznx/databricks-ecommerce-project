SELECT customer_id, year, SUM(Total_Profit) AS total_profit FROM fz_catalog.gold.full_order_info
GROUP BY 1,2
ORDER BY 1, 2 DESC
