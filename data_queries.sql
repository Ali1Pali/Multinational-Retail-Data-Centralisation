-- How many stores does the business have and in which countries?

SELECT country_code, COUNT(country_code) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- Which locations currently have the most stores?

SELECT locality, COUNT(locality) AS total_no_stores
FROM dim_store_details
GROUP BY locality
HAVING COUNT(locality) >= 10
ORDER BY total_no_stores DESC;

-- Which months produce the highest cost of sales typically?

SELECT SUM(product_quantity * product_price) AS total_sales, month
FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY month
ORDER BY total_sales DESC
LIMIT 6;

-- How many sales are coming from online?

SELECT COUNT(*) AS number_of_sales, SUM(product_quantity) AS product_quantity_count,
CASE
	WHEN store_type = 'Web Portal' THEN 'Web'
	ELSE 'Offline'
END AS location
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY location;

-- What percentage of sales come through each type of store?

SELECT store_type, SUM(product_quantity * product_price) AS total_sales,
COUNT(store_type) * 100 / (SELECT COUNT(*) FROM orders_table) AS percentage_total
FROM orders_table
JOIN dim_products ON dim_products.product_code = orders_table.product_code
JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
GROUP BY store_type
ORDER BY total_sales DESC;

-- Which month in each year produced the highest cost of sales?

SELECT SUM(product_quantity * product_price) AS total_sales, year, month
FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY year, month
ORDER BY total_sales DESC
LIMIT 10;

-- What is our staff headcount?

SELECT SUM(staff_numbers) AS total_staff_numbers, country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

-- Which German store type is selling the most?

SELECT SUM(product_quantity * product_price) AS total_sales, store_type, country_code
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN dim_products ON dim_products.product_code = orders_table.product_code
WHERE country_code = 'DE'
GROUP BY store_type, country_code
ORDER BY total_sales;

-- How quickly is the company making sales?

WITH cte AS
(SELECT TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') AS full_date_time,
year FROM dim_date_times
ORDER BY full_date_time DESC),
cte2 AS
(SELECT year, full_date_time, 
LEAD(full_date_time, 1) OVER (ORDER BY full_date_time DESC) AS time_difference FROM cte)
SELECT year, AVG((full_date_time - time_difference)) AS actual_time_taken FROM cte2
GROUP BY year
ORDER BY actual_time_taken DESC
LIMIT 5