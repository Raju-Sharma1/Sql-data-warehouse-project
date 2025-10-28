/*
================
Quality Checks
================

Purpose of the scripts:
-----------------------
      This script performs quality checks to validate the integrity, consistency and accuracy of the gold layer.
      
      These Checks ensures:
      --------------------
          1. Uniqueness of surrogate keys in the dimension tables.
          2. Referential integrity between fact and dimension tables.
          3. Validation of relationships in the data model for analytical purposes.

Usage Notes:
------------
  Run these checks after loading data to the Gold Layer.
  Investigate and resolve any discrepancies found during the checks.
=====================================================================
*/


/*
============================
gold.dim_customers - Checks
============================
*/
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;
/*
===========================
gold.dim_products - Checks
===========================
*/
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

/*
===========================
gold.fact_sales - Checks
===========================
*/

-- Check the data model connectivity between fact and dimensions

SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;

