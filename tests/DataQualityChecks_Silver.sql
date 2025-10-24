/*
=====================
            START....
=====================
Following SQL Queries are for Data Quality Checks:

    Purpose:
    --------
        These Queries performs various quality checks for Data consistency, Quality, Accuracy, and Standardization throughout the Silver Layer
        -------------------
        Include Checks for:
        -------------------
        1. Null values.
        2. Duplicate Values.
        3. Unwanted Spaces in string values.
        4. Data Standardization & Consistency.
        5. Invalid Date Ranges and Orders.
        6. Data Consistency between related Fields.
-----------
USAGE NOTES:
-----------
    * These SQL queries should be executed after data has been loaded to the Silver Layer for checks.
    * If found any discrepancies after the checks, Investigate and Resolve.

====================================
Checking:  silver.crm_cust_info
====================================
*/
-- Checking for Nulls and Blanks
Select
    cst_id
      From silver.crm_cust_info
        Where cst_id is null or cst_id = ''

-- Checking for Duplicates
Select
    cst_id,
    Count(*)
      From silver.crm_cust_info
        Group By cst_id
        Having Count(*) > 1;

-- Checking for unwanted spaces
Select
  cst_firstname,
  From silver.crm_cust_info
    Where cst_firstname != trim(cst_firstname);

-- Data standardization and consistency
Select Distinct
  cst_marital_status
    From silver.crm_cust_info

    
/*
====================================
Checking:  silver.crm_prd_info
====================================
*/

-- Checking for Nulls and Blanks
Select
  prd_id
    From silver.crm_prd_info
      Where prd_id is null or prd_id = ''

-- Checking for Duplicates
Select
  prd_id,
  count(*)
  From silver.crm_prd_info
    Group By prd_id
    Having prd_id > 1;

-- Checking for unwanted Spaces
Select
  prd_key
  From silver.crm_prd_info
    Where prd_key != trim(prd_key);

-- Data Standardization and Consistency Check
Select Distinct
  prd_line
  From silver.crm_prd_info;

-- Checking for Negative values
Select
  prd_cost
  From silver.crm_prd_info
    Where prd_cost < 0;

-- Checking for Invalid Date order for prd_start_dt and prd_end_dt (Start Date should be less than End Date)
Select
  *
  From silver.crm_prd_info
    Where prd_start_dt > prd_end_dt;


/*
====================================
Checking:  silver.crm_sales_details
====================================
*/

-- Checking for Negative values or 0's
Select
    sls_sales,
    sls_quantity,
    sls_price
    From silver.crm_sales_details
        Where sls_sales <= 0
        OR sls_quantity <= 0
        OR sls_price <= 0;

-- Data Standardization and Consistency Check
-- sls_sales = sls_quantity * sls_price
Select
    sls_sales
    From silver.crm_sales_details
      Where sls_sales != sls_quantity * sls_price;
-- sls_price = sls_sales / sls_quantity
Select
    sls_price
    From silver.crm_sales_details
      Where sls_price != sls_sales / sls_quantity;

-- Checking for Nulls
Select
    *
    From silver.crm_sales_details
        Where sls_sales is null
            OR sls_price is null
            OR sls_quantity is null
  
-- Checking for Invalid Dates
Select
  Nullif(sls_due_dt, 0) sls_due_dt
  From silver.crm_sales_details
    Where sls_due_dt <= 0
      OR Len(sls_due_dt) != 8
      OR sls_due_dt > 20500101
      OR sls_due_dt < 19000101;

-- Checking invalid date orders (Checking for : sls_order_dt > sls_ship_dt/sls_due_dt)
/* Correct Order: (sls_order_dt < sls_ship_dt)
                (sls_ship_dt < sls_due_dt)
*/
Select
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
    From silver.crm_sales_details
        Where sls_order_dt > sls_ship_dt
            OR sls_ship_dt > sls_due_dt;


/*
====================================
Checking:  silver.erp_cust_az12
====================================
*/

-- Checking for Invalid Birthdates
Select
    BDATE
    From silver.erp_cust_az12
        Where BDATE > GETDATE();

-- Data Standardization and Consistency check
Select Distinct
    GEN
    From silver.erp_cust_az12;


/*
====================================
Checking:  silver.erp_loc_a101
====================================
*/

-- Data Standardization and Consistency check
Select Distinct
    CNTRY
    From silver.erp_loc_a101;


/*
====================================
Checking:  silver.erp_px_cat_g1v2
====================================
*/

-- Checking for Unwanted spaces
Select
    *
    From silver.erp_px_cat_g1v2
        Where cat != trim(cat)
            OR subcat != trim(subcat)
            OR maintenance != trim(maintenance);

-- Data Standardization and Consistency check
Select Distinct
    Maintenance
    From silver.erp_px_cat_g1v2;

/*
===================
            END....
===================
*/
