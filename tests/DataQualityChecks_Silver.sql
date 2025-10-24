/*
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
-- ----------------------------------------------
-- Data Standardization and Consistency Check
-- ----------------------------------------------
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
  
-- Checking for Invalid Dates
Select
  Nullif(sls_due_dt, 0) sls_due_dt
  From silver.crm_sales_details
    Where sls_due_dt <= 0
      OR Len(sls_due_dt) != 8
      OR sls_due_dt > 20500101
      OR sls_due_dt < 19000101;
  

