/*
======================================================
  Creating: Bronze Layer (DDL Scripts)
======================================================
xxxxxx  xxxxxxxxxxxxx  xxxxxxxxxxxxx  xxxxxxxx  xxxxxxxxxxxxx
==============================================================================================
Purpose of the Script:
+++++++++++++++++++++
  It creates the Tables in the 'Bronze' Schema, upon dropping the tables if they alreay exist.
  To redefine the DDL structure of 'Bronze' Schema run this script.
===============================================================================================
*/
--Creating Table crm_cust_info in Bronze Schema, by deleting the table if it already exists.
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_cust_info;
GO
Create Table bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(10),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date Date
);

--Creating Table crm_prd_info in Bronze Schema, by deleting the table if it already exists.
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_prd_info;
GO
Create Table bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(100),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);
GO

--Creating Table crm_sales_details in Bronze Schema, by deleting the table if it already exists.
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
DROP TABLE bronze.crm_sales_details;
GO
Create Table bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
GO
--Creating Table erp_cust_az12 in Bronze Schema, by deleting the table if it already exists.
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
DROP TABLE bronze.erp_cust_az12;
GO
Create Table bronze.erp_cust_az12 (
	CID NVARCHAR(50),
	BDATE DATE,
	GEN NVARCHAR(10)
);
GO
--Creating Table erp_loc_a101 in Bronze Schema, by deleting the table if it already exists.
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE bronze.erp_loc_a101;
GO
Create Table bronze.erp_loc_a101 (
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50)
);
GO

--Creating Table erp_px_cat_g1v2 in Bronze Schema, by deleting the table if it already exists.
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
DROP TABLE bronze.erp_px_cat_g1v2;
GO
Create Table bronze.erp_px_cat_g1v2 (
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(10)
);
GO
