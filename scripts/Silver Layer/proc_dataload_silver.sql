/*
=====================================================================================
Stored Procedure: Load Silver Layer [From Source Bronze Layer -> to -> Silver Layer]
=====================================================================================

Purpose of the Script:
xxxxxxxxxxxxxxxxxxxxx
  This Stored Procedure performs the ETL Process Where data is Extracted from bronze layer, Tranformed and Loads it on to 'silver' schema.
  Following these actions along the way:
    - Truncates the tables on silver layer before loading.
    - INSERT INTO command is in use to load data from bronze tables to the Tables on the silver layer after Transforming and Cleaning the data.
-----------------------------------------------------------------------------------------------------------------------------------------------

Parameters:
xxxxxxxxxx
  NONE.
  No Parameters are accepted in this Stored Procedure neither it returns any values.
------------------------------------------------------------------------------------

Example Execution of Stored Procedure:
xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  EXEC silver.load_silver;
=====================================

*/

/* Alternate code to check if table silver.crm_cust_info exists then Truncate data in the Table
===============================================================================================
IF EXISTS (Select 1 from sys.tables Where name = 'crm_cust_info' and schema_id = schema_id('silver'))
Truncate Table silver.crm_cust_info;
===============================================================================================
*/

/*
=======================
LOADING SILVER LAYER
USING: STORED PROCEDURE
=======================

============================================================================
Insertig Data to -  Silver.crm.cust_info << ( FROM ) << bronze.crm.cust_info
============================================================================
*/
Create or alter PROCEDURE silver.load_silver AS
	BEGIN
		BEGIN TRY
			Declare @Start_time DATETIME, @end_time DATETIME, @start_loading DATETIME, @end_loading DATETIME
			Set @start_loading = GETDATE();
			PRINT '========================================='
			PRINT 'Loading Silver Layer'
			PRINT '========================================='

			PRINT '-----------------------------------------'
			PRINT 'Loading CRM Tables'
			PRINT '-----------------------------------------'

			SET @Start_time = GETDATE();
			PRINT 'Truncating Table: silver.crm_cust_info'

			IF Object_id('silver.crm_cust_info') is NOT NULL
				TRUNCATE Table silver.crm_cust_info;
			PRINT '============================================'
			Print '>> Inserting Data into: silver.crm_cust_info'
			Insert into silver.crm_cust_info (
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			)
			SELECT
				cst_id,
				cst_key,
				TRIM(cst_firstname) AS cst_firstname,
				TRIM(cst_lastname) AS cst_lastname,
				CASE
					When Upper(trim(cst_marital_status)) = 'M' Then 'Married'
					When Upper(trim(cst_marital_status)) = 'S' Then 'Single'
					Else 'Unknown'
				END as cst_marital_status,
				CASE
					When Upper(trim(cst_gndr)) = 'M' Then 'Male'
					When Upper(trim(cst_gndr)) = 'F' Then 'Female'
					Else 'Unknown'
				END as cst_gndr,
				cst_create_date
			From (
			Select
				*,
				Row_number() Over(Partition By cst_id Order By cst_create_date desc) Flag_last,
				count(cst_id) Over(Partition By cst_id) id_count
				From bronze.crm_cust_info
			) t Where cst_id is NOT NULL AND id_count = 1

				SET @end_time = GETDATE();
				Print ''
				Print '>> Data Insertion Completed to silver.crm_cust_info'
				print '==================================================='
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> ----------------';

			/*
			==========================================================================
			Insertig Data to -  Silver.crm.prd_info << ( FROM ) << bronze.crm_prd_info
			==========================================================================
			*/

			SET @Start_time = GETDATE();
			PRINT 'Truncating Table: silver.crm_prd_info'
			IF Object_id('silver.crm_prd_info') is NOT NULL
				Truncate table silver.crm_prd_info;
			PRINT '==========================================='
			Print '>> Inserting Data into: silver.crm_prd_info'
			Insert into silver.crm_prd_info
			(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)
			Select
				prd_id,
				Replace(SUBSTRING(prd_key,1, 5),'-', '_') as cat_id,
				SUBSTRING(prd_key,7, len(prd_key)) as Prd_key,
				prd_nm,
				Coalesce(prd_cost, 0) as prd_cost,
				CASE Upper(trim(prd_line))
					WHEN 'M' Then 'Mountain'
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'n/a'
				END as prd_line,
				prd_start_dt,
				DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
				From bronze.crm_prd_info;

				SET @end_time = GETDATE();
				Print ''
				Print '>> Data Insertion Completed to silver.crm_prd_info'
				print '=================================================='
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -----------------';
			/*
			===============================================================================
			-- Inserting Data to silver.crm_sales_details from bronze.crm_sales_details
			===============================================================================
			*/

			-- Checking the data in bronze.crm_sales_details
			-- Data Cleansing of table bronze.crm_sales_details
			SET @Start_time = GETDATE();
			PRINT 'Truncating Table: silver.crm_sales_details'
			If Object_id('silver.crm_sales_details') is NOT NULL
				Truncate table silver.crm_sales_details;
			PRINT '================================================'
			Print '>> Inserting Data into: silver.crm_sales_details'
			INSERT INTO silver.crm_sales_details (
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
			)
			Select
				trim(sls_ord_num) as sls_ord_num, -- all good just TRIM()
				trim(sls_prd_key) as sls_prd_key, -- all good just TRIM()
				sls_cust_id, -- all good
				Case 
					When sls_order_dt = 0 or LEN(sls_order_dt) != 8 Then NULL
					Else Cast(cast( sls_order_dt as Nvarchar) as Date)
				End as sls_order_dt, -- INT datatype found where it should be DATE, Found numbers that cannot be converted to date(0, 5489, 32154)
				Convert(Date, Convert(nvarchar (8), sls_ship_dt)) as sls_ship_dt, -- INT datatype found where it should be DATE
				Convert(Date, Convert(nvarchar (8), sls_due_dt)) as sls_due_dt, -- INT datatype found where it should be DATE
				ABS(sls_quantity * Case 
										When ABS(sls_price) is Null Then sls_sales / sls_quantity
										else ABS(sls_price)
									End
				) as sls_sales, -- negative and null values
				sls_quantity, -- all good
				Case 
					When ABS(sls_price) is Null Then sls_sales / nullif(sls_quantity, 0)
					else ABS(sls_price)
				End as sls_price -- Negative and null values
				From bronze.crm_sales_details;

				SET @end_time = GETDATE();
				Print ''
				Print '>> Data Insertion Completed to silver.crm_sales_details'
				print '======================================================='
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -----------------';

			/*
			========================================================================
			Clean and Load (bronze.erp_cust_az12) Table -to- (silver.erp_cust_az12)
			========================================================================
			*/
			Print '-------------------'
			Print 'Loading ERP Tables'
			Print '-------------------'
			-- Truncating the Table silver.erp_cust_az12 if Table is NOT NULL and Then inserting the Data to the Table

			SET @Start_time = GETDATE();
			PRINT 'Truncating Table: silver.erp_cust_az12'
			IF OBJECT_ID('silver.erp_cust_az12') is NOT NULL
				TRUNCATE Table silver.erp_cust_az12;
			PRINT '============================================'
			Print '>> Inserting Data into: silver.erp_cust_az12'
			INSERT INTO silver.erp_cust_az12
			(
				CID,
				BDATE,
				GEN
			)
			SELECT
				Case 
					When cid like 'NAS%' Then Substring(CID,4, len(CID)) -- Extracted the customer Key removing NAS prefix
					Else CID
				End as CID,
				Case 
					When BDATE > GETDATE() Then Null -- Flagged Null to all the DOB that are future dated than today
					Else BDATE
				End BDATE,
				Case 
					When Upper(Trim(GEN)) in ('F', 'FEMALE') Then 'Female' -- Flagged M, Male to 'Male' - F, Female to 'Female' and Else as N/a
					When Upper(Trim(GEN)) in ('M', 'MALE') Then 'Male'
					Else 'n/a'
					ENd GEN
				From bronze.erp_cust_az12;

				SET @end_time = GETDATE();
				Print ''
				Print '>> Data Insertion Completed to silver.erp_cust_az12'
				print '==================================================='
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> ----------------';

			/*
			========================================================================
			Clean and Load (bronze.erp_loc_a101) Table -to- (silver.erp_loc_a101)
			========================================================================
			*/

			-- Tuncating the table content if already exists in the table and then inserting the data to the table
			
			SET @Start_time = GETDATE();
			PRINT 'Truncating Table: silver.erp_loc_a101'
			IF Object_id('silver.erp_loc_a101') is not null
				TRUNCATE Table silver.erp_loc_a101;
			PRINT '==========================================='
			Print '>> Inserting Data into: silver.erp_loc_a101'
			INSERT INTO silver.erp_loc_a101 (
				CID,
				CNTRY
			)
			Select
				Replace(Trim(CID), '-','') as CID, -- Trimmed column to remove unwanted spaces and replaced '-' as ''
				Case
					When Trim(cntry) is Null or Trim(cntry) = '  ' Then 'n/a'
					When Trim(cntry) in ('US','USA', 'United States') THEN 'United States' -- Flagged (null and blank) values as 'n/a', (US, USA, United States) as 'USA'
					When Trim(CNTRY) in ('DE', 'Germany') Then 'Germany'-- (DE, Germany) as 'Germany' else as is
					Else Trim(cntry)
				End CNTRY
				From bronze.erp_loc_a101;

				SET @end_time = GETDATE();
				Print ''
				Print '>> Data Insertion Completed to silver.erp_loc_a101'
				print '=================================================='
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> ----------------';

			/*
			===========================================================================
			Clean and Load (bronze.erp_px_cat_g1v2) Table -to- (silver.erp_px_cat_g1v2)
			===========================================================================
			*/

			SET @Start_time = GETDATE();
			PRINT 'Truncating Table: silver.erp_px_cat_g1v2'
			IF Object_id('silver.erp_px_cat_g1v2') is NOT NULL
				TRUNCATE Table silver.erp_px_cat_g1v2;
			print '=============================================='
			Print '>> Inserting Data into: silver.erp_px_cat_g1v2'
			INSERT INTO silver.erp_px_cat_g1v2(
				id,
				cat,
				subcat,
				maintenance
			)
			Select 
				id, -- No Nulls no empty rows , No Unwanted Spaces
				cat, -- No Nulls No empty rows. No Unwanted Spaces 
				subcat, -- No Nulls No empty  , No Unwanted Spaces
				maintenance -- No Nulls No empty rows, No Unwanted Spaces
				From bronze.erp_px_cat_g1v2;

				SET @end_time = GETDATE();
				Print ''
				Print '>> Data Insertion Completed to silver.erp_px_cat_g1v2'
				print '====================================================='
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> ----------------';

				SET @end_loading = GETDATE();
				Print '================================='
				Print 'Loading Silver Layer is completed'
				Print '  - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @start_loading, @end_loading) AS NVARCHAR) + ' seconds';
				print '================================='
		END TRY
		BEGIN CATCH
			PRINT '========================================='
			PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
			PRINT 'ERROR MESSAGE:' + ERROR_MESSAGE()
			PRINT 'ERROR NUMBER:' + CAST(ERROR_NUMBER() as NVARCHAR)
			PRINT 'ERROR STATE:' + CAST(ERROR_STATE() as NVARCHAR)
			PRINT '========================================='
		END CATCH
	END
					
-- ========================
-- End of Stored Procedure
-- ========================

-- Executing Stored Procedure silver.load_silver
EXec silver.load_silver
