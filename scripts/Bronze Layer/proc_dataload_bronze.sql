/*
============================================================================
Stored Procedure: Load Bronze Layer [From Source -> to -> Bronze Layer]
============================================================================

Purpose of the Script:
xxxxxxxxxxxxxxxxxxxxx
  This Stored Procedure extracts data from an external .CSV file and loads it on to 'bronze' schema.
  Following these actions along the way:
    - Truncates the tables on bronze layer before loading.
    - BULK INSERT command is in use to load data from the .CSV file to the Tables on the bronze layer.
-------------------------------------------------------------------------------------------------------

Parameters:
xxxxxxxxxx
  NONE.
  No Parameters are accepted in this Stored Procedure neither it returns any values.
------------------------------------------------------

Example Execution of Stored Procedure:
xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  EXEC bronze.load_bronze;
=====================================

*/

-- Stored Procedure to insert data to the tables created on the 'bronze' schema
-- Using PRINT Messages for load clarity and calculating the load duration for performance check.

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @Start_time DATETIME, @End_time DATETIME, @start_loading DATETIME, @end_loading DATETIME;
		SET @start_loading = GETDATE();
	BEGIN TRY
		PRINT '======================'
		PRINT 'Loading Bronze Layer'
		PRINT '======================'
		PRINT '                       '
		PRINT '======================'
		PRINT 'Loading CRM Tables'
		PRINT '======================'

		SET @Start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>>> Inserting Data into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Desktop\SQL Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) as NVARCHAR) + ' seconds'
		PRINT '-----------------------------------------------------------------'

		-- Ingesting data into crm_prd_info table after truncating the table if it contains any data
		SET @start_time = getdate();
		PRINT '>>> Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>>> Inserting Data into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Desktop\SQL Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			KEEPNULLS,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) as NVARCHAR) + ' seconds'
		PRINT '-----------------------------------------------------------------'
		

		-- Ingesting data into crm_sales_details table after truncating the table if it contains any data
		SET @start_time = getdate();
		PRINT '>>> Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>> Inserting Data into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Desktop\SQL Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) as NVARCHAR) + ' seconds'
		PRINT '-----------------------------------------------------------------'

		-- Ingesting data into erp_cust_az12 table after truncating the table if it contains any data
		PRINT '======================'
		PRINT 'Loading ERP Tables'
		PRINT '======================'

		SET @start_time = getdate();
		PRINT '>>> Truncating Table: bronze.erp_cust_az12'
		TRUNCATE Table bronze.erp_cust_az12;

		PRINT '>>> Inserting Data into: bronze.erp_cust_az12'
		Bulk Insert bronze.erp_cust_az12
		From 'C:\Desktop\SQL Project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		With (
			Firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @End_time = getdate();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) as NVARCHAR) + ' seconds'
		PRINT '-----------------------------------------------------------------'

		-- Ingesting data into erp_cust_loc_a101 table after truncating the table if it contains any data
		SET @start_time = getdate();
		PRINT '>>> Truncating Table: bronze.erp_loc_a101'
		Truncate Table bronze.erp_loc_a101;

		PRINT '>>> Inserting Data into: bronze.erp_loc_a101'
		Bulk Insert bronze.erp_loc_a101
		From 'C:\Desktop\SQL Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			Firstrow = 2,
			Fieldterminator = ',',
			Tablock
		);
		SET @End_time = getdate();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) as NVARCHAR) + ' seconds'
		PRINT '-----------------------------------------------------------------'

		-- Ingesting data into erp_px_cat_g1v2 table after truncating the table if it contains any data
		SET @start_time = getdate();
		PRINT '>>> Truncating Table: bronze.erp_px_cat_g1v2'
		Truncate Table bronze.erp_px_cat_g1v2;

		PRINT '>>> Inserting Data into: bronze.erp_px_cat_g1v2'
		Bulk Insert bronze.erp_px_cat_g1v2
		From 'C:\Desktop\SQL Project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		With (
			Firstrow = 2,
			FieldTerminator = ',',
			Tablock
		);
		SET @End_time = getdate();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) as NVARCHAR) + ' seconds'
		PRINT '-----------------------------------------------------------------'
		PRINT '==================================================='
		PRINT 'BRONZE LAYER LOADING COMPLETED SUCCESSFULLY'
		PRINT '==================================================='
		SET @end_loading = GETDATE();
		PRINT '>> Layer Loading Duration: ' + CAST(DATEDIFF(Second, @start_loading, @end_loading) as NVARCHAR) + ' Seconds'
	END TRY
	BEGIN CATCH
		PRINT '================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE:' + ERROR_MESSAGE()
		PRINT 'ERROR NUMBER:' + CAST(ERROR_NUMBER() as NVARCHAR)
		PRINT 'ERROR STATE:' + CAST(ERROR_STATE() as NVARCHAR)
		PRINT '================================================='
	END CATCH
END

-- Executing Stored Procedure to load data into Bronze Tables
EXEC bronze.load_bronze;
GO
-- End of Code for Stored Procedure
