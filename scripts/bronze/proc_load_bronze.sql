/*
====================================================================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
====================================================================================================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncate the bronze tables before loading data.
   Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
   None.
   This stored procedure does not accept any parameters or return any values.

Usage Example:
 EXEC bronze.load_bronze;
=====================================================================================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
   DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME;
   BEGIN TRY
   SET @batch_start_time = GETDATE();
	PRINT '===========================================';
	PRINT 'Loading Bronze Layer';
	PRINT '===========================================';


	PRINT'--------------------------------------------';
	PRINT'Loading CRM Tables';
	PRINT'--------------------------------------------';


	SET @start_time = GETDATE();
	PRINT'>> Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;

	PRINT'>> Inserting Data Into: bronze.crm_cust_info';
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\Hp\AppData\Local\Temp\a5ae7cf2-d0df-456b-ad54-7dc0b2791726_sql-data-warehouse-project(4).zip.726\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		 FIRSTROW = 2,
		 FIELDTERMINATOR =',',
		 TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>--------------';


	SET @start_time = GETDATE();
    PRINT'>> Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;

	PRINT'>> Inserting Data Into: bronze.crm_prd_info';
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\Hp\AppData\Local\Temp\89c97f98-39ba-46a7-8ec0-5312f3f106fc_sql-data-warehouse-project(4).zip.6fc\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		 FIRSTROW = 2,
		 FIELDTERMINATOR =',',
		 TABLOCK
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------';

	SET @start_time = GETDATE();
    PRINT'>> Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	PRINT'>> Inserting Data Into: bronze.crm_sales_details';
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\Hp\AppData\Local\Temp\6a98e508-34c9-47ed-b091-bc565392e3e9_sql-data-warehouse-project(4).zip.3e9\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		 FIRSTROW =2,
		 FIELDTERMINATOR =',',
		 TABLOCK
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------';


    PRINT'--------------------------------------------';
	PRINT'Loading ERP Tables';
	PRINT'--------------------------------------------';

	SET @start_time = GETDATE();
	PRINT'>> Truncating Table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	PRINT'>> Inserting Data Into: bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\Hp\AppData\Local\Temp\3270af24-5965-4520-a053-61b599c63692_sql-data-warehouse-project(4).zip.692\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
	);
	SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
			PRINT '>>--------------';

    SET @start_time = GETDATE();
    PRINT'>> Truncating Table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	PRINT'>> Inserting Data Into: bronze.erp_cust_az12';
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\Hp\AppData\Local\Temp\d3c6fb1d-974e-44f7-956a-5d49bf06b8e2_sql-data-warehouse-project(4).zip.8e2\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		 FIRSTROW = 2,
		 FIELDTERMINATOR =',',
		 TABLOCK
	);
	SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
				PRINT '>>--------------';
    
	SET @start_time = GETDATE();
    PRINT'>> Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	PRINT'>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\Hp\AppData\Local\Temp\725babaf-5096-4c94-bdce-8206b7a64e5f_sql-data-warehouse-project(4).zip.e5f\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (
		 FIRSTROW = 2,
		 FIELDTERMINATOR =',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
					PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
					PRINT '>> --------------';

					SET @batch_end_time = GETDATE();
					PRINT'=================================='
					PRINT 'Loading Bronze Layer is Completed';
					PRINT'  - Total Load Duration:' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
					PRINT'=================================='
	END TRY
	BEGIN CATCH
	    PRINT'========================================================'
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'========================================================'
	END CATCH
END
