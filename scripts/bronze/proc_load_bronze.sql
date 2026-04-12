EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS --It has to be always the first line
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();


        PRINT '>> Truncating Table: bronze.crm_cust_info';
TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inse rting Data Into: bronze.crm_cust_info';

BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\sanja\OneDrive\Documents\Desktop\DA-PREP\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
			FIRSTROW = 2, --first row is header,row starts with 2nd
			FIELDTERMINATOR = ',',   --it is the separator in csv file(called file separator)
			TABLOCK   --locking entire table during loading to this table
			);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


--U have to test the quality of bronze table--
---SELECT * FROM bronze.crm_cust_info
---SELECT COUNT(*) FROM bronze.crm_cust_info

SET @start_time = GETDATE();	
PRINT '>> Truncating Table: bronze.crm_prd_info';
TRUNCATE TABLE bronze.crm_prd_info;
PRINT '>> Inserting Data Into: bronze.crm_prd_info';

BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\sanja\OneDrive\Documents\Desktop\DA-PREP\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

---SELECT * FROM bronze.crm_prd_info
---SELECT COUNT(*) FROM bronze.crm_prd_info
SET @start_time = GETDATE();
PRINT '>> Truncating Table: bronze.crm_sales_details';
TRUNCATE TABLE bronze.crm_sales_details;
PRINT '>> Inserting Data Into: bronze.crm_sales_details';

BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\sanja\OneDrive\Documents\Desktop\DA-PREP\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

---SELECT * FROM bronze.crm_sales_details
---SELECT COUNT(*) FROM bronze.crm_sales_details
        PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

SET @start_time = GETDATE();		
PRINT '>> Truncating Table: bronze.erp_loc_a101';
TRUNCATE TABLE bronze.erp_loc_a101;
PRINT '>> Inserting Data Into: bronze.erp_loc_a101';	

BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\sanja\OneDrive\Documents\Desktop\DA-PREP\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
SET @end_time = GETDATE();
	    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

---SELECT * FROM bronze.erp_loc_a101
---SELECT COUNT(*) FROM bronze.erp_loc_a101

SET @start_time = GETDATE();
PRINT '>> Truncating Table: bronze.erp_cust_az12';
TRUNCATE TABLE bronze.erp_cust_az12;
PRINT '>> Inserting Data Into: bronze.erp_cust_az12';

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\sanja\OneDrive\Documents\Desktop\DA-PREP\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

---SELECT * FROM bronze.erp_cust_az12
---SELECT COUNT(*) FROM bronze.erp_cust_az12
SET @start_time = GETDATE();
PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\sanja\OneDrive\Documents\Desktop\DA-PREP\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
SET @batch_end_time = GETDATE();
--- SELECT * FROM bronze.erp_px_cat_g1v2
---SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2

		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
 END TRY
 BEGIN CATCH --what to do when there is an error in ur code
        PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='

 END CATCH
END
