/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from bronze layer tables. 
    It performs the following actions:
    - Truncate and Insert data into all silver layer tables by performing data cleanig , transformation, aggeration etc
    - The data comes form the bronze layer not from the source system 

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
	DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME ; 
	SET @batch_start_time = GETDATE();
	BEGIN TRY 
		PRINT 'Inserting Clean data into Silver layer ';
		PRINT '----------------------------------------';
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table and Inserting data into : silver.crm_cust_info '
		INSERT INTO silver.crm_cust_info(
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
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'n/a'   -- Normalize short form of marital status 
		END cst_martial_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'n/a'   -- Normalize short form of gender 
		END cst_gndr,
		cst_create_date
		-- Remove dublicate values by create_date 
		FROM (
			SELECT 
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS FLAG_last 
				FROM bronze.crm_cust_info
				WHERE cst_id  IS NOT NULL 
		) t WHERE FLAG_last = 1   -- Select the most recent record from customer 
		SET @end_time  = GETDATE();
		PRINT 'Loading time ' + cast(DATEDIFF(SECOND, @start_time, @end_time ) AS NVARCHAR)  + ' seconds ';
		PRINT '-----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table and Inserting data into : silver.crm_prd_info '
		INSERT INTO silver.crm_prd_info(
			prd_id ,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost ,
			prd_line ,
			prd_start_dt ,
			prd_end_dt 
		)

		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')  AS  cat_id ,  -- Extract cat_id
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,				-- Extract prd_key 
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))				-- Normalize short form 
				WHEN 'M'  THEN 'Mountain'
				WHEN 'R'  THEN 'Road'
				WHEN 'S'  THEN 'Other Sales'
				WHEN 'T'  THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS DATE ) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ) - 1 AS DATE ) AS prd_end_dt    -- Calculate end date as one day before start date 
		FROM bronze.crm_prd_info
		SET @end_time  = GETDATE();
		PRINT 'Loading time ' + cast(DATEDIFF(SECOND, @start_time, @end_time ) AS NVARCHAR)  + ' seconds ';
		PRINT '-----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table and Inserting data into : silver.crm_sales_details '
		INSERT INTO silver.crm_sales_details(
			sls_ord_num ,
			sls_prd_key ,
			sls_cust_id ,
			sls_order_dt ,
			sls_ship_dt ,
			sls_due_dt ,
			sls_sales ,
			sls_quantity ,
			sls_price     
		)

		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL 
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS  DATE )
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL 
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS  DATE )
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL 
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS  DATE )
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,  -- Again Calculate sales if original value is missing
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price   -- Calculate value if original value is invalid

		FROM bronze.crm_sales_details
		SET @end_time  = GETDATE();
		PRINT 'Loading time ' + cast(DATEDIFF(SECOND, @start_time, @end_time ) AS NVARCHAR)  + ' seconds ';
		PRINT '-----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table and Inserting data into : silver.erp_loc_a101 '
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)

		SELECT 
		REPLACE(CID , '-', '') cid ,   -- Replace " - " 
		CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'    -- formate country info
			 WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(CNTRY)  = '' OR CNTRY IS NULL THEN 'n/a'
			 ELSE TRIM(CNTRY) 
		END AS cntry
		FROM bronze.erp_loc_a101
		SET @end_time  = GETDATE();
		PRINT 'Loading time ' + cast(DATEDIFF(SECOND, @start_time, @end_time ) AS NVARCHAR)  + ' seconds ';
		PRINT '-----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table and Inserting data into : silver.erp_cust_az12 '
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		SELECT 
		CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4 , LEN(CID))    -- Remove prefix from cid
			ELSE CID 
		END AS cid ,
		CASE WHEN BDATE > GETDATE() THEN NULL      --- Remove future birth date 
			 ELSE BDATE
		END AS bdate,
		CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'     -- Handle gender information
			 WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'N/A'
		END AS gen
		FROM bronze.erp_cust_az12
		SET @end_time  = GETDATE();
		PRINT 'Loading time ' + cast(DATEDIFF(SECOND, @start_time, @end_time ) AS NVARCHAR)  + ' seconds ';
		PRINT '-----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table and Inserting data into : silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)

		-- No data transformation needed 
		SELECT 
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		FROM bronze.erp_px_cat_g1v2
		SET @end_time  = GETDATE();
		PRINT 'Loading time ' + cast(DATEDIFF(SECOND, @start_time, @end_time ) AS NVARCHAR)  + ' seconds ';
		PRINT '-----------------';

		SET @batch_end_time = GETDATE();
		PRINT 'Whole Silver layer loding time : ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time ) AS NVARCHAR)  + ' seconds ';
	END TRY 
	BEGIN CATCH
		PRINT '-------------------------------------------------------'
		PRINT 'Error occured during Inserting data into the Silver layer : '
		PRINT 'Error Message ' + ERROR_MESSAGE();
		PRINT 'Error Message ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '-------------------------------------------------------'
	END CATCH
END


