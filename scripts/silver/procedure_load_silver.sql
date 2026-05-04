/*
	=========================================================================
	Create Stored Procedure: Load Silver Layer {From Bronze To Silver Schema}
	=========================================================================

	What This Script Does:
		This stored procedure loads cleaned bronze layer data into the silver schema.
		>>>	It Truncates the silver layer tables before loading the data.

	Usage:
		run script >>> EXEC silver.load_silver; <<< to execute the stored procedure load the data 
		to the silver layer tables
*/

USE DataWareHouse;

GO

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME

BEGIN TRY

SET @batch_start_time = GETDATE();
	PRINT '----------------------------------------------------------------------------------';
SET @start_time = GETDATE();
	PRINT '>>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '';
	PRINT '>>> Inserting Data Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info
	(
		cst_id
		,cst_key
		,cst_firstname
		,cst_lastname
		,cst_marital_status
		,cst_gndr
		,cst_create_date
	)
	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE	UPPER(TRIM(cst_marital_status))
			WHEN	'M'	THEN 'Married'
			WHEN	'S'	THEN 'Single'
			ELSE	'Not Specified'
		END	AS	cst_marital_status,	-- Normalizing the marital status values to readable formats

		CASE	UPPER(TRIM(cst_gndr))
			WHEN	'M'	THEN 'Male'
			WHEN	'F'	THEN 'Female'
			ELSE	'Not Specified'
		END	AS	cst_gndr,	-- Normalizing the gender values to readable formats

		CAST (cst_create_date AS DATE) AS cst_create_date
	FROM
		(
		SELECT
			*,
			ROW_NUMBER()	OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rnk
		FROM	bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t
	WHERE	rnk = 1;	-- Selecting the most recent record for each customer
	PRINT '>>> Inserted Cleaned Data Into: silver.crm_cust_info';
SET @end_time = GETDATE();
PRINT	'>>> Load Time Is: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
	PRINT '----------------------------------------------------------------------------------';
	PRINT '';


SET @start_time = GETDATE();
	PRINT '>>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '';
	PRINT '>>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO	silver.crm_prd_info
	(
		prd_id
		,cat_id
		,prd_key
		,prd_nm
		,prd_cost
		,prd_line
		,prd_start_dt
		,prd_end_dt
	)
	SELECT
			prd_id
			,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
			,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
			,prd_nm
			,ISNULL(prd_cost, 0) AS prd_cost -- Assign zero (0) to NULL values to aid with better calculations
			,CASE	UPPER(TRIM(prd_line))
				WHEN	'R' THEN	'Road'
				WHEN	'M' THEN	'Mountain'
				WHEN	'S' THEN	'Accessories'
				WHEN	'T' THEN	'Touring'
				ELSE	'Not Specified'
			END	AS	prd_line -- Give descriptive values to the product line
			,CAST (prd_start_dt AS DATE) AS prd_start_dt
			,CAST (LEAD(prd_start_dt)	OVER(PARTITION BY	prd_key	ORDER BY	prd_start_dt) - 1 AS DATE) AS prd_end_dt -- Calculate the end date as 1 day before the next start date
	FROM	bronze.crm_prd_info;
	PRINT '>>> Inserted Cleaned Data Into: silver.crm_prd_info';
SET @end_time = GETDATE();
PRINT	'>>> Load Time Is: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
	PRINT '----------------------------------------------------------------------------------';
	PRINT '';


SET @start_time = GETDATE();
	PRINT '>>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '';
	PRINT '>>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details
	(
		sls_ord_num
		,sls_prd_key
		,sls_cust_id
		,sls_order_dt
		,sls_ship_dt
		,sls_due_dt
		,sls_sales
		,sls_quantity
		,sls_price
	)
	SELECT
		sls_ord_num
		,sls_prd_key
		,sls_cust_id

		,CASE
			WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) <> 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt

		,CASE
			WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt

		,CASE
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) <> 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt

		,CASE
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price)
					THEN sls_quantity * sls_price
			ELSE	sls_sales
			END AS sls_sales

			,sls_quantity

			,CASE
				WHEN sls_price IS NULL OR sls_price <= 0
					THEN sls_sales / NULLIF(sls_quantity, 0) -- Replaces all zeros with NULLs to avoid division errors
			ELSE	sls_price
			END AS sls_price
	FROM	bronze.crm_sales_details;
	PRINT '>>> Inserted Cleaned Data Into: silver.crm_sales_details';
SET @end_time = GETDATE();
PRINT	'>>> Load Time Is: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
	PRINT '----------------------------------------------------------------------------------';
	PRINT '';


SET @start_time = GETDATE();
	PRINT '>>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '';
	PRINT '>>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12
	(
		cid
		,bdate
		,gen
	)
	SELECT
		CASE
			WHEN	cid LIKE 'NAS%'	THEN	SUBSTRING(cid, 4, LEN(cid))
			ELSE	cid
		END AS cid,

		CASE
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,

		CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
				ELSE 'Not Specified'
			END AS gen
	FROM	bronze.erp_cust_az12;
	PRINT '>>> Inserted Cleaned Data Into: silver.erp_cust_az12';
SET @end_time = GETDATE();
PRINT	'>>> Load Time Is: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
	PRINT '----------------------------------------------------------------------------------';
	PRINT '';

	
SET @start_time = GETDATE();
	PRINT '>>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '';
	PRINT '>>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101
	(
		cid
		,cntry
	)
	SELECT
		REPLACE(cid, '-', '') AS cid,
		CASE
				WHEN UPPER(TRIM(cntry)) IN ('DE', 'Germany') THEN 'Germany'
				WHEN UPPER(TRIM(cntry)) IN ('USA', 'US', 'United States') THEN 'United States'
				WHEN UPPER(TRIM(cntry)) IN ('UK', 'United Kingdom') THEN 'United Kindgom'
				WHEN UPPER(TRIM(cntry)) IN ('FR', 'France') THEN 'France'
				WHEN UPPER(TRIM(cntry)) IN ('AU', 'Australia') THEN 'Australia'
				WHEN UPPER(TRIM(cntry)) IN ('CA', 'Canada') THEN 'Canada'
				WHEN UPPER(TRIM(cntry)) IN (NULL, '') THEN NULL
				ELSE cntry
			END AS cntry
	FROM	bronze.erp_loc_a101;
	PRINT '>>> Inserted Cleaned Data Into: silver.erp_loc_a101';
SET @end_time = GETDATE();
PRINT	'>>> Load Time Is: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
	PRINT '----------------------------------------------------------------------------------';
	PRINT '';


SET @start_time = GETDATE();
	PRINT '>>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '';
	PRINT '>>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2
	(
		id
		,cat
		,subcat
		,maintenance
	)
	SELECT
			id
			,cat
			,subcat
			,maintenance
	FROM	bronze.erp_px_cat_g1v2;
	PRINT '>>> Inserted Cleaned Data Into: silver.erp_px_cat_g1v2';
SET @end_time = GETDATE();
PRINT	'>>> Load Time Is: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
	PRINT '----------------------------------------------------------------------------------';
	PRINT '';

SET @batch_end_time = GETDATE();
PRINT	'>>> Loading is Completed! The Total Load Time Is: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' second';
PRINT '';
PRINT '----------------------------------------------------------------------------------';
END TRY

BEGIN CATCH
	PRINT '=====================================================';
	PRINT 'An Error Occured While Loading The Silver Layer';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR)
	PRINT '=====================================================';
	PRINT '';
END CATCH

END

