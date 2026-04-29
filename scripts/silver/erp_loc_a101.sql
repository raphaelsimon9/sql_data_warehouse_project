/*
	===============================================================
	DML Script For Inserting Cleaned Data Into Silver.erp_loc_a101
	===============================================================

	What This Script Does:
		It inserts cleaned and standardized customer info data from the bronze layer into silver.erp_loc_a101.
		
	Ensure you have proper backups before attempting to run this script.

	Running this script will Truncate first, then Insert the cleaned data into the table.
*/

-- Inserting the cleaned data into the 'ERP Customer Location' table of the Silver Schema
TRUNCATE TABLE silver.erp_loc_a101;
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


SELECT * FROM silver.erp_loc_a101;
