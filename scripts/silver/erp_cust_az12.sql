/*
	===============================================================
	DML Script For Inserting Cleaned Data Into Silver.erp_cust_az12
	===============================================================

	What This Script Does:
		It inserts cleaned and standardized customer info data from the bronze layer into silver.erp_cust_az12.
		
	Ensure you have proper backups before attempting to run this script.

	Running this script will Truncate first, then Insert the cleaned data into the table.
*/

-- Inserting the cleaned data into the 'ERP Customer Info' table of the Silver Schema
TRUNCATE TABLE silver.erp_cust_az12;
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


SELECT * FROM silver.erp_cust_az12;
