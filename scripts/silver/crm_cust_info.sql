/*
	===============================================================
	DML Script For Inserting Cleaned Data Into Silver.crm_cust_info
	===============================================================

	What This Script Does:
		It inserts cleaned and standardized customer info data from the bronze layer into silver.crm_cust_info.
		
	Ensure you have proper backups before attempting to run this script.

	Running this script will Truncate first, then Insert the cleaned data into the table.
*/

-- Inserting the cleaned data into the 'Customer Info' table of the Silver Schema
TRUNCATE TABLE silver.crm_cust_info;
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
