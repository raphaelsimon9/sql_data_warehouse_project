/*
	=================================================================
	DML Script For Inserting Cleaned Data Into Silver.erp_px_cat_g1v2
	=================================================================

	What This Script Does:
		It inserts cleaned and standardized category data from the bronze layer into silver.erp_px_cat_g1v2.
		
	Ensure you have proper backups before attempting to run this script.

	Running this script will Truncate first, then Insert the cleaned data into the table.
*/

USE DataWareHouse;

GO

-- Inserting the cleaned data into the 'ERP Category' table of the Silver Schema
TRUNCATE TABLE silver.erp_px_cat_g1v2;
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


SELECT * FROM silver.erp_px_cat_g1v2;
