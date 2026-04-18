/*
	=====================================================
	Create Databases and Schemas
	=====================================================

	What This Script Does:
		This script creates a database named DataWarehouse after checking if it already exists.
		If it exists, it is dropped and recreated. Three schemas named: 'bronze', 'silver', 'gold'
		were created within the database.

	CRITICAL WARNING (Exercise Extreme Caution):
		Running this script will drop the entire 'DataWarehouse' database if it exists and all data
		within it will be permanently deleted.

		Ensure you have proper backups before attempting to run this script.
*/

-- CREATE Database 'DataWarehouse

USE master;

GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	DROP DATABASE DataWareHouse;
END;

GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWareHouse;

GO

-- Switch to the 'DataWarehouse' database
USE DataWareHouse;

GO
-- Create Schemas
CREATE SCHEMA bronze;

GO

CREATE SCHEMA silver;

GO

CREATE SCHEMA gold;

