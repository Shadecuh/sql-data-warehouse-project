/*===============================================================================
Create Database and Schemas
=================================================================================
Script Purpose:
  This script creates a new database named 'DataWarehouse' after checking if it already exists.
  If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
within the database: 'bronze', 'silver', and 'gold'.

WARNING:
  Running this scipt will drop the entire 'DataWarehouse' database if it exists.
  All data in the database will be permanently deleted. Proceed with the caution 
  and ensure you have proper backups before running this scipt.
*/



USE master;
Go

  --DROP and recreate the 'DataWarehouse' database
  IF Exists(Select 1 from sys.databases where name= 'DataWArehouse')
  Begin
    Alter DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

--Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO
  
Use Database DataWarehouse;
Go
  
--Create Schemas
Create SCHEMA bronze;
GO
  
Create SCHEMA silver;
GO
  
Create SCHEMA gold;
GO
