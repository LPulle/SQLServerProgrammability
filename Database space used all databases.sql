/*------------------------------------------------------------------------------------------------------ 
Description   : Identify all databases on SQL Server
				List all tables, compression type and space used for each
				Uses undocumented sp_MSforeachdb function
				Could take a long time for Servers with a lot of databases and tables
                 
Date			Version		Author		Comment
---------------------------------------------------------------------------------------------------------
06-Jul-2017	    1.0			LP			First version
07-Dec-2018		1.1			LP			Tidy up for sharing
--------------------------------------------------------------------------------------------------------- */

-- Suppress Counts
SET NOCOUNT ON

-- Declare Variables
DECLARE 
	@RETURN_VALUE INT
	,@sqlcommand1 VARCHAR(MAX)
	,@Rowcount INT
	,@Deletes INT = 0
	,@CurrentDate VARCHAR(20)
	,@Databases VARCHAR(MAX);
	
-- Drop Temp Tables
IF OBJECT_ID('tempdb..#Databases', 'U') IS NOT NULL DROP TABLE #Databases;
IF OBJECT_ID('tempdb..#Tables', 'U') IS NOT NULL DROP TABLE #Tables;

--Creating Temp Tables
-- Table to hold the datbases
CREATE TABLE #Databases (DatabaseName VARCHAR(MAX));

-- Get a list of databases
-- We can use this to control where we are going to search for the tables
-- This makes it easier than having to amend the @sqlcommand1 below
SET @sqlcommand1 = 'USE [?]; INSERT INTO #Databases SELECT db_name()';
EXEC @RETURN_VALUE = sp_MSforeachdb @sqlcommand1;

DELETE FROM #Databases
WHERE DatabaseName IN ('master','msdb','tempdb','model');
-- alternative criteria to run for just one database
-- DatabaseName <> 'DatabaseName' 

-- Stuff list of Databases into variable
SET @Databases = (
		SELECT TOP 1
		STUFF((SELECT ', '+DatabaseName
		FROM #Databases
		ORDER BY DatabaseName DESC
		FOR XML PATH('')),1,1,'')
		FROM #Databases
			);

RAISERROR('Databases to be investigated ... %s', 0, 1, @Databases) WITH NOWAIT;

-- Create #temp
CREATE TABLE #Tables (
	DatabaseName VARCHAR(MAX)
	,TableName VARCHAR(MAX)
	,PartitionNumber BIGINT
	,DataCompressionDesc NVarchar(120)
	,TotalSpaceMB INT
		);

-- ========================================================
-- Identify tables from the sys tables and the allocation
-- and the space used 
-- Search every database using msforeachdb
-- Databases switched via Use [?]
-- ========================================================

RAISERROR('Identifying all tables and space used ...', 0, 1) WITH NOWAIT;
SET @sqlcommand1 = 
'use [?]; INSERT INTO #Tables 
SELECT Db_Name() AS DatabaseName, t.name AS TableName, p.partition_number, 
p.data_compression_desc, SUM(a.total_pages) * 8 / 1024 AS TotalSpaceMB
FROM sys.partitions AS p
INNER JOIN sys.tables AS t ON t.object_id = p.object_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
AND ''?'' IN (SELECT DatabaseName FROM #Databases)
GROUP BY t.name, p.partition_number, p.data_compression_desc';

-- Exec the above SQL Command inserting into #temp
EXEC @RETURN_VALUE = sp_MSforeachdb @sqlcommand1;

SET @Rowcount = (SELECT COUNT(*) FROM #Tables);
RAISERROR('Rows inserted... %i',0,1, @Rowcount)  WITH NOWAIT;

SELECT * FROM #Tables;