/*----------------------------------------------------------------------------------------------------- 
Description   	Identify all objects on SQL Server by keyword search
		List all tables, compression type and space used for each
		Uses undocumented sp_MSforeachdb function
                 
Date		Version		Author		Comment
---------------------------------------------------------------------------------------------------------
30-Jan-2019	1.0		LP		First version based on Database space used script
--------------------------------------------------------------------------------------------------------- */

CREATE PROC dbo.ScanAllDbsForObject (@Keyword VARCHAR(MAX)) AS

BEGIN
	-- Suppress Counts
	SET NOCOUNT ON

	-- Declare Variables
	DECLARE 
		@RETURN_VALUE INT
		,@sqlcommand1 VARCHAR(MAX)
		,@ObjectLike VARCHAR(MAX) = @Keyword
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
		,SchemaName VARCHAR(MAX)
		,ObjectName VARCHAR(MAX)
		,ObjectID INT
		,type_desc NVarchar(120)
		,create_date DATE
		,modify_date DATE
		,data_compression_desc nVarchar(120)
		,TotalSpaceMB INT
			);

	-- ========================================================
	-- Identify objects from the sys objects, the allocation
	-- and space used and some object info
	-- Search every database using msforeachdb
	-- Databases switched via Use [?]
	-- ========================================================

	RAISERROR('Identifying all tables and space used ...', 0, 1) WITH NOWAIT;
	SET @sqlcommand1 = 
	'USE [?]; INSERT INTO #Tables 
	SELECT Db_Name() AS DatabaseName,
	s.name AS SchemaName,o.name AS ObjectName,o.object_id AS ObjectID,o.type_desc,
	o.create_date,o.modify_date,p.data_compression_desc,
	SUM(a.total_pages) * 8 / 1024 AS TotalSpaceMB
	FROM 
	sys.all_objects o
	INNER JOIN sys.schemas s
	ON o.schema_id = s.schema_id
	INNER JOIN sys.partitions AS p
	ON o.object_id = p.object_id
	INNER JOIN sys.allocation_units a 
	ON p.partition_id = a.container_id
	WHERE o.name LIKE ''%'
	+@ObjectLike+
	'%'' AND is_ms_shipped = 0
	AND ''?'' IN (SELECT DatabaseName FROM #Databases)
	GROUP BY s.name,o.name,o.object_id,o.type_desc,o.create_date,
	o.modify_date,p.data_compression_desc';

	-- Exec the above SQL Command inserting into #temp
	EXEC @RETURN_VALUE = sp_MSforeachdb @sqlcommand1;

	SET @Rowcount = (SELECT COUNT(*) FROM #Tables);
	RAISERROR('Rows inserted... %i',0,1, @Rowcount)  WITH NOWAIT;

	SELECT * FROM #Tables;

END