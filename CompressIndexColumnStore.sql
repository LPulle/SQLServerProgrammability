SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*------------------------------------------------------------------------------------------------------ 
Description  	
    Identifies tables over a certain size for specific databases with Page Compression
		Parameter @AllowCompress is set to 1 by default and will result
		in any tables found being compressed. If set to 0 will simply
		report back that tables have been found that can be compressed.
		Compression is done by creating a clustered columnstore index.
		Tables will be rebuilt using as a result of the index being added.
		Threshold is set to 100mb by default but can be changed in the
		parameter @thresholdsize.
		May need to be careful with the uniqueness of the index name

Usage: 	EXEC dbo.IndexCompressColumnstore
		EXEC dbo.CompressIndexColumnStore @AllowCompress = 1, @thresholdsize = 100
		EXEC dbo.CompressIndexColumnStore @AllowCompress = 0
		EXEC dbo.CompressIndexColumnStore @AllowCompress = 1, @thresholdsize = 300
		EXEC dbo.CompressIndexColumnStore @AllowCompress = 0, @thresholdsize = 300

                 
Date			Version		Author 		Comment
---------------------------------------------------------------------------------------------------------
28-Feb-2022	   	 1.0		LP		First version
--------------------------------------------------------------------------------------------------------- */

CREATE PROCEDURE [dbo].[CompressIndexColumnStore] (
@AllowCompress INT = 1, @thresholdsize INT = 100) 
AS
BEGIN
	
SET NOCOUNT ON

-- For Logging
DECLARE @LogId INT;
DECLARE @CreateDate DATETIME = GETDATE();
DECLARE @EndDate DATETIME;
DECLARE @ProcName VARCHAR(50) = (SELECT OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID));

INSERT INTO ETL.dbo.ETLLogsStats (StartDateTime, PackageName)
SELECT @CreateDate, @ProcName;
SET @LogId=SCOPE_IDENTITY();

-- Threshold for table size >= this size will be compressed
	DECLARE 
		@threshold INT = @thresholdsize,
		@Compress INT = @AllowCompress;

-- Variables for loop
	DECLARE @Databases TABLE (DatabaseID INT IDENTITY, DatabaseName VARCHAR(100));
	DECLARE 
		@DbName VARCHAR(100),
		@sqlcommand1 VARCHAR(MAX),
		@i INT,
		@j INT,
		@k INT,
      		@sqlcommand VARCHAR(MAX),
      		@Table VARCHAR(MAX);

-- Create a table for storing table information
IF OBJECT_ID('tempdb..#TablesCompress') IS NOT NULL DROP TABLE #TablesCompress
CREATE TABLE #TablesCompress (
	TableID INT IDENTITY
	,DatabaseName VARCHAR(50)
	,TableName VARCHAR(100)
	,DatabaseSchema VARCHAR(20)
	,PartitionNumber INT
	,DataCompressionDesc VARCHAR(10)
	,TotalSpaceMB INT
	,SQLStatement VARCHAR(MAX)
	,TableID2 INT
	,TableName2 VARCHAR(100)
	PRIMARY KEY (TableID)
		);

-- Populate loop variables 
-- Put names of the databases you're interested in in the @Databases table variable
INSERT @Databases (DatabaseName) 
VALUES ('Database1'),('Database2'),('Database3'),('Database4');
SET @i = 1;
SET @j = (SELECT MAX(DatabaseID) FROM @Databases);

-- Get all tables currently without compression over threshold size for each database
WHILE @i <= @j
BEGIN
	SET @DbName = (SELECT DatabaseName FROM @Databases WHERE DatabaseID = @i)
	SET @sqlcommand1 =
	'INSERT #TablesCompress (DatabaseName, TableName, DatabaseSchema, PartitionNumber, DataCompressionDesc, TotalSpaceMB)
	SELECT ' + '''' +
	@DbName +
	+ ''' AS DatabaseName,' +
	+' t.name AS TableName, s.name AS DatabaseSchema,
	p.partition_number, p.data_compression_desc, SUM(a.total_pages) * 8 / 1024 AS TotalSpaceMB
	FROM '
	+@DbName+'.sys.partitions AS p WITH(NOLOCK)
	INNER JOIN '+@DbName+'.sys.tables AS t WITH(NOLOCK) 
		ON t.object_id = p.object_id
	INNER JOIN '+@DbName+'.sys.allocation_units a WITH(NOLOCK) 
		ON p.partition_id = a.container_id
	INNER JOIN '+@DbName+'.sys.Schemas s WITH(NOLOCK) 
		ON t.schema_id = s.schema_id
	LEFT JOIN '+@DbName+'.sys.indexes i WITH(NOLOCK)
		ON t.object_id = i.object_id AND
		i.type IN (1,5)
	WHERE 
		p.data_compression_desc = ''PAGE'' AND
		i.object_id IS NULL
	GROUP BY t.name, p.partition_number, p.data_compression_desc, s.name'
		
	EXEC(@sqlcommand1)
	SET @i += 1
END

-- This uses a function to remove anything that isn't a character from the table name - we put the output in TableName2
-- Reason for this is to remove numbers, special characters like &, #, -, * etc
-- It was easier to just remove all numbers rather than check to see where they appeared in the name
-- We will still use TableName to know which table the index is created but we will name the Index using TableName2
UPDATE #TablesCompress
SET TableName2 = (SELECT dbo.RemoveCharSpecialSymbolValue(TableName))

-- The function looks like this but could be made more sophisticated or could even just be done as a replace on the name
/*
create function dbo.RemoveCharSpecialSymbolValue(@Temp varchar(1000))  
Returns VarChar(1000)
AS
Begin
    Declare @KeepValues as varchar(50)
    Set @KeepValues = '%[^a-z]%'
    While PatIndex(@KeepValues, @Temp) > 0
        Set @Temp = Stuff(@Temp, PatIndex(@KeepValues, @Temp), 1, '')
    Return @Temp
End
*/

-- Remove tables from the temp table which are smaller than the threshold size
DELETE FROM #TablesCompress WHERE TotalSpaceMB < @thresholdsize;
DELETE FROM #TablesCompress WHERE TableName IN ('DDLEvents', 'DDLAudits')

-- We need to protect against non-unique index names
-- Simple fix is to remove records where the name would be reused: group by having count(*) > 1
DELETE FROM #TablesCompress	WHERE DatabaseName+TableName2 IN (
SELECT DatabaseName+TableName2 FROM #TablesCompress 
GROUP BY DatabaseName+TableName2 HAVING COUNT(*) > 1)

-- Next we need to check for index names that already exist in the database
-- Again remove any records which meet this criteria
-- This bit was manually coded - should be put into a loop using the values from @Databases
DELETE t
FROM #TablesCompress t
WHERE DatabaseName = 'Database1' AND EXISTS (
	SELECT * FROM Database1.sys.indexes i
	WHERE TableName2 = i.name)

DELETE t
FROM #TablesCompress t
WHERE DatabaseName = 'Database2' AND EXISTS (
	SELECT * FROM Database2.sys.indexes i
	WHERE TableName2 = i.name)

DELETE t
FROM #TablesCompress t
WHERE DatabaseName = 'Database3' AND EXISTS (
	SELECT * FROM Database3.sys.indexes i
	WHERE TableName2 = i.name)

DELETE t
FROM #TablesCompress t
WHERE DatabaseName = 'Database4' AND EXISTS (
	SELECT * FROM Database4.sys.indexes i
	WHERE TableName2 = i.name)

-- Resequence the IDs into TableID2
UPDATE t
SET TableID2 = t2.SequenceID
FROM
#TablesCompress t
INNER JOIN
(SELECT TableID, ROW_NUMBER() OVER (ORDER BY TableID) AS SequenceID 
FROM #TablesCompress) t2
ON t.TableID = t2.TableID

-- Generate a SQL statemenet for each table to do columnstore index and hence compression
	UPDATE t
	SET SQLStatement = 
		'USE '
		+QUOTENAME(DatabaseName)
		+'; '
		+'CREATE CLUSTERED COLUMNSTORE INDEX cci_'
		+TableName2
		+' ON '
		+QUOTENAME(DatabaseSchema)
		+'.'
		+QUOTENAME(TableName)
	FROM
	#TablesCompress t;

	-- If any records are identified over the size for compression (@threshold) in MB
	-- They will be parsed into the below loop for page compression
	DECLARE @records INT = (SELECT COUNT(*) FROM #TablesCompress);
	
	-- Check if there are any tables found that met the criteria
	IF @records = 0
	BEGIN
		RAISERROR('No Tables to Compress ... %i', 0, 1, @records) WITH NOWAIT;
	END

	IF @records > 0
	BEGIN
		IF @Compress != 1
			BEGIN
				RAISERROR('Tables can be Compressed ... %i', 0, 1, @records)  WITH NOWAIT;
			END
		IF @Compress = 1
		BEGIN
			-- loop from min to max TableID from the table
        		-- reuse @i and @j as variables
			SET @i = (SELECT MIN(TableID2) FROM #TablesCompress);
			SET @j = (SELECT MAX(TableID2) FROM #TablesCompress);
			SET @k = (SELECT COUNT(*) FROM #TablesCompress)
			-- Start Loop to compress tables
			WHILE @i <= @j
				BEGIN
					SET @sqlcommand = (SELECT SQLStatement FROM #TablesCompress WHERE TableID = @i)
					-- Execute the command
					SET @Table = (SELECT TableName FROM #TablesCompress WHERE TableID = @i)
					RAISERROR('Compressing Table ... %s', 0, 1, @Table) WITH NOWAIT;
					EXEC(@sqlcommand)
					SET @i += 1
				END;
			RAISERROR('Tables Compressed ... %i', 0, 1, @k)  WITH NOWAIT;
		END;
	END;


-- For Logging Part 2 - record final time stamp and records updated
SET @EndDate = GETDATE()
UPDATE ETL.dbo.ETLLogsStats
SET
	EndDateTime = @EndDate,
	TotalUpdates = @records
WHERE ETLLogId = @LogId

END
