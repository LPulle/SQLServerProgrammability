SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*------------------------------------------------------------------------------------------------------ 
Description  	Identifies tables over a certain size for specific databases
		Parameter @AllowCompress is set to 1 by default and will result
		in any tables found being compressed. If set to 0 will simply
		report back that tables have been found that can be compressed.
		Compression is done as PAGE rather than ROW level.
		Tables will be rebuilt with this using an alter table statement.
		Threshold is set to 100mb by default but can be changed in the
		parameter @thresholdsize

Usgage: 	EXEC dbo.CompressTables
		EXEC dbo.CompressTables @AllowCompress = 1, @thresholdsize = 100
		EXEC dbo.CompressTables @AllowCompress = 0
		EXEC dbo.CompressTables @AllowCompress = 1, @thresholdsize = 300
		EXEC dbo.CompressTables @AllowCompress = 0, @thresholdsize = 300

                 
Date			Version		Author 		Comment
---------------------------------------------------------------------------------------------------------
10-Jul-2017	   	 1.0		LP		First version
27-Jul-2017		 1.1		LP		Added @thresholdsize Added description and documentation
01-Aug-2017		 1.2		LP		Added WITH(NOLOCK) to system tables (prevent locking)
06-Dec-2018		 1.3		LP		Changed population of #TableCompress to use while loop

--------------------------------------------------------------------------------------------------------- */
CREATE PROCEDURE dbo.CompressTables (
      @AllowCompress INT = 1, @thresholdsize INT = 100) AS
BEGIN

SET NOCOUNT ON
-- Threshold for table size >= this size will be compressed
	DECLARE 
		@threshold INT = @thresholdsize
		,@Compress INT = @AllowCompress;

-- Variables for loop
	DECLARE @Databases TABLE (DatabaseID INT IDENTITY, DatabaseName VARCHAR(100));
	DECLARE 
		@DbName VARCHAR(100)
		,@sqlcommand1 VARCHAR(MAX)
		,@i INT
		,@j INT
      		,@sqlcommand VARCHAR(MAX)
      		,@Table VARCHAR(MAX);

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
			FROM IntegratedCareLinked.sys.partitions AS p WITH(NOLOCK)
			INNER JOIN IntegratedCareLinked.sys.tables AS t WITH(NOLOCK) ON t.object_id = p.object_id
			INNER JOIN IntegratedCareLinked.sys.allocation_units a WITH(NOLOCK) ON p.partition_id = a.container_id
			INNER JOIN IntegratedCareLinked.sys.Schemas s WITH(NOLOCK) ON t.schema_id = s.schema_id
			WHERE 
				p.data_compression_desc = ''NONE''
			GROUP BY t.name, p.partition_number, p.data_compression_desc, s.name'
			EXEC(@sqlcommand1)
			SET @i += 1
END

-- Remove tables from the temp table which are smaller than the threshold size
DELETE FROM #TablesCompress WHERE TotalSpaceMB < @thresholdsize;

-- Generate a SQL statemenet for each table to do page compression
	UPDATE t
	SET SQLStatement = 
		'USE '
		+QUOTENAME(DatabaseName)
		+'; '
		+'ALTER TABLE '
		+QUOTENAME(DatabaseSchema)
		+'.'
		+QUOTENAME(TableName)
		+' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)'
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
				RAISERROR('Tables can be Compressed ... %i', 0, 1, @Compress)  WITH NOWAIT;
			END
		IF @Compress = 1
		BEGIN
			-- loop from min to max TableID from the table
        		-- reuse @i and @j as variables
			SET @i = (SELECT MIN(TableID) FROM #TablesCompress);
			SET @j = (SELECT MAX(TableID) FROM #TablesCompress);
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
			RAISERROR('Tables Compressed ... %i', 0, 1, @j)  WITH NOWAIT;
		END;
	END;
END
