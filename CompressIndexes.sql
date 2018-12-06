SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*------------------------------------------------------------------------------------------------------ 
Author        : Laurence Pulle
Description   : Identifies Indexes that are not compressed and compresses them
				Tables where the indexes aren't compressed makes them continue to appear in CompressTables.sql
				This is even if the table is compressed as it looks like something is still not compressed
				So need to run this first

Usgage		  : EXEC dbo.CompressIndexes

                 
Date			    Version		Author		          Comment
---------------------------------------------------------------------------------------------------------
24-Jul-2018	     1.0		Laurence Pulle			First version
06-Dec-2018		    1.1		Laurence Pulle			Changed population of #IndexCompress to while loop
--------------------------------------------------------------------------------------------------------- */

ALTER PROC [dbo].[CompressIndexes] AS 
BEGIN 

SET NOCOUNT ON

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
IF OBJECT_ID('tempdb..#IndexesCompress') IS NOT NULL DROP TABLE #IndexesCompress
CREATE TABLE #IndexesCompress (
	IndexID INT IDENTITY
	,DatabaseName VARCHAR(50)
	,IndexName VARCHAR(100)
	,TableName VARCHAR(100)
	,DatabaseSchema VARCHAR(20)
	,DataCompressionCode INT
	,DataCompressionDesc VARCHAR(10)
	,SQLStatement VARCHAR(MAX)
	PRIMARY KEY (IndexID)
		);

-- Populate loop variables 
-- Put names of the databases you're interested in in the @Databases table variable
INSERT @Databases (DatabaseName) 
VALUES ('Database1'),('Database2'),('Database3'),('Database4');

-- Loop from @i to @j (1 to MAX(DatabaseID))
SET @i = 1;
SET @j = (SELECT MAX(DatabaseID) FROM @Databases);

-- Get all tables currently without compression over threshold size for each database
WHILE @i <= @j
BEGIN
	SET @DbName = (SELECT DatabaseName FROM @Databases WHERE DatabaseID = @i)
	SET @sqlcommand1 =
	'INSERT #IndexesCompress  (DatabaseName, IndexName, TableName, DatabaseSchema, DataCompressionCode, DataCompressionDesc)
	SELECT ' + '''' +
	@DbName +
	+ ''' AS DatabaseName,' +
	+'i.name AS IndexName,
		t.name AS TableName, 
		s.name AS DatabaseSchema,
		p.data_compression, 
		p.data_compression_desc
		FROM 
		IntegratedCareLinked.sys.indexes AS i WITH(NOLOCK)
		INNER JOIN IntegratedCareLinked.sys.partitions AS p WITH(NOLOCK) ON i.object_id = p.object_id AND i.index_id = p.index_id
		INNER JOIN IntegratedCareLinked.sys.tables AS t WITH(NOLOCK) ON t.object_id = i.object_id
		INNER JOIN IntegratedCareLinked.sys.Schemas s WITH(NOLOCK) ON t.schema_id = s.schema_id
		WHERE 
			p.data_compression = 0 AND
			i.name IS NOT NULL'
		EXEC(@sqlcommand1)
		SET @i += 1
END

-- Generate a SQL statemenet for each index to do page compression
	UPDATE t
	SET SQLStatement = 
		'USE '
		+QUOTENAME(DatabaseName)
		+'; '
		+'ALTER INDEX '
		+QUOTENAME(IndexName)
		+'ON '
		+QUOTENAME(DatabaseSchema)
		+'.'
		+QUOTENAME(TableName)
		+' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)'
	FROM
	#IndexesCompress t;

	-- If any records are identified they will be parsed into the below loop for page compression
	DECLARE @records INT = (SELECT COUNT(*) FROM #IndexesCompress);

	-- Check if there are any indexes found that are not compressed
	IF @records = 0
	BEGIN
		RAISERROR('No Indexes to Compress ... %i', 0, 1, @records) WITH NOWAIT;
	END

	IF @records > 0
			BEGIN
				-- reuse @i and @j as variables
				SET @i = (SELECT MIN(IndexID) FROM #IndexesCompress)
				SET @j = (SELECT MAX(IndexID) FROM #IndexesCompress)

				-- Start Loop
				WHILE @i <= @j
					BEGIN
						SET @sqlcommand = (SELECT SQLStatement FROM #IndexesCompress WHERE IndexID = @i)
						-- Execute the command
						SET @Table = (SELECT IndexName FROM #IndexesCompress WHERE IndexID = @i)
						RAISERROR('Compressing Index ... %s', 0, 1, @Table) WITH NOWAIT;
						EXEC(@sqlcommand)
						SET @i += 1
					END;
				RAISERROR('Indexes Compressed ... %i', 0, 1, @j)  WITH NOWAIT;
	END;

END
