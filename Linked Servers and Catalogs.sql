/*
-- SQLCMD options to spool output to file
:CONNECT 'Enter SQLServer name'
:SETVAR Path "H:\"
:SETVAR FileName "LinkedServers.csv"
:OUT $(Path)$(FileName)
*/

SET NOCOUNT ON
-- Drop #LinkedServers
IF OBJECT_ID(N'tempdb..#LinkedServers', N'U') IS NOT NULL DROP TABLE #LinkedServers;

-- Create #LinkedServers
CREATE TABLE #LinkedServers (
	SRV_ID INT IDENTITY,
	SRV_RUN VARCHAR(MAX),
	SRV_NAME NVARCHAR(256), 
	SRV_PROVIDERNAME NVARCHAR(256), 
	SRV_PRODUCT  NVARCHAR(256), 
	SRV_DATASOURCE NVARCHAR(4000),
	SRV_PROVIDERSTRING NVARCHAR(4000), 
	SRV_LOCATION NVARCHAR(4000), 
	SRV_CAT NVARCHAR(256)
		);

-- Insert into #LinkedServers
INSERT INTO #LinkedServers (
	SRV_NAME, SRV_PROVIDERNAME, SRV_PRODUCT, SRV_DATASOURCE
	, SRV_PROVIDERSTRING, SRV_LOCATION, SRV_CAT)
SELECT 
a.name, a.provider, a.product, 
a.data_source, a.provider_string, 
a.location, a.catalog
FROM sys.Servers a

UPDATE #LinkedServers
SET SRV_RUN = @@SERVERNAME

-- Drop #Catalogs
IF OBJECT_ID(N'tempdb..#Catalogs', N'U') IS NOT NULL DROP TABLE #Catalogs;

CREATE TABLE #Catalogs (
	SRV_RUN VARCHAR(MAX),
	SRV_NAME VARCHAR(MAX),
	CATALOG_NAME VARCHAR(MAX),
	DESCRIPTION VARCHAR(MAX)
	)

DECLARE @i INT
DECLARE @SRV_NAME VARCHAR(MAX)
DECLARE @maxi INT = (SELECT MAX(SRV_ID) FROM #LinkedServers)

SET @i = 1

WHILE @i <= @maxi
BEGIN
	DECLARE @xstate int;
	SET @SRV_NAME = (SELECT SRV_NAME FROM #LinkedServers WHERE SRV_ID = @i)
	BEGIN TRY
		INSERT INTO #Catalogs (CATALOG_NAME, DESCRIPTION)
			EXEC sp_catalogs @SRV_NAME
			IF @@ROWCOUNT != 0
			UPDATE c
				SET
				SRV_RUN = @@SERVERNAME,
				SRV_NAME = @SRV_NAME
				FROM #Catalogs c
					WHERE SRV_RUN IS NULL
	END TRY

-- Error Handler
-- This is to catch Linked Servers where the server isn't configured for connection
	BEGIN CATCH
		DECLARE @ErrorText NVARCHAR(max)
		SELECT @ErrorText = 'Error caught:' + 
		' N-' + cast(ERROR_NUMBER() as nvarchar(max)) +
		' S-' + cast(ERROR_SEVERITY() as nvarchar(max)) +
		' S-' + cast(ERROR_STATE() as nvarchar(max)) +
		' P-' + cast(ERROR_PROCEDURE() as nvarchar(max)) +
		' L-' + cast(ERROR_LINE() as nvarchar(max)) +
		' M-' + cast(ERROR_MESSAGE() as nvarchar(max)), 
			@xstate = XACT_STATE()
			SET @i = @i + 1
			--IF @xstate <> 0 ROLLBACK
			;
	END CATCH;
	
	SET @i += 1
END

SELECT * FROM #Catalogs;
