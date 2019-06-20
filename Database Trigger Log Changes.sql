/*------------------------------------------------------------------------------------------------------ 
Description   	Create a table and Database Trigger to log changes 
		made to all objects and new objects created
		Logs user, date and time of change and SQL executed

Variation on original by Aaron Bertrand 2010-08-09:
https://www.mssqltips.com/sqlservertip/2085/sql-server-ddl-triggers-to-track-all-database-changes/
                 
Date		Version		Author		Comment
---------------------------------------------------------------------------------------------------------
04-Aug-2015	1.0		LP		First version
07-Dec-2018	1.1		LP		Tidy up for sharing
20-Jun-2019	1.2		LP		Added NewObjectName field for tracking renamed objects
--------------------------------------------------------------------------------------------------------- */

-- Create the table for storing changes
CREATE TABLE dbo.DDLEvents (
	ID INT IDENTITY(1,1) NOT NULL,
	EventDate DATETIME NOT NULL,
	EventType NVARCHAR(64) NULL,
	EventDDL NVARCHAR(max) NULL,
	EventXML XML NULL,
	DatabaseName NVARCHAR(255) NULL,
	SchemaName NVARCHAR(255) NULL,
	ObjectName NVARCHAR(255) NULL,
	NewObjectName NVARCHAR(255) NULL,
	HostName VARCHAR(64) NULL,
	ProgramName NVARCHAR(255) NULL,
	LoginName NVARCHAR(255) NULL
) ON PRIMARY TEXTIMAGE_ON PRIMARY
GO

-- Default Current date and time to EventDate for each entry
ALTER TABLE dbo.DDLEvents ADD  DEFAULT (GETDATE()) FOR EventDate
GO

-- Create index on ID field
CREATE CLUSTERED INDEX [ci_DDLEvents] ON [dbo].[DDLEvents]
(
	[ID] ASC)
WITH (
	PAD_INDEX = OFF, 
	STATISTICS_NORECOMPUTE = OFF, 
	SORT_IN_TEMPDB = OFF, 
	DROP_EXISTING = OFF, 
	ONLINE = OFF, 
	ALLOW_ROW_LOCKS = ON, 
	ALLOW_PAGE_LOCKS = ON
	) ON [PRIMARY]
GO

-- Drop Trigger (only include if it already exists)
DROP TRIGGER DDLTrigger ON DATABASE
GO

-- Create the trigger - Specific DDL events can be removed if not required
CREATE TRIGGER DDLTrigger
    ON DATABASE
    FOR
	-- DDL Events included
	CREATE_TABLE, ALTER_TABLE, DROP_TABLE, 
	CREATE_VIEW, ALTER_VIEW, DROP_VIEW, 
	CREATE_PROCEDURE, ALTER_PROCEDURE, DROP_PROCEDURE, 
	CREATE_SCHEMA, ALTER_SCHEMA, DROP_SCHEMA,
	CREATE_FUNCTION, ALTER_FUNCTION, DROP_FUNCTION,
	CREATE_TYPE, DROP_TYPE,
	CREATE_INDEX, ALTER_INDEX, DROP_INDEX,
	CREATE_ROLE, ALTER_ROLE, DROP_ROLE,
	RENAME
AS

BEGIN
    SET NOCOUNT ON;
    DECLARE
        @EventData XML = EVENTDATA();
 
    INSERT dbo.DDLEvents
    (
        EventType,
        EventDDL,
        EventXML,
        DatabaseName,
        SchemaName,
        ObjectName,
        NewObjectName,
        HostName,
        ProgramName,
        LoginName
    )
    SELECT
        @EventData.value('(/EVENT_INSTANCE/EventType)1',   'NVARCHAR(100)'), 
        @EventData.value('(/EVENT_INSTANCE/TSQLCommand)1', 'NVARCHAR(MAX)'),
        @EventData,
        DB_NAME(),
        @EventData.value('(/EVENT_INSTANCE/SchemaName)1',  'NVARCHAR(255)'), 
        @EventData.value('(/EVENT_INSTANCE/ObjectName)1',  'NVARCHAR(255)'),
	ISNULL(@EventData.value('(/EVENT_INSTANCE/NewObjectName)[1]',  'NVARCHAR(255)'), @EventData.value('(/EVENT_INSTANCE/ObjectName)[1]',  'NVARCHAR(255)')),
        HOST_NAME(),
        PROGRAM_NAME(),
        SUSER_SNAME();
END

GO

-- Enable the trigger
ENABLE TRIGGER DDLTrigger ON DATABASE
GO

-- Below is if you want to remove a specific entry from DDLEvents table
DECLARE @DDLID INT = 'Enter ID number here'
DELETE FROM dbo.DDLEvents WHERE ID = @DDLID
-- Reseed the Identity ID field on DDLEvents
DECLARE @reseed INT = (SELECT MAX(ID) FROM dbo.DDLEvents)
DBCC CHECKIDENT ([DDLEvents], RESEED, @reseed)
