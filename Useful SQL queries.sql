-- 2 digit month (mm include leading 0 when m<10)
REPLACE(STR(DATEPART(mm, ActivityDate), 2), ' ', '0')

-- year month syntax yyyy-mm
-- 2 methods: 1) Construct with CAST, STR, DATEPART 2) CONVERT function
year_month = CAST(DATEPART(YEAR, ActivityDate) as CHAR(4))+ '-'+
CAST(REPLACE(STR(DATEPART(mm, ActivityDate), 2), ' ', '0') as CHAR(2))
year_month 

alternative:
year_month = CONVERT(VARCHAR(7), GETDATE(), 121)

-- Increase date to the next available Sunday except when day is already a Sunday then  use that date
CASE 
WHEN DATENAME(WEEKDAY, ActivityDate) = 'Sunday' 
	THEN LEFT(CAST(ActivityDate AS DATE),10)
	ELSE
	CAST(DATEADD (D, -1 * DATEPART (DW, ActivityDate) + 8,ActivityDate) AS date) 
END AS WeekEnding


-- Remove the timestamp in a datetime field to just leave the date
-- For Sql Server 2008 and later:
CAST(GETDATE() As Date) 

-- For Sql Server pre 2008 where the above doesn't work:
DATEADD(dd, DATEDIFF(dd,0, GETDATE()), 0) 

-- List all objects in database including counts and module description when object is a view/function/stored proc
SELECT
o.object_id, 
s.name AS SchemaName, 
o.name AS ObjectName, 
o.type_desc, definition,
create_date, modify_date
FROM
sys.objects AS o 
LEFT JOIN sys.sql_modules AS m 
on m.object_id = o.object_id
INNER JOIN sys.schemas  s
ON o.schema_id = s.schema_id
WHERE 
	o.type_desc not like '%CONSTRAINT%' AND
	s.name <> 'sys'


-- sp_help
-- Get information on a table
USE tempdb
EXEC sp_help 'sys.all_objects'

-- For temp tables you need to specify sp_help function from tempdb
tempdb..sp_help '#temp'

-- List all databases and schemas on server
DECLARE @SQL NVARCHAR(MAX)
SELECT @SQL = STUFF((SELECT ' UNION ALL
SELECT ' +  + QUOTENAME(name,'''') + ' as DbName, cast(Name as varchar(128)) COLLATE DATABASE_DEFAULT 
AS Schema_Name FROM ' + QUOTENAME(name) + '.sys.schemas'
FROM sys.databases
Order BY [name] FOR XML PATH(''),type).value('.','nvarchar(max)'),1,12,'')
SET @SQL = @SQL + ' ORDER BY DbName, Schema_Name'
EXECUTE (@SQL)

-- Active SQL sessions and who initiated the query
-- Commented fields give the command and query plan 
-- To include you need to uncomment the relevant cross apply statements aswell
SELECT  
S.session_id, @@SPID, S.host_name, S.program_name, S.login_name, 
R.request_id, ERQ.status, ERQ.command, ERQ.sql_handle, ERQ.blocking_session_id, 
ERQ.date_format, ERQ.cpu_time, ERQ.wait_type, ERQ.wait_time, 
SUM(SU.internal_objects_alloc_page_count) AS alloc_pages, 
SUM(SU.internal_objects_dealloc_page_count) AS dealloc_pages, 
SUM(DDSSU.user_objects_alloc_page_count*8/1024) AS SpaceUsedMB
--, ST.text
--, QP.query_plan
FROM sys.dm_exec_sessions AS S
INNER JOIN sys.dm_exec_requests AS R
ON S.session_id = R.session_id
INNER JOIN sys.dm_db_task_space_usage AS SU WITH (NOLOCK) 
ON S.session_id = SU.session_id
INNER JOIN sys.dm_exec_requests ERQ WITH (NOLOCK)
ON SU.session_id = ERQ.session_id AND 
SU.request_id = ERQ.request_id
INNER JOIN sys.dm_db_session_space_usage DDSSU WITH(NOLOCK)
ON S.session_id = DDSSU.session_id
--CROSS APPLY sys.dm_exec_sql_text(R.sql_handle) AS ST
--CROSS APPLY sys.dm_exec_query_plan(R.plan_handle) AS QP
WHERE   
	S.is_user_process = 1 AND
	S.session_id <> @@SPID
GROUP BY
S.session_id, @@SPID, S.host_name, S.program_name, S.login_name, 
R.request_id, ERQ.status, ERQ.command, ERQ.sql_handle, ERQ.blocking_session_id, 
ERQ.date_format, ERQ.cpu_time, ERQ.wait_type, ERQ.wait_time
