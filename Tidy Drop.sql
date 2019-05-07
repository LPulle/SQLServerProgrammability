/*------------------------------------------------------------------------------------------------------ 
Author        : LP
Description   : Drop temp tables without error messages

When you drop a temp table which doesn't exist in a sequence of code it will continue but give an error
To prevent this you can do a check using IF statement to check if the temp table is there before
attempting to drop it
This also works for views, stored procedures, functions and maybe others too

In the code below I show the error you get when you try to drop a table that doesn't exist
Then that you don't get an error when you check first
The final query is when you have a lot of temp tables that you want to drop in one go
It checks the sysobjects table in the tempdb for all temp tables and drops them in one statement
This prevents the need to drop them one by one in individual statements

--------------------------------------------------------------------------------------------------------- */
DROP TABLE #temp;

/* ERROR Warning
 Msg 3701, Level 11, State 5, Line 1
 Cannot drop the table '#temp', because it does not exist or you do not have permission.
*/

-- Tidy drop 1 table
IF OBJECT_ID(N'tempdb..#temp', N'U') IS NOT NULL DROP TABLE #temp;

-- Tidy drop all temp tables
DECLARE @sql NVARCHAR(MAX)
SELECT @sql = ISNULL(@sql+';', '') + 'DROP TABLE ' + QUOTENAME(name)
FROM tempdb..sysobjects
WHERE name LIKE '#%' AND name NOT LIKE '##%'
EXEC (@sql);
 
