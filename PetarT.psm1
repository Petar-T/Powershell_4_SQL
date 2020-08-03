<#
 .Synopsis
  Displays missing indexes on SQL server.

 .Description
  Displays a list of missing indexes on targeted server. This function supports filtering number or results,
  filtering specific databases or just getting scripts.

 .Parameter ServerName
  Name of SQL server. Could be default or ServerName\InstanceName

 .Parameter Database
  Name of targeted database. Filters results to just one database, otherwise all user databases are used. 
  
 .Parameter TopN
  List Top results. Default 20. use 0 for all

 .Parameter -GetScript 
  Will result in just returning scripts for missing indexes. Default : false  

 .Example
   # Show top 20 missing indexes on instance
   Get-MissingIndexes -Server "MyServer\Inst1"

 .Example
   # Show all missing indexes on instance
   Get-MissingIndexes -Server "MyServer\Inst1" -TopN 0

 .Example
   # Show top 20 missing indexes on database AdventureWorks.
   Get-MissingIndexes -Server "MyServer\Inst1" -Database "AdventureWorks" 

 .Example
   # Show script only for all missing indexes on database AdventureWorks.
   Get-MissingIndexes -Server "MyServer\Inst1" -TopN 0 -Database "AdventureWorks666" -GetScript true 
#>
function Get-MissingIndexes{
param(
    [String] $ServerName,
    [String] $DatabaseName,
    [int] $TopN=20,
    [switch] $GetScript
    )

if ($TopN -eq 0){
   $par1=" "}
   else{
   $par1=" top {0} " -f $TopN }

if (!$DatabaseName){
   $par4=" where db.[database_id] > 4 "}
   else{
   $par4=" where db.[database_id] = DB_ID('{0}') " -f $DatabaseName }

 If ($GetScript){
    $par2 = "/*"
    $par3 = "*/"}
   else {
    $par2 = ""
    $par3 = ""}
 

$Query="Select {0} {1} 
 @@ServerName as ServerName 
    ,db.[name] AS [DatabaseName]
    ,id.[object_id] AS [ObjectID]
	,OBJECT_NAME(id.[object_id], db.[database_id]) AS [ObjectName]
    ,id.[statement] AS [FullyQualifiedObjectName]
    ,id.[equality_columns] AS [EqualityColumns]
    ,id.[inequality_columns] AS [InEqualityColumns]
    ,id.[included_columns] AS [IncludedColumns]
    ,gs.[unique_compiles] AS [UniqueCompiles]
    ,gs.[user_seeks] AS [UserSeeks]
    ,gs.[user_scans] AS [UserScans]
    ,gs.[last_user_seek] AS [LastUserSeekTime]
    ,gs.[last_user_scan] AS [LastUserScanTime]
    ,gs.[avg_total_user_cost] AS [AvgTotalUserCost]  
    ,gs.[avg_user_impact] AS [AvgUserImpact]  
    ,gs.[system_seeks] AS [SystemSeeks]
    ,gs.[system_scans] AS [SystemScans]
    ,gs.[last_system_seek] AS [LastSystemSeekTime]
    ,gs.[last_system_scan] AS [LastSystemScanTime]
    ,gs.[avg_total_system_cost] AS [AvgTotalSystemCost]
    ,gs.[avg_system_impact] AS [AvgSystemImpact]  
    ,((gs.[user_seeks]+gs.[user_scans]) * gs.[avg_total_user_cost] * gs.[avg_user_impact]) AS [Overall_Impact]
    , {2}
    'CREATE INDEX [IX_' + OBJECT_NAME(id.[object_id], db.[database_id]) + '_' + REPLACE(REPLACE(REPLACE(ISNULL(id.[equality_columns], ''), ', ', '_'), '[', ''), ']', '') + CASE
        WHEN id.[equality_columns] IS NOT NULL
            AND id.[inequality_columns] IS NOT NULL
            THEN '_'
        ELSE ''
        END + REPLACE(REPLACE(REPLACE(ISNULL(id.[inequality_columns], ''), ', ', '_'), '[', ''), ']', '') + '_' + LEFT(CAST(NEWID() AS [nvarchar](64)), 5) + ']' + ' ON ' + id.[statement] + ' (' + ISNULL(id.[equality_columns], '') + CASE
        WHEN id.[equality_columns] IS NOT NULL
            AND id.[inequality_columns] IS NOT NULL
            THEN ','
        ELSE ''
        END + ISNULL(id.[inequality_columns], '') + ')' + ISNULL(' INCLUDE (' + id.[included_columns] + ')', '') AS [ProposedIndex]
    ,CAST(CURRENT_TIMESTAMP AS [smalldatetime]) AS [CollectionDate]
FROM [sys].[dm_db_missing_index_group_stats] gs WITH (NOLOCK)
INNER JOIN [sys].[dm_db_missing_index_groups] ig WITH (NOLOCK) ON gs.[group_handle] = ig.[index_group_handle]
INNER JOIN [sys].[dm_db_missing_index_details] id WITH (NOLOCK) ON ig.[index_handle] = id.[index_handle]
INNER JOIN [sys].[databases] db WITH (NOLOCK) ON db.[database_id] = id.[database_id]
{3}
--AND OBJECT_NAME(id.[object_id], db.[database_id]) = 'YourTableName'
ORDER BY ((gs.[user_seeks]+gs.[user_scans]) * gs.[avg_total_user_cost] * gs.[avg_user_impact]) DESC" -f $Par1 ,$Par2 ,$Par3 ,$Par4 


$tbl= Invoke-Sqlcmd -Query $Query -ServerInstance $ServerName
$tbl
#Write-Host 'Total found missing indexes {0}' -f $tbl.Rows.Count

}
<#
 .Synopsis
  Displays non-used indexes on SQL server.

 .Description
  Displays a list of non-used nonclustered indexes on targeted server. This function supports filtering per specific database,
  or just getting scripts.

 .Parameter ServerName
  Name of SQL server. Could be default or ServerName\InstanceName

 .Parameter Database
  Name of targeted database. Filters results to just one database, otherwise all user databases are used. 
  
 .Parameter -GetScript 
  Will result in just returning scripts for unused indexes. Default : false  

 .Example
   # Show all unused indexes on instance
   Get-unusedIndexes -Server "MyServer\Inst1"

 .Example
   # Show all unused indexes on specific database 
   Get-unusedIndexes -Server "MyServer\Inst1" -Database "AdventureWorks" 

 .Example
   # Show script to drop all unused indexes on specific database
   Get-unusedIndexes -Server "MyServer\Inst1" -Database "AdventureWorks" -GetScript true 
#>
function Get-unusedIndexes{
param(
    [String] $ServerName,
    [String] $DatabaseName,
    [switch] $GetScript
    )

$TotalFound=0

if (!$DatabaseName){
    $dbs = invoke-sqlcmd -ServerInstance $ServerName -Query "Select name, database_ID from sys.databases where database_ID > 4" }
  else{
   $dbs = invoke-sqlcmd -ServerInstance $ServerName -Query "Select name, database_ID from sys.databases where [name] = '$DatabaseName'"}


 If ($GetScript){
    $par1 = "/*"
    $par2 = "*/"}
   else {
    $par1 = ""
    $par2 = ""}
 
$Query="select {0}
db_Name() as DatabaseName,
OBJECT_SCHEMA_NAME(i.object_id ) as Schema_Name,
object_name(i.object_id) as ObjectName, 
i.name as [Unused Index],
MAX(p.rows) Rows,
SUM(a.used_pages) * 8 AS 'Indexsize(KB)', 
case  
    when i.type = 0 then 'Heap'  
    when i.type= 1 then 'clustered' 
    when i.type=2 then 'Non-clustered'   
    when i.type=3 then 'XML'   
    when i.type=4 then 'Spatial'  
    when i.type=5 then 'Clustered xVelocity memory optimized columnstore index'   
    when i.type=6 then 'Nonclustered columnstore index'  
end index_type, 
{1}
'DROP INDEX ' + i.name + ' ON ' + '[' + db_Name() + '].[' + OBJECT_SCHEMA_NAME(i.object_id ) + '].[' + object_name(i.object_id) + ']' as 'Drop Statement' 
--'DROP INDEX ' + i.name + ' ON ' + '[' + OBJECT_SCHEMA_NAME(i.object_id ) + '].[' + object_name(i.object_id) + ']' as 'Drop Statement' 
from sys.indexes i 
left join sys.dm_db_index_usage_stats s on s.object_id = i.object_id 
     and i.index_id = s.index_id 
     and s.database_id = db_id() 
left join sys.extended_properties ep on i.object_id = ep.major_id  
JOIN sys.partitions AS p ON p.OBJECT_ID = i.OBJECT_ID AND p.index_id = i.index_id 
JOIN sys.allocation_units AS a ON a.container_id = p.partition_id 
where objectproperty(i.object_id, 'IsIndexable') = 1 
AND objectproperty(i.object_id, 'IsIndexed') = 1 
and objectproperty(i.object_id,'IsUserTable') = 1
and (ep.name is Null or    ep.name <> 'microsoft_database_tools_support')
and i.type  = 2 -- just nonclustered
and s.index_id is null -- and dm_db_index_usage_stats has no reference to this index 
or (s.user_updates > 0 and s.user_seeks = 0 and s.user_scans = 0 and s.user_lookups = 0)-- index is being updated, but not used by seeks/scans/lookups 
GROUP BY OBJECT_SCHEMA_NAME(i.object_id ),object_name(i.object_id) ,i.name,i.type 
order by object_name(i.object_id) asc" -f $par1, $par2 


 
foreach ($db in $dbs)
{
    $tbl= Invoke-Sqlcmd -Database $db.name -Query $Query -ServerInstance $ServerName
    $tbl
    $TotalFound+=$tbl.Rows.Count
}

#Write-Host ("Total found unused indexes {0}" -f $TotalFound) -ForegroundColor yellow 

}
<#
 .Synopsis
  Displays most expensive queries on SQL server.

 .Description
  Displays a list of expensive queries on targeted server. This function supports filtering number or results.

 .Parameter ServerName
  Name of SQL server. Could be default or ServerName\InstanceName

 .Parameter TopN
  List Top results. Default 20. use 0 for all

 .Parameter DoNotIncludePlan
  Do not include execution plan for queries. Default : false  

 .Example
   # Show top 20 most expensive queries on Server
   Get-ExpensiveQueries -Server "MyServer\Inst1"

 .Example
   # Show top 10 most expensive queries on Server without plans
   Get-ExpensiveQueries -Server "MyServer\Inst1" -TopN 10 -DoNotIcludePlan
#>
function Get-ExpensiveQueries{
param(
    [String] $ServerName,
    [int] $TopN=20,
    [switch] $DoNotIncludePlan 
    )

if ($TopN -eq 0){
   $par1=" "}
   else{
   $par1=" top {0} " -f $TopN }


    If ($DoNotIncludePlan){
    $par2 = "/*"
    $par3 = "*/"}
   else {
    $par2 = ""
    $par3 = ""}

$Query="SELECT {0} SUBSTRING(qt.TEXT, (qs.statement_start_offset/2)+1  ,
((CASE qs.statement_end_offset
WHEN -1 THEN DATALENGTH(qt.TEXT)
ELSE qs.statement_end_offset
END - qs.statement_start_offset)/2)+1) as statement,
qs.execution_count,
qs.total_logical_reads, qs.last_logical_reads,
qs.total_logical_writes, qs.last_logical_writes,
qs.total_worker_time,
qs.last_worker_time,
qs.total_elapsed_time/1000000 total_elapsed_time_in_S,
qs.last_elapsed_time/1000000 last_elapsed_time_in_S,
qs.last_execution_time
{1},qp.query_plan{2}
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
--ORDER BY qs.total_logical_reads DESC -- logical reads
 ORDER BY qs.total_worker_time DESC -- CPU time" -f $par1, $par2, $par3  

$tbl= Invoke-Sqlcmd -Query $Query -ServerInstance $ServerName
$tbl

}
<#
 .Synopsis
  Displays warnings from default trace within last 24 hours
 

 .Description
  Displays warnings from default trace, supporting detailed view and different time span
  List is limited to filter only:
  55     Hash Warning
  69     Sort Warnings
  79     Missing Column Statistics
  80     Missing Join Predicate
 
  .Parameter ServerName
  Name of SQL server. Could be default or ServerName\InstanceName

 .Parameter HoursLast
  List results found in last n Hours. Default 24 

 .Parameter showDetail
  List detail vs grouped results count. Default : false 

 .Example
   # Show warnings from trace hapened in last 24 hours.
   Get-Warnings -Server "MyServer\Inst1"

 .Example
   # Show warnings from trace hapened in last 2 hours.
   Get-Warnings -Server "MyServer\Inst1" -HoursLast 2

 .Example
   # Show detailed warnings from trace hapened in last 24 hours.
   Get-Warnings -Server "MyServer\Inst1" -HoursLast 2 -showDetail
#>
function Get-Warnings{
param(
    [String] $ServerName,
    [int] $HoursLast=24,
    [switch] $showDetail
    )

    If ($showDetail){
    $par1 = "--"
    $par2 = ""}
   else {
    $par1 = ""
    $par2 = "--"}

$Query="DECLARE @path NVARCHAR(260)  
SELECT @path=path FROM sys.traces WHERE is_default = 1

{0}SELECT  QUOTENAME(TE.name) as Event_Name, T.DatabaseID ,  Count(*) as EventsNum  
{1}SELECT  QUOTENAME(TE.name) as Event_Name, T.DatabaseID ,  (t.RowCounts) as TotalRows, (t.CPU) as TotalCPU   ,t.TextData, t.ApplicationName, t.StartTime , t.EndTime
        FROM sys.fn_trace_gettable(@path, DEFAULT) T
              inner join sys.trace_events TE ON T.EventClass = TE.trace_event_id
			 
              WHERE T.EventClass IN (55,79,80,69)
			  And  T.StartTime >= DATEADD(hour, -1*{2}, GETDATE())
			  {0}Group by QUOTENAME(TE.name), T.DatabaseID" -f $par1, $par2, $HoursLast 

$tbl= Invoke-Sqlcmd -Query $Query -ServerInstance $ServerName
$tbl
}

function createDT()
{
    $tempTable = New-Object System.Data.DataTable
   
    $col1 = New-Object System.Data.DataColumn(“Matching”)
    $col2 = New-Object System.Data.DataColumn(“Source_Index”)
    $col3 = New-Object System.Data.DataColumn(“Destination_Index”)
    $col4 = New-Object System.Data.DataColumn(“Source_ScriptFull”)
    $col5 = New-Object System.Data.DataColumn(“Source_Fields”)
    $col6 = New-Object System.Data.DataColumn(“Source_IncludedFields”)
    $col7 = New-Object System.Data.DataColumn(“Destination_ScriptFull”)
    $col8 = New-Object System.Data.DataColumn(“Destination_Fields”)
    $col9 = New-Object System.Data.DataColumn(“Destination_IncludedFields”)
    
    $tempTable.columns.Add($col1)
    $tempTable.columns.Add($col2)
    $tempTable.columns.Add($col3)
    $tempTable.columns.Add($col4)
    $tempTable.columns.Add($col5)
    $tempTable.columns.Add($col6)
    $tempTable.columns.Add($col7)
    $tempTable.columns.Add($col8)
    $tempTable.columns.Add($col9)
       
    return ,$tempTable
}
<#
 .Synopsis
  Displays list of indexes that are not matching on two SQL server databases.

 .Description
  Displays a list of indexes on two different databases in common matrix. This function is typically used when DBA needs to check if sourca and target indexes are the same.
  filtering specific databases or just getting scripts.

 .Parameter Source_Server
  Name of source SQL server. Could be default or ServerName\InstanceName

 .Parameter Source_Database
  Name of source database. Filters results to just one database, otherwise all user databases are used. 
  
 .Parameter Destination_Server
  Name of destination SQL server. Could be default or ServerName\InstanceName

 .Parameter Destination_Database
  Name of destination database. Filters results to just one database, otherwise all user databases are used. 

 .Parameter -Unmatched_Only 
  Will result in just returning list for indexes not identical in source and destination databases. Default : false  

 .Example
   # Show list of all indexes in both databases in Matrix view
   Match_Indexes -Source_Server 'MyServer' -Source_Database 'AdventureWorksDW2012' -Destination_Server 'myServer' -Destination_Database 'AdventureWorksDW2012_Clone' 


 .Example
    # Show list of all indexes in both databases in Matrix view reduced to only those non identical 
    Match_Indexes -Source_Server 'MyServer' -Source_Database 'AdventureWorksDW2012' -Destination_Server 'myServer' -Destination_Database 'AdventureWorksDW2012_Clone' -Unmatched_Only

#>
function Match_Indexes{
param(
    [String] $Source_Server,
    [String] $Source_Database,
    [String] $Destination_Server,
    [String] $Destination_Database,
    [switch] $Unmatched_Only
    )


[System.Data.DataTable]$dTable = createDT


$SqlQuery="SET nocount on
declare @SchemaName varchar(100)
declare @TableName varchar(256)
declare @IndexName varchar(256)
declare @ColumnName varchar(100)
declare @is_unique varchar(100)
declare @IndexTypeDesc varchar(100)
declare @FileGroupName varchar(100)
declare @is_disabled varchar(100)
declare @IndexOptions varchar(max)
declare @IndexColumnId int
declare @IsDescendingKey int 
declare @IsIncludedColumn int
declare @TSQLScripCreationIndex varchar(max)

create  table #ResultsTbl
(IndexScript varchar(max),
 SchemaName varchar(100), 
 TableName varchar(256), 
 IndexName varchar(256), 
 is_unique varchar(10),
 IndexTypeDesc varchar(100), 
 IndexOptions varchar(max),
 is_disabled bit, 
 FileGroupName varchar(100),
 IndexColumns varchar(max),
 IncludedColumns varchar(max))
 
declare CursorIndex cursor for
 select schema_name(t.schema_id) [schema_name], t.name, ix.name,
 case when ix.is_unique = 1 then 'UNIQUE ' else '' END 
 , ix.type_desc,
 case when ix.type in (5,6) then
	 case when ix.compression_delay=1 then 'COMPRESSION_DELAY = ON' else 'COMPRESSION_DELAY = OFF' end 
	--+ case when ix.on  then 'ALLOW_PAGE_LOCKS = ON, ' else 'ALLOW_PAGE_LOCKS = OFF, ' end
 else 
 case when ix.is_padded=1 then 'PAD_INDEX = ON, ' else 'PAD_INDEX = OFF, ' end
 + case when ix.allow_page_locks=1 then 'ALLOW_PAGE_LOCKS = ON, ' else 'ALLOW_PAGE_LOCKS = OFF, ' end
 + case when ix.allow_row_locks=1 then  'ALLOW_ROW_LOCKS = ON, ' else 'ALLOW_ROW_LOCKS = OFF, ' end
 + case when INDEXPROPERTY(t.object_id, ix.name, 'IsStatistics') = 1 then 'STATISTICS_NORECOMPUTE = ON, ' else 'STATISTICS_NORECOMPUTE = OFF, ' end
 + case when ix.ignore_dup_key=1 then 'IGNORE_DUP_KEY = ON, ' else 'IGNORE_DUP_KEY = OFF, ' end
 + 'SORT_IN_TEMPDB = OFF, FILLFACTOR =' + CAST(ix.fill_factor AS VARCHAR(3)) end  AS IndexOptions
 , ix.is_disabled , FILEGROUP_NAME(ix.data_space_id) FileGroupName
 from sys.tables t 
 inner join sys.indexes ix on t.object_id=ix.object_id
 where ix.type>0 and t.is_ms_shipped=0 and t.name<>'sysdiagrams'
 --and ix.is_primary_key=0 and ix.is_unique_constraint=0
 order by schema_name(t.schema_id), t.name, ix.name

open CursorIndex
fetch next from CursorIndex into  @SchemaName, @TableName, @IndexName, @is_unique, @IndexTypeDesc, @IndexOptions,@is_disabled, @FileGroupName

while (@@fetch_status=0)
begin
 declare @IndexColumns varchar(max)
 declare @IncludedColumns varchar(max)
 
 set @IndexColumns=''
 set @IncludedColumns=''
 
 declare CursorIndexColumn cursor for 
  select col.name, ixc.is_descending_key, ixc.is_included_column
  from sys.tables tb 
  inner join sys.indexes ix on tb.object_id=ix.object_id
  inner join sys.index_columns ixc on ix.object_id=ixc.object_id and ix.index_id= ixc.index_id
  inner join sys.columns col on ixc.object_id =col.object_id  and ixc.column_id=col.column_id
  where ix.type>0 and (ix.is_primary_key=0 or ix.is_unique_constraint=0)
  and schema_name(tb.schema_id)=@SchemaName and tb.name=@TableName and ix.name=@IndexName
  order by ixc.index_column_id
 
 open CursorIndexColumn 
 fetch next from CursorIndexColumn into  @ColumnName, @IsDescendingKey, @IsIncludedColumn
 
 while (@@fetch_status=0)
 begin
  if @IsIncludedColumn=0 
   set @IndexColumns=@IndexColumns + @ColumnName  + case when @IsDescendingKey=1  then ' DESC, ' else  ' ASC, ' end
  else 
   set @IncludedColumns=@IncludedColumns  + @ColumnName  +', ' 

  fetch next from CursorIndexColumn into @ColumnName, @IsDescendingKey, @IsIncludedColumn
 end

 close CursorIndexColumn
 deallocate CursorIndexColumn

 set @IndexColumns = case when len(@IndexColumns) >0 then substring(@IndexColumns, 1, len(@IndexColumns)-1) else '' end
 set @IncludedColumns = case when len(@IncludedColumns) >0 then substring(@IncludedColumns, 1, len(@IncludedColumns)-1) else '' end



 set @TSQLScripCreationIndex =''
 set @TSQLScripCreationIndex='CREATE '+ @is_unique  +@IndexTypeDesc + ' INDEX ' +QUOTENAME(@IndexName)+' ON ' + QUOTENAME(@SchemaName) +'.'+ QUOTENAME(@TableName)+ '('+@IndexColumns+') '+ 
  case when len(@IncludedColumns)>0 then CHAR(13) +'INCLUDE (' + @IncludedColumns+ ')' else '' end + CHAR(13)+'WITH (' + @IndexOptions+ ') ON ' + QUOTENAME(@FileGroupName) + ';'  

 if @is_disabled=1 
  set  @TSQLScripCreationIndex= 'ALTER INDEX ' +QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) +'.'+ QUOTENAME(@TableName) + ' DISABLE;' + CHAR(13) 
  
 if @IndexTypeDesc ='CLUSTERED COLUMNSTORE' 
 set @TSQLScripCreationIndex='CREATE '+ @is_unique  +@IndexTypeDesc + ' INDEX ' +QUOTENAME(@IndexName)+' ON ' + QUOTENAME(@SchemaName) +'.'+ QUOTENAME(@TableName)+ ' WITH (' + @IndexOptions+ ') ON ' + QUOTENAME(@FileGroupName) + ';'  

 Insert into #ResultsTbl (IndexScript, SchemaName, TableName, IndexName, is_unique, IndexTypeDesc, IndexOptions, is_disabled, FileGroupName, IndexColumns, IncludedColumns) 
				Values   (@TSQLScripCreationIndex, @SchemaName, @TableName, @IndexName, @is_unique, @IndexTypeDesc, @IndexOptions, @is_disabled, @FileGroupName, @IndexColumns, @IncludedColumns)

 fetch next from CursorIndex into  @SchemaName, @TableName, @IndexName, @is_unique, @IndexTypeDesc, @IndexOptions,@is_disabled, @FileGroupName

end
close CursorIndex
deallocate CursorIndex

select * from  #ResultsTbl 
Drop table #ResultsTbl"

 
$SqlConnection_Src = New-Object System.Data.SqlClient.SqlConnection
$SqlConnection_Src.ConnectionString = "Server = $Source_Server; Database = $Source_Database; Integrated Security = True"
 
 $SqlCmd_Src = New-Object System.Data.SqlClient.SqlCommand
 $SqlCmd_Src.CommandText = $SqlQuery
 $SqlCmd_Src.Connection = $SqlConnection_Src
 
 $SqlAdapter_Src = New-Object System.Data.SqlClient.SqlDataAdapter
 $SqlAdapter_Src.SelectCommand = $SqlCmd_Src
 $DataSet_Src = New-Object System.Data.DataSet
 
    $SqlAdapter_Src.Fill($DataSet_Src) >$null
    $SqlConnection_Src.Close()

$SqlConnection_Dest = New-Object System.Data.SqlClient.SqlConnection
$SqlConnection_Dest.ConnectionString = "Server = $Destination_Server; Database = $Destination_Database; Integrated Security = True"
 
 $SqlCmd_Dest = New-Object System.Data.SqlClient.SqlCommand
 $SqlCmd_Dest.CommandText = $SqlQuery
 $SqlCmd_Dest.Connection = $SqlConnection_Dest
 
 $SqlAdapter_Dest = New-Object System.Data.SqlClient.SqlDataAdapter
 $SqlAdapter_Dest.SelectCommand = $SqlCmd_Dest
 $DataSet_Dest = New-Object System.Data.DataSet
 

    $SqlAdapter_Dest.Fill($DataSet_Dest)  >$null
    $SqlConnection_Dest.Close()


 #fill resultset with indexes from source 
 foreach ($srcrow in $DataSet_Src.Tables[0].Rows)
 {
    $workRow =$dTable.NewRow();  

    $workRow[“Matching”]              = "-->"
    $workRow[“Source_Index”]          = $srcrow.IndexName 
    $workRow[“Source_ScriptFull”]     = $srcrow.IndexScript 
    $workRow[“Destination_Index”]     = ""
    $workRow[“Destination_ScriptFull”]= ""
    $workRow[“Source_Fields”]         = $srcrow.IndexColumns 
    $workRow[“Source_IncludedFields”] = $srcrow.IncludedColumns 
    
    $dTable.Rows.Add($workRow);  
 }


  foreach ($DestRow in $DataSet_Dest.Tables[0].Rows)
  {
    $Search_def = ("Source_ScriptFull = '{0}'" -f $DestRow.IndexScript)
    $DataRow = $dTable.Select($Search_def  );

  If ($DataRow.Count -eq 1) # let's try FULL match
    {

    $DataRow[0]["Matching"] = "==" 
    $DataRow[0]["Destination_Index"] =$DestRow.IndexName
    $DataRow[0]["Destination_ScriptFull"] =$DestRow.IndexScript
    }
  else
    {
        $Search_def2 = ("Source_Index = '{0}'" -f $DestRow.IndexName) # let's try NAME match
        $DataRow2 = $dTable.Select($Search_def2 );
        If ($DataRow2.Count -eq 1) 
            {
                $DataRow2[0]["Matching"] = "same NAME" 
                $DataRow2[0]["Destination_Index"] =$DestRow.IndexName
                $DataRow2[0]["Destination_ScriptFull"] =$DestRow.IndexScript
                $DataRow2[0][“Destination_Fields”]         = $DestRow.IndexColumns 
                $DataRow2[0][“Destination_IncludedFields”] = $DestRow.IncludedColumns
            }
        else
        {
            $Search_def3 = ("Source_Fields = '{0}'" -f $DestRow.IndexColumns) # let's try NAME match
            $DataRow3 = $dTable.Select($Search_def3 );
            If ($DataRow3.Count -eq 1) 
                {
                    $DataRow3[0]["Matching"] = "same FIELDS" 
                    $DataRow3[0]["Destination_Index"] =$DestRow.IndexName
                    $DataRow3[0]["Destination_ScriptFull"] =$DestRow.IndexScript
                    $DataRow3[0][“Destination_Fields”]         = $DestRow.IndexColumns 
                    $DataRow3[0][“Destination_IncludedFields”] = $DestRow.IncludedColumns
                }
            else
                {
                     $workRow =$dTable.NewRow();  #nomatch
     
                    $workRow[“Matching”]                   = "<--"
                    $workRow[“Source_Index”]               = "" 
                    $workRow[“Source_ScriptFull”]          = "" 
                    $workRow[“Destination_Index”]          = $DestRow.IndexName
                    $workRow[“Destination_ScriptFull”]     = $DestRow.IndexScript
                    $workRow[“Destination_Fields”]         = $DestRow.IndexColumns 
                    $workRow[“Destination_IncludedFields”] = $DestRow.IncludedColumns
      
                    $dTable.Rows.Add($workRow);

                }
        }
    }
  
  }
    
  If ($Unmatched_Only)
  {
    $viewDef = $dTable.DefaultView
    $viewDef.RowFilter = "Matching <>  '=='";
    $viewDef
  }
  else
  {
    $dTable
  }
  
}
<#
 .Synopsis
  Displays list of logins on SQL server NOT matching in all Availability Group nodes.

 .Description
  Displays list of logins on SQL server NOT matching in all Availability Group nodes. It could be used from any of nodes or listeners

 .Parameter Source_Server
  Name of source SQL server.
  It could be any of nodes (not just primary) or listeners 
  

 .Parameter Source_Database
  Name of source database. Filters results to just one database, otherwise all user databases are used. 
  
 .Parameter -$ReturnKPI 
  Will result in just returning Integer with number of different logins. Ideally it would return 0 as "everything matching"
  
 .Parameter -$ReturnALL 
  Will result in list of all logins in all nodes 


 .Example
   # Show list of all non-matching logins in all availability group nodes 
   Match_AG_Logins -Source_Server 'MyListener'

 .Example
   # Show list of all logins in all availability group nodes 
   Match_AG_Logins -Source_Server 'MyListener' -ReturnALL 

 .Example
   # Show number of how many different logins in all availability group nodes is found
   Match_AG_Logins -Source_Server 'MyListener' -ReturnKPI  


   

 .Example
    # Show list of all indexes in both databases in Matrix view reduced to only those non identical 
    Match_Indexes -Source_Server 'MyServer' -Source_Database 'AdventureWorksDW2012' -Destination_Server 'myServer' -Destination_Database 'AdventureWorksDW2012_Clone' -Unmatched_Only

#>
function Match_AG_Logins{
param(
    [String] $Source_Server, 
    [switch] $ReturnKPI,
    [switch] $ReturnALL
    )

$ReplicasQuery= 'Select Distinct Replica_Name from (
select AG.name as Group_Name,
Rep.replica_server_name as Replica_Name,
Sta.Role_desc,
Sta.Role,
Sta.is_local
  FROM master.sys.availability_groups AS AG
  inner join master.sys.availability_replicas AS rep on AG.group_id = Rep.group_id 
  --inner join sys.dm_hadr_availability_replica_states sta on Rep.replica_id = sta.replica_id and Rep.group_id = Sta.group_id) A order by Replica_Name
  left outer join sys.dm_hadr_availability_replica_states sta on Rep.replica_id = sta.replica_id and Rep.group_id = Sta.group_id) A order by Replica_Name'

$UsersQuery= "SELECT @@ServerName  as Server_Name, 
p.name,
master.dbo.fn_varbintohexstr(p.sid) as new_sid,
-- CAST( LOGINPROPERTY( p.name, 'PasswordHash' ) AS varbinary (256) ) as Password_Hash,
 master.dbo.fn_varbintohexstr( CAST( LOGINPROPERTY( p.name, 'PasswordHash' ) AS varbinary (256) ) ) as pwd_hash
--, p.type, p.is_disabled, p.default_database_name, l.hasaccess, l.denylogin 
FROM sys.server_principals P 
LEFT JOIN sys.syslogins L      ON ( L.name = P.name ) 
WHERE p.type IN ( 'S', 'G', 'U' ) 
AND p.name <> 'sa' 
AND p.name not like 'NT %'
AND p.name not like '##%'"
 
$SqlConn_Replicas = New-Object System.Data.SqlClient.SqlConnection
$SqlConn_Replicas.ConnectionString = "Server = $Source_Server; Database = 'Master'; Integrated Security = True"
 
 $SqlCmd_Rep = New-Object System.Data.SqlClient.SqlCommand
 $SqlCmd_Rep.CommandText = $ReplicasQuery
 $SqlCmd_Rep.Connection = $SqlConn_Replicas
 
 $SqlAdapter_Rep = New-Object System.Data.SqlClient.SqlDataAdapter
 $SqlAdapter_Rep.SelectCommand = $SqlCmd_Rep
 $DataSet_Replicas = New-Object System.Data.DataSet
 
 $SqlAdapter_Rep.Fill($DataSet_Replicas) >$null
 $SqlConn_Replicas.Close()

    $Result_Table = New-Object System.Data.DataTable
    $Coll_Replicas = {}.Invoke()

   foreach ($DataRow in $DataSet_Replicas.Tables[0].Rows)
    {
        $col1 = New-Object System.Data.DataColumn($DataRow["Replica_Name"])
        $Result_Table.columns.Add($col1)

        $Coll_Replicas.Add($DataRow["Replica_Name"])
    }
        $col2 = New-Object System.Data.DataColumn(“User_Name”)
            $Result_Table.columns.Add($col2)
        $col3 = New-Object System.Data.DataColumn(“User_SID”)
            $Result_Table.columns.Add($col3)
        $col4 = New-Object System.Data.DataColumn(“User_PasswordHash”)
            $Result_Table.columns.Add($col4)
        $col5 = New-Object System.Data.DataColumn(“Description”)
            $Result_Table.columns.Add($col5)

 

foreach ($DataRow in $DataSet_Replicas.Tables[0].Rows)
{

    $SqlConn_Usr = New-Object System.Data.SqlClient.SqlConnection
    $SqlConn_Usr.ConnectionString = "Server = {0}; Database = 'Master'; Integrated Security = True" -f  $DataRow["Replica_Name"] 
 
    $SqlCmd_Usr = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd_Usr.CommandText = $UsersQuery
    $SqlCmd_Usr.Connection = $SqlConn_Usr
 
    $SqlAdapter_Usr = New-Object System.Data.SqlClient.SqlDataAdapter
    $SqlAdapter_Usr.SelectCommand = $SqlCmd_Usr
    $DataSet_Usr = New-Object System.Data.DataSet
 
    $SqlAdapter_Usr.Fill($DataSet_Usr) >$null
    $SqlConn_Usr.Close()

    foreach($srcrow in $DataSet_Usr.Tables[0].Rows)
    { 
        If (!$srcrow.pwd_hash.ToString()) 
            {
            $Search_def = ("User_Name = '{0}' and User_Sid='{1}'" -f $srcrow.Name , $srcrow.New_Sid.ToString())}
        else
            {
            $Search_def = ("User_Name = '{0}' and User_Sid='{1}' and User_PasswordHash='{2}'" -f $srcrow.Name , $srcrow.New_Sid.ToString(), $srcrow.pwd_hash.ToString())}
        
        $DataRow = $Result_Table.Select($Search_def  );

    If ($DataRow.Count -eq 1) # let's try FULL match
    {
         $DataRow[0][$srcrow.Server_Name] = "X"
    }
    else
    {
        $workRow =$Result_Table.NewRow();  
            $workRow[$srcrow.Server_Name] = "X"
            $workRow[“User_Name”]         = $srcrow.Name 
            $workRow[“User_SID”]          = $srcrow.New_Sid.ToString()  
            $workRow[“User_PasswordHash”] = $srcrow.pwd_hash.ToString()
        $Result_Table.Rows.Add($workRow);
    }
  }
}
#format final output 
    $KPI=0

    foreach($finrow in  $Result_Table.Rows)
    {
 
            $positive_List='Found on'
            $negative_List='Missing on'

        foreach ($s in  $Coll_Replicas)
        {
            if ($finrow[$s] -eq "x") 
            {
            $positive_List=$positive_List +' '+ $s
            }
            else
            {
            $negative_List=$negative_List +' '+ $s
            }
        }

        if ($negative_List -eq 'Missing on')
        {
            $finrow["Description"]='OK'
        }
        else
        {
            $finrow["Description"]=  $positive_List + ' ' + $negative_List
            $KPI++
        }
    }

    if ($ReturnKPI )
    { $KPI  }
    else
    { if ($ReturnALL)
        {$Result_Table}
        else
        {$Result_Table.Select("Description <> 'OK'") }
    }   
}
<#
 .Synopsis
  Displays list of bad passwords from SQL server(s)

 .Description
  Displays list of bad passwords from SQL server(s). It uses list provided by parameter, but it is also checking for same password as login

 .Parameter ServerName
  Mandatory,  Name of source SQL server.

 .Parameter ServerList
  Mandatory,  text file (with path) containing list of servers. 
  
  .Parameter Timespan
  Not Mandatory,  number of hours of  
  
 .Parameter -$JustKPI 
  Will result in just returning Integer with number of issues. Ideally it would return 0 as "everything matching"
  
 .Example
   # Show list of bad password logins on single server 
   Get-BadPasswords -ServerName MyServer  -PasswordProbeList 'C:\Lists\Passwordlist.txt' 

 .Example
   # Show list of bad password logins on multiple servers 
   Get-BadPasswords -ServerList 'C:\Lists\Serverlist.txt'  -PasswordProbeList 'C:\Lists\Passwordlist.txt'  

 .Example
   # Show just number of bad password logins on single server 
   Get-BadPasswords -ServerName MyServer  -PasswordProbeList 'C:\Lists\Passwordlist.txt' -JustKPI
#>
function Get-BadPasswords
{
    [CmdletBinding(DefaultParameterSetName='SingleServer')]
    param (
        [Parameter(Mandatory = $true, ParameterSetName = 'SingleServer')]
        [System.String]
        $ServerName,

        [Parameter(Mandatory = $true, ParameterSetName = 'MultipleServer')]
        [System.IO.FileInfo]
        $ServerList,

        
        [Parameter(Mandatory = $true)]
        [System.IO.FileInfo]
        $PasswordProbeList,

        [Parameter(Mandatory = $false)]
        [switch]
        $JustKPI
    )

$Qry="Declare @Tested_Value varchar(255)
set @Tested_Value = 'xyz'

select  serverproperty('machinename')                                        as 'Server Name',
       isnull(serverproperty('instancename'),serverproperty('machinename')) as 'Instance Name',  
       GetDate() as ExecutedTime,
       name   as 'Login With issue',
	   case when @Tested_Value = '' THEN 'Login same as pasword' ELSE @Tested_Value end as TestedValue
        from    master.sys.sql_logins
        where  1 = 
		CASE WHEN @Tested_Value = '' THEN 
			 pwdcompare(name,password_hash) 
			ELSE
			pwdcompare(@Tested_Value,password_hash) 
			END
        order by name
        option (maxdop 1)"

$issuesFound=0
#$PwdList=get-content -Path $PasswordProbeList
if (!(Test-Path $PasswordProbeList) ) {
  Write-Warning "$PasswordProbeList is not correct path !!"
  }
  else
  {$PwdList=get-content -Path $PasswordProbeList}


    if (!$ServerList)
    {
        $srvrs=$ServerName
    }
    else
        {$srvrs=get-content -Path $ServerList
    }


foreach ($srvr in $srvrs )
{
    foreach ($pwdtest in $PwdList )
    {
        try
        {
            $QryNew = $Qry -replace 'xyz', $pwdtest
            $resultTbl=@(Invoke-Sqlcmd -Query  $QryNew -ServerInstance $srvr -ErrorAction Continue)
            if (!$JustKPI) {$resultTbl}
            $issuesFound= $issuesFound  + $resultTbl.Count
        }
        Catch 
        {
            Write-Host   $Error[0] -ForegroundColor Red
        }
    }

    $QryNew = $Qry -replace 'xyz', ''
    $resultTbl=@(Invoke-Sqlcmd -Query  $QryNew -ServerInstance $srvr -ErrorAction Continue)
    if (!$JustKPI) {$resultTbl}
    $issuesFound= $issuesFound  + $resultTbl.Count

       #Write-Host   $Qry.Name 'executed on ' $srvr -ForegroundColor Green

}

if ($JustKPI)
{ 
 Write-Host   $Qry.Name 'Total number of issues found ' $issuesFound -ForegroundColor Red}
 }

<#
 .Synopsis
  Displays Trace data from SQL server(s)

 .Description
  Displays Trace data from SQL server(s). can be used for single or multiple servers 

 .Parameter ServerName
  Mandatory,  Name of source SQL server.

 .Parameter ServerList
  Mandatory,  text file (with path) containing list of servers. 
  
  .Parameter PasswordProbeList
  Mandatory,  text file (with path) containing list of password to test against. 
  
 .Parameter -$JustKPI 
  Will result in just returning Integer with number of issues. Ideally it would return 0 as "everything matching"
  
 .Example
   # Show list of bad password logins on single server 
   Get-BadPasswords -ServerName MyServer  -PasswordProbeList 'C:\Lists\Passwordlist.txt' 

 .Example
   # Show list of bad password logins on multiple servers 
   Get-BadPasswords -ServerList 'C:\Lists\Serverlist.txt'  -PasswordProbeList 'C:\Lists\Passwordlist.txt'  

 .Example
   # Show just number of bad password logins on single server 
   Get-BadPasswords -ServerName MyServer  -PasswordProbeList 'C:\Lists\Passwordlist.txt' -JustKPI
#>

function Get-DefaultTrace
{
    [CmdletBinding(DefaultParameterSetName='SingleServer')]
    param (
        [Parameter(Mandatory = $true, ParameterSetName = 'SingleServer')]
        [System.String]
        $ServerName,

        [Parameter(Mandatory = $true, ParameterSetName = 'MultipleServer')]
        [System.IO.FileInfo]
        $ServerList,

        [Parameter(Mandatory = $false)]
        [int]
        $TimeSpan )


    if (!$ServerList)
    {
        $srvrs=$ServerName}
    else
    { 
        $srvrs=get-content -Path $ServerList
    }

    if (!$TimeSpan)
        {
           $TimeSpan=1 
        }

$Qry="SELECT @@ServerName as Server_Name,
		GetDate() as CurrentTime,
        t.StartTime as [Event_StartTime],
        TE.name AS [Event_Name] ,
        v.subclass_name ,
        T.DatabaseName ,
        t.DatabaseID ,
        t.NTDomainName ,
        t.ApplicationName ,
        t.LoginName ,
        t.SPID ,
        t.RoleName ,
        t.TargetUserName ,
        t.TargetLoginName ,
        t.SessionLoginName
FROM    sys.fn_trace_gettable(CONVERT(VARCHAR(150), ( SELECT TOP 1
                                                              f.[value]
                                                      FROM    sys.fn_trace_getinfo(NULL) f
                                                      WHERE   f.property = 2
                                                    )), DEFAULT) T
        JOIN sys.trace_events TE ON T.EventClass = TE.trace_event_id
        JOIN sys.trace_subclass_values v ON v.trace_event_id = TE.trace_event_id
                                            AND v.subclass_value = t.EventSubClass
Where t.StartTime > DATEADD(HOUR, -xyz, GETDATE())
--AND te.name IN ( 'Audit Addlogin Event', 'Audit Add DB User Event','Audit Add Member to DB Role Event' )
--AND v.subclass_name IN ( 'add', 'Grant database access' )"


$QryNew = $Qry -replace 'xyz', $TimeSpan

foreach ($srvr in $srvrs )
{
    try
    {
        Invoke-Sqlcmd -Query  $QryNew -ServerInstance $srvr -ErrorAction Continue 
        #$resultTbl=@(Invoke-Sqlcmd -Query  $QryNew -ServerInstance $srvr -ErrorAction Continue)
        #$resultTbl
        #Write-Host   $Qry.Name 'executed on ' $srvr -ForegroundColor Green
    }
    Catch 
    {
    Write-Host   $Error[0] -ForegroundColor Red
    }
}

}






Export-ModuleMember -Function Get-MissingIndexes     #v1.0
Export-ModuleMember -Function Get-unusedIndexes      #v1.0
Export-ModuleMember -Function Get-ExpensiveQueries   #v1.1
Export-ModuleMember -Function Get-Warnings           #v1.1
Export-ModuleMember -Function Match_Indexes          #v2.0 
Export-ModuleMember -Function Match_AG_Logins        #v2.0  
Export-ModuleMember -Function Get-BadPasswords       #v2.0
Export-ModuleMember -Function Get-DefaultTrace       #v2.0

#Get-MissingIndexes -ServerName 'petar_t' -databasename 'msdb' -GetScript 
#Get-unusedIndexes -ServerName 'petar_t' -GetScript  | Format-Table
#Get-ExpensiveQueries -ServerName 'petar_t' -Top 10 -DoNotIncludePlan | Format-Table
#Get-Warnings -ServerName 'petar_t' -HoursLast 48 -showDetail
#Match_Indexes -Source_Server 'Petar_T' -Source_Database 'AdventureWorksDW2012' -Destination_Server 'Petar_T' -Destination_Database 'AdventureWorksDW2012_Clone' | Out-GridView
#Match_Indexes -Source_Server 'Petar_T' -Source_Database 'AdventureWorksDW2012' -Destination_Server 'Petar_T' -Destination_Database 'AdventureWorksDW2012_Clone' -Unmatched_Only | Out-GridView
#Match_AG_Logins -Source_Server "AlwaysONN3"   | Format-Table
#Match_AG_Logins -Source_Server "Listener1" | ogv