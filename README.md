# CSharp Connector

* This C# connector supports: Linux 64/Windows x64/Windows x86.
* This C# connector can be downloaded and included as a normal package from [Nuget.org](https://www.nuget.org/packages/TDengine.Connector/).
* From V3.0.2, this C# connector support WebSocket.But need additional some configurations in your project file.

## Installation preparations

* Install TDengine client.
* .NET interface file TDengineDriver.cs and reference samples both
  are located under Windows client's installation path:install_directory/examples/C#.
* Install [.NET SDK](https://dotnet.microsoft.com/download)

## Installation

For native connection:

```cmd
dotnet add package TDengine.Connector
```

For WebSocket, Need add the following ItemGroup in your project file:

``` XML
// .csproj
<ItemGroup>
    <PackageReference Include="TDengine.Connector" Version="3.0.*" GeneratePathProperty="true" />
  </ItemGroup>
  <Target Name="copyDLLDepency" BeforeTargets="BeforeBuild">
    <ItemGroup>
      <DepDLLFiles Include="$(PkgTDengine_Connector_test)\runtimes\**\*.*" />
    </ItemGroup>
    <Copy SourceFiles="@(DepDLLFiles)" DestinationFolder="$(OutDir)\dep_lib" />
  </Target>

```

**Tips:**
`TDengine.Connector` support WebSocket from V3.0.2. And for v1.x doesn't support WebSocket.

## Example Source Code

You can find examples under follow directories:

* {client_installation_directory}/examples/C#
* [TDengine example source code](https://github.com/taosdata/TDengine/tree/main/docs/examples/csharp)
* [Current repo example source code](https://github.com/taosdata/taos-connector-dotnet/tree/3.0/examples)

## Use C# connector

### **prepare**

**tips:** Need to install .NET SDK first.

* Create a dotnet project(using console project as an example).

``` cmd
mkdir test
cd test
dotnet new console
```

* Add "TDengine.Connector" as a package through Nuget into project.

``` cmd
dotnet add package TDengine.Connector
```

### **Connection**

``` C#
using TDengineDriver;
using System.Runtime.InteropServices;
// ... do something ...
string host = "127.0.0.1" ; 
string configDir =  "C:/TDengine/cfg"; // For linux should it be /etc/taos.
string user = "root";
string password = "taosdata";
string db = ''; // Also can set it to the db name you want to connect.
string port = 0

/* Set client options (optional step):charset, locale, timezone.
 * Default: charset, locale, timezone same to system.
 * Current supports options:TSDB_OPTION_LOCALE, TSDB_OPTION_CHARSET, TSDB_OPTION_TIMEZONE, TSDB_OPTION_CONFIGDIR.
*/
TDengine.Options((int)TDengineInitOption.TSDB_OPTION_CONFIGDIR,configDir);

// Get an TDengine connection
InPtr conn = TDengine.Connect(host, user, taosdata, db, port);

// Check if get connection success
if (conn == IntPtr.Zero)
{
   Console.WriteLine("Connect to TDengine failed");
}
else
{
   Console.WriteLine("Connect to TDengine success");
}

// Close TDengine Connection
if (conn != IntPtr.Zero)
{
    TDengine.Close(this.conn);
}

// Suggest to clean environment, before exit your application.
TDengine.Cleanup();
```

### **Execute SQL**

```C#
// Suppose conn is a valid tdengine connection from previous Connection sample
public static void ExecuteSQL(IntPtr conn, string sql)
{
    IntPtr res = TDengine.Query(conn, sql);
    // Check if query success
    if((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
    {
        Console.Write(sql + " failure, ");
        // Get error message while Res is a not null pointer.
        if (res != IntPtr.Zero)
         {
             Console.Write("reason:" + TDengine.Error(res));
         }
    }
    else
    {
        Console.Write(sql + " success, {0} rows affected", TDengine.AffectRows(res));
        //... do something with res ...

        // Important: need to free result to avoid memory leak.
        TDengine.FreeResult(res);
    }
}

// Calling method to execute sql;
ExecuteSQL(conn,$"create database if not exists {db};");
ExecuteSQL(conn,$"use {db};");
string createSql = "CREATE TABLE meters(ts TIMESTAMP, current FLOAT,"+
" voltage INT, phase FLOAT)TAGS(location BINARY(30), groupId INT);"
ExecuteSQL(conn,createSql);
ExecuteSQL(conn," INSERT INTO d1001 USING meters TAGS('Beijing.Chaoyang', 2) VALUES('a');");
ExecuteSQL(conn,$"drop database if exists {db};");
```

### **Get Query Result**

```C#

using System;
using System.Collections.Generic;
using TDengineDriver.Impl;

// Following code is a sample that traverses retrieve data from TDengine.
public void ExecuteQuery(IntPtr conn,string sql)
{
    // "conn" is a valid TDengine connection which can
    // be got from previous "Connection" sample.
    IntPrt res = TDengine.Query(conn, sql);
    if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
    {
         Console.Write(sql.ToString() + " failure, ");
         if (res != IntPtr.Zero)
         {
             Console.Write("reason: " + TDengine.Error(res));
         }
         // Execute query sql failed
         // ... do something ...
    }

    List<TDengineDriver.TDengineMeta> resMeta = LibTaos.GetMeta(res);
    List<Object> resData = LibTaos.GetData(res);

    foreach (var meta in resMeta)
    {
        Console.Write($"\t|{meta.name} {meta.TypeName()} ({meta.size})\t|");
    }
    Console.WriteLine("")
    for (int i = 0; i < resData.Count; i++)
    {
        Console.Write($"|{resData[i].ToString()} \t");
        //Console.WriteLine("{0},{1},{2}", i, resMeta.Count, (i) % resMeta.Count);
        if (((i + 1) % resMeta.Count == 0))
        {
            Console.WriteLine("");
        }
    }
    Console.WriteLine("")
    // Important free "res".
     TDengine.FreeResult(res);
}
```

### **Stmt Bind Sample**

* Bind different types of data.

```C#
// Prepare tags values used to binding by stmt.
// An instance of TAOS_BIND can just bind a cell of table.
TAOS_BIND[] binds = new TAOS_BIND[1];
binds[0] = TaosBind.BindNchar("-123acvnchar");
// Use TaosBind.BindNil() to bind null values.

long[] tsArr = new long[5] { 1637064040000, 1637064041000,
1637064042000, 1637064043000, 1637064044000 };
bool?[] boolArr = new bool?[5] { true, false, null, true, true };
int?[] intArr = new int?[5] { -200, -100, null, 0, 300 };
long?[] longArr = new long?[5] { long.MinValue + 1, -2000, null,
1000, long.MaxValue };
string[] binaryArr = new string[5] { "/TDengine/src/client/src/tscPrepare.c",
 String.Empty, null, "doBindBatchParam",
 "string.Join:1234567890123456789012345" };

// TAOS_MULTI_BIND can bind a column of data.
TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[5];

mBinds[0] = TaosMultiBind.MultiBindTimestamp(tsArr);
mBinds[1] = TaosMultiBind.MultiBindBool(boolArr);
mBinds[4] = TaosMultiBind.MultiBindInt(intArr);
mBinds[5] = TaosMultiBind.MultiBindBigint(longArr);
mBinds[12] = TaosMultiBind.MultiBindBinary(binaryArr);

// After using instance of TAOS_MULTI_BIND and TAOS_BIND,
// need to free the allocated unmanaged memory.
TaosMultiBind.FreeBind(mBind);
TaosMultiBind.FreeMBind(mBinds);
```

* Insert

```C#
  /* Pre-request: create stable or normal table.
   * Target table for this sample：stmtdemo
   * Structure：create stable stmtdemo (ts timestamp,b bool,v4 int,
   * v8 bigint,bin binary(100))tags(blob nchar(100));
  */
  // This conn should be a valid connection that is returned by TDengine.Connect().
  IntPtr conn;
  IntPtr stmt = IntPtr.Zero;
  // Insert statement
  string sql = "insert into ? using stmtdemo tags(?,?,?,?,?) values(?)";
  // "use db" before stmtPrepare().

  stmt = TDengine.StmtInit(conn);
  TDengine.StmtPrepare(stmt, sql);

  // Use method StmtSetTbname() to config tablename,
  // but needs to create the table before.
  // Using StmtSetTbnameTags() to config table name and
  // tags' value.(create sub table based on stable automatically)
  TDengine.StmtSetTbname_tags(stmt,"t1",binds);

  // Binding multiple lines of data.
  TDengine.StmtBindParamBatch(stmt,mBinds);

  // Add current bind parameters into batch.
  TDengine.StmtAddBatch(stmt);

  // Execute the batch instruction which has been prepared well by bind_param() method.
  TDengine.StmtExecute(stmt);

  // Cause we use unmanaged memory, remember to free occupied memory, after execution.
  TaosMultiBind.FreeBind(mBind);
  TaosMultiBind.FreeMBind(mBinds);

  // Get error information if current stmt operation failed.
  // This method is appropriate for all the stmt methods to get error message.
  TDengine.StmtError(stmt);
```

* Query

``` C#
stmt = StmtInit(conn);

string querySql = "SELECT * FROM T1 WHERE V4 > ? AND V8 < ?";
StmtPrepare(stmt, querySql);

// Prepare Query parameters.
TAOS_BIND qparams[2];
qparams[0] = TaosBind.bindInt(-2);
qparams[1] = TaosBind.bindLong(4);

// Bind parameters.
TDengine.StmtBindParam(stmt, qparams);

// Execute
TDengine.StmtExecute(stmt);

// Get querying result, for SELECT only.
// User application should be freed with API FreeResult() at the end.
IntPtr result = TDengine.StmtUseResult(stmt);

// This "result" cam be traversed as normal sql query result.
// ... Do something with "result" ...

TDengine.FreeResult(result);

// Cause we use unmanaged memory, we need to free occupied memory after execution.
TaosMultiBind.FreeBind(qparams);

// Close stmt and release resource.
TDengine.StmtClose(stmt);
```

* Assert (samples about how to assert every step of stmt is succeed or failed)

```C#
// Special  StmtInit().
IntPtr stmt = TDengine.StmtInit(conn);
if ( stmt == IntPtr.Zero)
{
       Console.WriteLine("Init stmt failed:{0}",TDengine.StmtErrorStr(stmt));
       // ... do something ...
}
else
{
      Console.WriteLine("Init stmt success");
      // Continue
}

// For all stmt methods that return int type,we can get error message by StmtErrorStr().
if (TDengine.StmtPrepare(this.stmt, sql) == 0)
{
    Console.WriteLine("stmt prepare success");
    // Continue
}
else
{
     Console.WriteLine("stmt prepare failed:{0} " , TDengine.StmtErrorStr(stmt));
     // ... do something ...
}

// Estimate weather StmtUseResult() is successful or failed.
// If failed, get the error message by TDengine.Error(res)
IntPtr res = TDengine.StmtUseResult(stmt);
if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
{
      Console.Write( " StmtUseResult failure, ");
      if (res != IntPtr.Zero) {
        Console.Write("reason: " + TDengine.Error(res));
       }
}
else
{
 Console.WriteLine(sql.ToString() + " success");
}
```

### Websocket Example

``` XML
// modify your project file (.csproj), copy dynamic library from the nupkg into your project directory.
<ItemGroup>
    <PackageReference Include="TDengine.Connector" Version="3.0.*" GeneratePathProperty="true" />
</ItemGroup>
  <Target Name="copyDLLDepency" BeforeTargets="BeforeBuild">
    <ItemGroup>
      <DepDLLFiles Include="$(PkgTDengine_Connector_test)\runtimes\**\*.*" />
    </ItemGroup>
    <Copy SourceFiles="@(DepDLLFiles)" DestinationFolder="$(OutDir)\dep_lib" />
  </Target>
```

* [WebSocket basic use](https://github.com/taosdata/taos-connector-dotnet/blob/3.0/examples/NET6Examples/WS/WebSocketSample.cs).
* [WebSocket STMT](https://github.com/taosdata/taos-connector-dotnet/blob/3.0/examples/NET6Examples/WS/WebSocketSTMT.cs).
* More samples reference from [examples](https://github.com/taosdata/taos-connector-dotnet/tree/3.0/examples/NET6Examples/WS).

**Note：**

* For WebSocket need copy dynamic library from the nupkg into your project directory.
  
* Since this. NET connector interface requires the taos.dll file, so before
  executing the application, copy the taos.dll file in the
  Windows {client_install_directory}/driver directory to the folder where the
  .NET project finally generated the .exe executable file. After running the exe
  file, you can access the TDengine database and do operations such as insert
  and query(This step can be skip if the client has been installed on you machine).
