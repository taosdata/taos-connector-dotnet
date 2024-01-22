using System;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;
using Xunit;
using Xunit.Abstractions;

namespace Driver.Test.Client.WS
{
    public class Client
    {
        private readonly ITestOutputHelper _output;

        public Client(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public void TDengineWSDriverTest()
        {
            var db = "driver_test_ws";
            Random rand = new Random();
            bool v1 = true;
            sbyte v2 = (sbyte)rand.Next(sbyte.MinValue, sbyte.MaxValue);
            short v3 = (short)rand.Next(short.MinValue, short.MaxValue);
            int v4 = rand.Next();
            long v5 = rand.Next();
            byte v6 = (byte)rand.Next(byte.MinValue, byte.MaxValue);
            ushort v7 = (ushort)rand.Next(ushort.MinValue, ushort.MaxValue);
            uint v8 = (uint)rand.Next();
            ulong v9 = (ulong)rand.Next();
            float v10 = (float)rand.NextDouble();
            double v11 = rand.NextDouble();

            DateTime dateTime = DateTime.Now;
            DateTime now = new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute,
                dateTime.Second, dateTime.Millisecond, dateTime.Kind);
            DateTime nextSecond = now.Add(TimeSpan.FromSeconds(1));

            var builder =
                new ConnectionStringBuilder(
                    "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata;enableCompression=true");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    client.Exec($"drop database if exists {db}");
                    client.Exec($"create database {db}");
                    client.Exec($"use {db}");
                    client.Exec("create table if not exists all_type(ts timestamp," +
                                "c1 bool," +
                                "c2 tinyint," +
                                "c3 smallint," +
                                "c4 int," +
                                "c5 bigint," +
                                "c6 tinyint unsigned," +
                                "c7 smallint unsigned," +
                                "c8 int unsigned," +
                                "c9 bigint unsigned," +
                                "c10 float," +
                                "c11 double," +
                                "c12 binary(20)," +
                                "c13 nchar(20)" +
                                ")" +
                                "tags(t json)");
                    string insertQuery = string.Format(
                        "insert into t1 using all_type tags('{{\"a\":\"b\"}}') values('{0}',{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},'test_binary','test_nchar')('{12}',null,null,null,null,null,null,null,null,null,null,null,null,null)",
                        now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK"),
                        v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11,
                        nextSecond.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK"));
                    client.Exec(insertQuery);
                    string query = "select * from all_type order by ts asc";
                    var rows = client.Query(query);
                    var haveNext = rows.Read();
                    Assert.True(haveNext);
                    Assert.Equal(now, rows.GetValue(0));
                    Assert.Equal(v1, (bool)rows.GetValue(1));
                    Assert.Equal(v2, (sbyte)rows.GetValue(2));
                    Assert.Equal(v3, (short)rows.GetValue(3));
                    Assert.Equal(v4, (int)rows.GetValue(4));
                    Assert.Equal(v5, (long)rows.GetValue(5));
                    Assert.Equal(v6, (byte)rows.GetValue(6));
                    Assert.Equal(v7, (ushort)rows.GetValue(7));
                    Assert.Equal(v8, (uint)rows.GetValue(8));
                    Assert.Equal(v9, (ulong)rows.GetValue(9));
                    Assert.Equal(v10, (float)rows.GetValue(10));
                    Assert.Equal(v11, (double)rows.GetValue(11));
                    Assert.Equal(Encoding.UTF8.GetBytes("test_binary"), rows.GetValue(12));
                    Assert.Equal("test_nchar", rows.GetValue(13));
                    Assert.Equal(Encoding.UTF8.GetBytes("{\"a\":\"b\"}"), rows.GetValue(14));
                    haveNext = rows.Read();
                    Assert.True(haveNext);
                    Assert.Equal(nextSecond, rows.GetValue(0));
                    for (int i = 1; i < 13; i++)
                    {
                        Assert.Null(rows.GetValue(i));
                    }

                    Assert.Equal(Encoding.UTF8.GetBytes("{\"a\":\"b\"}"), rows.GetValue(14));
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop database if exists {db}");
                }
            }
        }

        [Fact]
        public void TDengineWSDriverNanoTest()
        {
            var db = "driver_test_ns_ws";
            Random rand = new Random();
            bool v1 = true;
            sbyte v2 = (sbyte)rand.Next(sbyte.MinValue, sbyte.MaxValue);
            short v3 = (short)rand.Next(short.MinValue, short.MaxValue);
            int v4 = rand.Next();
            long v5 = rand.Next();
            byte v6 = (byte)rand.Next(byte.MinValue, byte.MaxValue);
            ushort v7 = (ushort)rand.Next(ushort.MinValue, ushort.MaxValue);
            uint v8 = (uint)rand.Next();
            ulong v9 = (ulong)rand.Next();
            float v10 = (float)rand.NextDouble();
            double v11 = rand.NextDouble();

            DateTime dateTime = DateTime.Now;
            var ts = (dateTime.ToUniversalTime().Ticks - TDengineConstant.TimeZero.Ticks) * 100;
            var nextSecond = (dateTime.Add(TimeSpan.FromSeconds(1)).ToUniversalTime().Ticks -
                              TDengineConstant.TimeZero.Ticks) * 100;

            var builder =
                new ConnectionStringBuilder(
                    "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata;enableCompression=true");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    client.Exec($"drop database if exists {db}");
                    client.Exec($"create database {db} precision 'ns'");
                    client.Exec($"use {db}");
                    client.Exec("create table if not exists all_type(ts timestamp," +
                                "c1 bool," +
                                "c2 tinyint," +
                                "c3 smallint," +
                                "c4 int," +
                                "c5 bigint," +
                                "c6 tinyint unsigned," +
                                "c7 smallint unsigned," +
                                "c8 int unsigned," +
                                "c9 bigint unsigned," +
                                "c10 float," +
                                "c11 double," +
                                "c12 binary(20)," +
                                "c13 nchar(20)" +
                                ")" +
                                "tags(t json)");
                    string insertQuery = string.Format(
                        "insert into t1 using all_type tags('{{\"a\":\"b\"}}') values({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},'test_binary','test_nchar')({12},null,null,null,null,null,null,null,null,null,null,null,null,null)",
                        ts,
                        v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11,
                        nextSecond);
                    client.Exec(insertQuery);
                    string query = "select * from all_type order by ts asc";
                    var rows = client.Query(query);
                    var haveNext = rows.Read();
                    Assert.True(haveNext);
                    Assert.Equal(
                        TDengineConstant.ConvertTimeToDatetime(ts, TDenginePrecision.TSDB_TIME_PRECISION_NANO),
                        rows.GetValue(0));
                    Assert.Equal(v1, (bool)rows.GetValue(1));
                    Assert.Equal(v2, (sbyte)rows.GetValue(2));
                    Assert.Equal(v3, (short)rows.GetValue(3));
                    Assert.Equal(v4, (int)rows.GetValue(4));
                    Assert.Equal(v5, (long)rows.GetValue(5));
                    Assert.Equal(v6, (byte)rows.GetValue(6));
                    Assert.Equal(v7, (ushort)rows.GetValue(7));
                    Assert.Equal(v8, (uint)rows.GetValue(8));
                    Assert.Equal(v9, (ulong)rows.GetValue(9));
                    Assert.Equal(v10, (float)rows.GetValue(10));
                    Assert.Equal(v11, (double)rows.GetValue(11));
                    Assert.Equal(Encoding.UTF8.GetBytes("test_binary"), rows.GetValue(12));
                    Assert.Equal("test_nchar", rows.GetValue(13));
                    Assert.Equal(Encoding.UTF8.GetBytes("{\"a\":\"b\"}"), rows.GetValue(14));
                    haveNext = rows.Read();
                    Assert.True(haveNext);
                    Assert.Equal(
                        TDengineConstant.ConvertTimeToDatetime(nextSecond,
                            TDenginePrecision.TSDB_TIME_PRECISION_NANO), rows.GetValue(0));
                    for (int i = 1; i < 13; i++)
                    {
                        Assert.Null(rows.GetValue(i));
                    }

                    Assert.Equal(Encoding.UTF8.GetBytes("{\"a\":\"b\"}"), rows.GetValue(14));
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop database if exists {db}");
                }
            }
        }

        [Fact]
        public void TDengineWSDriverNanoReqTest()
        {
            var db = "driver_test_ns_req_ws";
            Random rand = new Random();
            bool v1 = true;
            sbyte v2 = (sbyte)rand.Next(sbyte.MinValue, sbyte.MaxValue);
            short v3 = (short)rand.Next(short.MinValue, short.MaxValue);
            int v4 = rand.Next();
            long v5 = rand.Next();
            byte v6 = (byte)rand.Next(byte.MinValue, byte.MaxValue);
            ushort v7 = (ushort)rand.Next(ushort.MinValue, ushort.MaxValue);
            uint v8 = (uint)rand.Next();
            ulong v9 = (ulong)rand.Next();
            float v10 = (float)rand.NextDouble();
            double v11 = rand.NextDouble();

            DateTime dateTime = DateTime.Now;
            var ts = (dateTime.ToUniversalTime().Ticks - TDengineConstant.TimeZero.Ticks) * 100;
            var nextSecond = (dateTime.Add(TimeSpan.FromSeconds(1)).ToUniversalTime().Ticks -
                              TDengineConstant.TimeZero.Ticks) * 100;

            var builder =
                new ConnectionStringBuilder(
                    "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata;enableCompression=true");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    client.Exec($"drop database if exists {db}", ReqId.GetReqId());
                    client.Exec($"create database {db} precision 'ns'");
                    client.Exec($"use {db}");
                    client.Exec("create table if not exists all_type(ts timestamp," +
                                "c1 bool," +
                                "c2 tinyint," +
                                "c3 smallint," +
                                "c4 int," +
                                "c5 bigint," +
                                "c6 tinyint unsigned," +
                                "c7 smallint unsigned," +
                                "c8 int unsigned," +
                                "c9 bigint unsigned," +
                                "c10 float," +
                                "c11 double," +
                                "c12 binary(20)," +
                                "c13 nchar(20)" +
                                ")" +
                                "tags(t json)");
                    string insertQuery = string.Format(
                        "insert into t1 using all_type tags('{{\"a\":\"b\"}}') values({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},'test_binary','test_nchar')({12},null,null,null,null,null,null,null,null,null,null,null,null,null)",
                        ts,
                        v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11,
                        nextSecond);
                    client.Exec(insertQuery);
                    string query = "select * from all_type order by ts asc";
                    var rows = client.Query(query, ReqId.GetReqId());
                    var haveNext = rows.Read();
                    Assert.True(haveNext);
                    Assert.Equal(
                        TDengineConstant.ConvertTimeToDatetime(ts, TDenginePrecision.TSDB_TIME_PRECISION_NANO),
                        rows.GetValue(0));
                    Assert.Equal(v1, (bool)rows.GetValue(1));
                    Assert.Equal(v2, (sbyte)rows.GetValue(2));
                    Assert.Equal(v3, (short)rows.GetValue(3));
                    Assert.Equal(v4, (int)rows.GetValue(4));
                    Assert.Equal(v5, (long)rows.GetValue(5));
                    Assert.Equal(v6, (byte)rows.GetValue(6));
                    Assert.Equal(v7, (ushort)rows.GetValue(7));
                    Assert.Equal(v8, (uint)rows.GetValue(8));
                    Assert.Equal(v9, (ulong)rows.GetValue(9));
                    Assert.Equal(v10, (float)rows.GetValue(10));
                    Assert.Equal(v11, (double)rows.GetValue(11));
                    Assert.Equal(Encoding.UTF8.GetBytes("test_binary"), rows.GetValue(12));
                    Assert.Equal("test_nchar", rows.GetValue(13));
                    Assert.Equal(Encoding.UTF8.GetBytes("{\"a\":\"b\"}"), rows.GetValue(14));
                    haveNext = rows.Read();
                    Assert.True(haveNext);
                    Assert.Equal(
                        TDengineConstant.ConvertTimeToDatetime(nextSecond,
                            TDenginePrecision.TSDB_TIME_PRECISION_NANO), rows.GetValue(0));
                    for (int i = 1; i < 13; i++)
                    {
                        Assert.Null(rows.GetValue(i));
                    }

                    Assert.Equal(Encoding.UTF8.GetBytes("{\"a\":\"b\"}"), rows.GetValue(14));
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop database if exists {db}");
                }
            }
        }

        [Fact]
        public void TDengineWSStmtTest()
        {
            var db = "stmt_test_ws";
            Random rand = new Random();
            bool v1 = true;
            sbyte v2 = (sbyte)rand.Next(sbyte.MinValue, sbyte.MaxValue);
            short v3 = (short)rand.Next(short.MinValue, short.MaxValue);
            int v4 = rand.Next();
            long v5 = rand.Next();
            byte v6 = (byte)rand.Next(byte.MinValue, byte.MaxValue);
            ushort v7 = (ushort)rand.Next(ushort.MinValue, ushort.MaxValue);
            uint v8 = (uint)rand.Next();
            ulong v9 = (ulong)rand.Next();
            float v10 = (float)rand.NextDouble();
            double v11 = rand.NextDouble();

            DateTime dateTime = DateTime.Now;
            DateTime now = new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute,
                dateTime.Second, dateTime.Millisecond, dateTime.Kind);
            DateTime nextSecond = now.Add(TimeSpan.FromSeconds(1));


            var builder =
                new ConnectionStringBuilder(
                    "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata;enableCompression=true");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    client.Exec($"drop database if exists {db}");
                    client.Exec($"create database {db}");
                    client.Exec($"use {db}");
                    client.Exec("create table if not exists all_type(ts timestamp," +
                                "c1 bool," +
                                "c2 tinyint," +
                                "c3 smallint," +
                                "c4 int," +
                                "c5 bigint," +
                                "c6 tinyint unsigned," +
                                "c7 smallint unsigned," +
                                "c8 int unsigned," +
                                "c9 bigint unsigned," +
                                "c10 float," +
                                "c11 double," +
                                "c12 binary(20)," +
                                "c13 nchar(20)" +
                                ")" +
                                "tags(t json)");

                    var stmt = client.StmtInit();
                    stmt.Prepare("insert into ? using all_type tags(?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                    var isInsert = stmt.IsInsert();
                    Assert.True(isInsert);
                    stmt.SetTableName("t1");
                    stmt.SetTags(new object[] { "{\"a\":\"b\"}" });
                    stmt.BindRow(new object[]
                        { now, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, "test_binary", "test_nchar" });
                    stmt.BindRow(new object?[]
                        { nextSecond, null, null, null, null, null, null, null, null, null, null, null, null, null });
                    stmt.AddBatch();
                    stmt.Exec();
                    var affected = stmt.Affected();
                    Assert.Equal((long)2, affected);
                    stmt.Prepare("select * from all_type where ts >= ? order by ts asc");
                    isInsert = stmt.IsInsert();
                    Assert.False(isInsert);
                    stmt.BindRow(new object[] { now });
                    stmt.AddBatch();
                    stmt.Exec();
                    using (var result = stmt.Result())
                    {
                        var fieldCount = result.FieldCount;
                        Assert.Equal(15, fieldCount);
                        Assert.Equal("ts", result.GetName(0));
                        Assert.Equal("c1", result.GetName(1));
                        Assert.Equal("c2", result.GetName(2));
                        Assert.Equal("c3", result.GetName(3));
                        Assert.Equal("c4", result.GetName(4));
                        Assert.Equal("c5", result.GetName(5));
                        Assert.Equal("c6", result.GetName(6));
                        Assert.Equal("c7", result.GetName(7));
                        Assert.Equal("c8", result.GetName(8));
                        Assert.Equal("c9", result.GetName(9));
                        Assert.Equal("c10", result.GetName(10));
                        Assert.Equal("c11", result.GetName(11));
                        Assert.Equal("c12", result.GetName(12));
                        Assert.Equal("c13", result.GetName(13));
                        Assert.Equal("t", result.GetName(14));
                        Assert.Equal(-1, result.AffectRows);
                        var haveNext = result.Read();
                        Assert.True(haveNext);
                        Assert.Equal(now, result.GetValue(0));
                        Assert.Equal(v1, (bool)result.GetValue(1));
                        Assert.Equal(v2, (sbyte)result.GetValue(2));
                        Assert.Equal(v3, (short)result.GetValue(3));
                        Assert.Equal(v4, (int)result.GetValue(4));
                        Assert.Equal(v5, (long)result.GetValue(5));
                        Assert.Equal(v6, (byte)result.GetValue(6));
                        Assert.Equal(v7, (ushort)result.GetValue(7));
                        Assert.Equal(v8, (uint)result.GetValue(8));
                        Assert.Equal(v9, (ulong)result.GetValue(9));
                        Assert.Equal(v10, (float)result.GetValue(10));
                        Assert.Equal(v11, (double)result.GetValue(11));
                        Assert.Equal(Encoding.UTF8.GetBytes("test_binary"), result.GetValue(12));
                        Assert.Equal("test_nchar", result.GetValue(13));
                        Assert.Equal(Encoding.UTF8.GetBytes("{\"a\":\"b\"}"), result.GetValue(14));
                        haveNext = result.Read();
                        Assert.True(haveNext);
                        Assert.Equal(nextSecond, result.GetValue(0));
                        for (int i = 1; i < 13; i++)
                        {
                            Assert.Null(result.GetValue(i));
                        }

                        Assert.Equal(Encoding.UTF8.GetBytes("{\"a\":\"b\"}"), result.GetValue(14));
                    }
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop database if exists {db}");
                }
            }
        }

        [Fact]
        public void TDengineInfluxDBTest()
        {
            var db = "sml_influx_ws";

            var builder =
                new ConnectionStringBuilder(
                    "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata;enableCompression=true");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    client.Exec($"drop database if exists {db}");
                    client.Exec($"create database {db} precision 'ns'");
                    client.Exec($"use {db}");
                    var data =
                        @"http_response,host=host161,method=GET,result=success,server=http://localhost,status_code=404 response_time=0.003226372,http_response_code=404i,content_length=19i,result_type=""success"",result_code=0i 1648090640000000000
request_histogram_latency_seconds_max,aaa=bb,api_range=all,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
process_files_max_files,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=10240 1648090640000000000
request_timer_seconds,host=host161,quantile=0.5,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
request_timer_seconds,host=host161,quantile=0.9,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
request_timer_seconds,host=host161,quantile=0.95,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
request_timer_seconds,host=host161,quantile=0.99,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
request_timer_seconds,host=host161,url=http://192.168.17.148:8080/actuator/prometheus 0.223696211=0,0.016777216=0,0.178956969=0,0.156587348=0,0.2=0,0.626349396=0,0.015379112=0,5=0,0.089478485=0,0.357913941=0,5.726623061=0,0.008388607=0,0.894784851=0,0.006990506=0,3.937053352=0,0.001=0,0.061516456=0,0.134217727=0,1.431655765=0,0.005592405=0,0.984263336=0,0.001398101=0,3.22122547=0,0.033554431=0,0.805306366=0,0.002446676=0,0.003844776=0,0.20132659=0,1.073741824=0,0.022369621=0,1=0,0.002796201=0,1.789569706=0,0.001048576=0,0.246065832=0,0.050331646=0,4.294967296=0,8.589934591=0,0.536870911=0,0.447392426=0,2.505397588=0,10=0,0.013981011=0,0.003495251=0,0.044739241=0,2.863311529=0,0.039146836=0,0.268435456=0,sum=0,3.579139411=0,7.158278826=0,0.011184809=0,0.01258291=0,0.1=0,0.003145726=0,0.055924051=0,0.067108864=0,0.004194304=0,0.001747626=0,0.002097151=0,2.147483647=0,count=0,0.715827881=0,0.009786708=0,0.111848106=0,0.027962026=0,+Inf=0 1648090640000000000
executor_completed_tasks_total,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
jvm_memory_committed_bytes,area=heap,host=host161,id=PS\ Survivor\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=4718592 1648090640000000000
jvm_memory_committed_bytes,area=heap,host=host161,id=PS\ Old\ Gen,url=http://192.168.17.148:8080/actuator/prometheus gauge=100139008 1648090640000000000
jvm_memory_committed_bytes,area=heap,host=host161,id=PS\ Eden\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=123207680 1648090640000000000
jvm_memory_committed_bytes,area=nonheap,host=host161,id=Metaspace,url=http://192.168.17.148:8080/actuator/prometheus gauge=44998656 1648090640000000000
jvm_memory_committed_bytes,area=nonheap,host=host161,id=Code\ Cache,url=http://192.168.17.148:8080/actuator/prometheus gauge=8847360 1648090640000000000
jvm_memory_committed_bytes,area=nonheap,host=host161,id=Compressed\ Class\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=6463488 1648090640000000000
executor_active_threads,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
tomcat_sessions_active_max_sessions,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
system_cpu_count,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=8 1648090640000000000
logback_events_total,host=host161,level=warn,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
logback_events_total,host=host161,level=debug,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
logback_events_total,host=host161,level=error,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
logback_events_total,host=host161,level=trace,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
logback_events_total,host=host161,level=info,url=http://192.168.17.148:8080/actuator/prometheus counter=7 1648090640000000000
application_ready_time_seconds,host=host161,main_application_class=cn.iospider.actuatormicrometer.ActuatorMicrometerApplication,url=http://192.168.17.148:8080/actuator/prometheus gauge=28.542 1648090640000000000
jvm_buffer_total_capacity_bytes,host=host161,id=direct,url=http://192.168.17.148:8080/actuator/prometheus gauge=57345 1648090640000000000
jvm_buffer_total_capacity_bytes,host=host161,id=mapped,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_threads_live_threads,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=41 1648090640000000000
jvm_gc_max_data_size_bytes,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=2863661056 1648090640000000000
executor_pool_max_threads,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=2147483647 1648090640000000000
jvm_gc_overhead_percent,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.00010333333333333333 1648090640000000000
http_server_requests_seconds_max,exception=None,host=host161,method=GET,outcome=SUCCESS,status=200,uri=/actuator/prometheus,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.008994315 1648090640000000000
http_server_requests_seconds_max,exception=None,host=host161,method=GET,outcome=CLIENT_ERROR,status=404,uri=/**,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
tomcat_sessions_rejected_sessions_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
request_histogram_latency_seconds,aaa=bb,api_range=all,host=host161,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
disk_free_bytes,host=host161,path=/Users/jtlian/Downloads/actuator-micrometer/.,url=http://192.168.17.148:8080/actuator/prometheus gauge=77683585024 1648090640000000000
process_cpu_usage,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.0005609754336738071 1648090640000000000
jvm_threads_peak_threads,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=42 1648090640000000000
jvm_gc_memory_allocated_bytes_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=271541440 1648090640000000000
jvm_gc_live_data_size_bytes,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=14251648 1648090640000000000
jvm_memory_used_bytes,area=heap,host=host161,id=PS\ Survivor\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=4565576 1648090640000000000
jvm_memory_used_bytes,area=heap,host=host161,id=PS\ Old\ Gen,url=http://192.168.17.148:8080/actuator/prometheus gauge=14268032 1648090640000000000
jvm_memory_used_bytes,area=heap,host=host161,id=PS\ Eden\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=16630104 1648090640000000000
jvm_memory_used_bytes,area=nonheap,host=host161,id=Metaspace,url=http://192.168.17.148:8080/actuator/prometheus gauge=41165008 1648090640000000000
jvm_memory_used_bytes,area=nonheap,host=host161,id=Code\ Cache,url=http://192.168.17.148:8080/actuator/prometheus gauge=8792832 1648090640000000000
jvm_memory_used_bytes,area=nonheap,host=host161,id=Compressed\ Class\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=5735248 1648090640000000000
jvm_buffer_count_buffers,host=host161,id=direct,url=http://192.168.17.148:8080/actuator/prometheus gauge=9 1648090640000000000
jvm_buffer_count_buffers,host=host161,id=mapped,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
application_started_time_seconds,host=host161,main_application_class=cn.iospider.actuatormicrometer.ActuatorMicrometerApplication,url=http://192.168.17.148:8080/actuator/prometheus gauge=28.535 1648090640000000000
process_start_time_seconds,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=1648087193.449 1648090640000000000
jvm_memory_usage_after_gc_percent,area=heap,host=host161,pool=long-lived,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.004982444402805749 1648090640000000000
system_cpu_usage,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.11106101593026751 1648090640000000000
tomcat_sessions_active_current_sessions,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
executor_queue_remaining_tasks,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=2147483647 1648090640000000000
jvm_threads_daemon_threads,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=37 1648090640000000000
process_uptime_seconds,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=3446.817 1648090640000000000
tomcat_sessions_alive_max_seconds,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
executor_queued_tasks,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
request_timer_seconds_max,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
tomcat_sessions_created_sessions_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
jvm_threads_states_threads,host=host161,state=runnable,url=http://192.168.17.148:8080/actuator/prometheus gauge=17 1648090640000000000
jvm_threads_states_threads,host=host161,state=blocked,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_threads_states_threads,host=host161,state=waiting,url=http://192.168.17.148:8080/actuator/prometheus gauge=19 1648090640000000000
jvm_threads_states_threads,host=host161,state=timed-waiting,url=http://192.168.17.148:8080/actuator/prometheus gauge=5 1648090640000000000
jvm_threads_states_threads,host=host161,state=new,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_threads_states_threads,host=host161,state=terminated,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
process_files_open_files,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=119 1648090640000000000
jvm_memory_max_bytes,area=heap,host=host161,id=PS\ Survivor\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=4718592 1648090640000000000
jvm_memory_max_bytes,area=heap,host=host161,id=PS\ Old\ Gen,url=http://192.168.17.148:8080/actuator/prometheus gauge=2863661056 1648090640000000000
jvm_memory_max_bytes,area=heap,host=host161,id=PS\ Eden\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=1411907584 1648090640000000000
jvm_memory_max_bytes,area=nonheap,host=host161,id=Metaspace,url=http://192.168.17.148:8080/actuator/prometheus gauge=-1 1648090640000000000
jvm_memory_max_bytes,area=nonheap,host=host161,id=Code\ Cache,url=http://192.168.17.148:8080/actuator/prometheus gauge=251658240 1648090640000000000
jvm_memory_max_bytes,area=nonheap,host=host161,id=Compressed\ Class\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=1073741824 1648090640000000000
executor_pool_size_threads,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
disk_total_bytes,host=host161,path=/Users/jtlian/Downloads/actuator-micrometer/.,url=http://192.168.17.148:8080/actuator/prometheus gauge=328000839680 1648090640000000000
http_server_requests_seconds,exception=None,host=host161,method=GET,outcome=SUCCESS,status=200,uri=/actuator/prometheus,url=http://192.168.17.148:8080/actuator/prometheus count=7,sum=0.120204066 1648090640000000000
http_server_requests_seconds,exception=None,host=host161,method=GET,outcome=CLIENT_ERROR,status=404,uri=/**,url=http://192.168.17.148:8080/actuator/prometheus count=4,sum=0.019408184 1648090640000000000
jvm_buffer_memory_used_bytes,host=host161,id=direct,url=http://192.168.17.148:8080/actuator/prometheus gauge=57346 1648090640000000000
jvm_buffer_memory_used_bytes,host=host161,id=mapped,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_gc_memory_promoted_bytes_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=3055728 1648090640000000000
jvm_classes_loaded_classes,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=8526 1648090640000000000
system_load_average_1m,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=3.10107421875 1648090640000000000
tomcat_sessions_expired_sessions_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
executor_pool_core_threads,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=8 1648090640000000000
jvm_classes_unloaded_classes_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
jvm_gc_pause_seconds,action=end\ of\ major\ GC,cause=Metadata\ GC\ Threshold,host=host161,url=http://192.168.17.148:8080/actuator/prometheus count=1,sum=0.037 1648090640000000000
jvm_gc_pause_seconds,action=end\ of\ minor\ GC,cause=Metadata\ GC\ Threshold,host=host161,url=http://192.168.17.148:8080/actuator/prometheus count=1,sum=0.005 1648090640000000000
jvm_gc_pause_seconds,action=end\ of\ minor\ GC,cause=Allocation\ Failure,host=host161,url=http://192.168.17.148:8080/actuator/prometheus count=2,sum=0.041 1648090640000000000
jvm_gc_pause_seconds_max,action=end\ of\ major\ GC,cause=Metadata\ GC\ Threshold,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_gc_pause_seconds_max,action=end\ of\ minor\ GC,cause=Metadata\ GC\ Threshold,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_gc_pause_seconds_max,action=end\ of\ minor\ GC,cause=Allocation\ Failure,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000";
                    client.SchemalessInsert(new string[] { data }, TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL,
                        TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NANO_SECONDS, 0, ReqId.GetReqId());
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop database if exists {db}");
                }
            }
        }

        [Fact]
        public void TDengineTelnetTest()
        {
            var db = "sml_telnet_ws";

            var builder =
                new ConnectionStringBuilder(
                    "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata;enableCompression=true");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    client.Exec($"drop database if exists {db}");
                    client.Exec($"create database {db} precision 'ns'");
                    client.Exec($"use {db}");
                    var data = new string[]
                    {
                        "sys_if_bytes_out 1479496100 1.3E3 host=web01 interface=eth0",
                        "sys_procs_running 1479496100 42 host=web01",
                    };
                    client.SchemalessInsert(data, TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL,
                        TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED, 0, ReqId.GetReqId());
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop database if exists {db}");
                }
            }
        }

        [Fact]
        public void TDengineJsonTest()
        {
            var db = "sml_json_ws";

            var builder =
                new ConnectionStringBuilder(
                    "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata;enableCompression=true");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    client.Exec($"drop database if exists {db}");
                    client.Exec($"create database {db} precision 'ns'");
                    client.Exec($"use {db}");
                    var data = new string[]
                    {
                        @"{
    ""metric"": ""sys"",
    ""timestamp"": 1692346407,
    ""value"": 18,
    ""tags"": {
       ""host"": ""web01"",
       ""dc"": ""lga""
    }
}"
                    };
                    client.SchemalessInsert(data, TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL,
                        TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED, 0, ReqId.GetReqId());
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop database if exists {db}");
                }
            }
        }
    }
}