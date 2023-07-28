using System;
using System.Data.Common;
using NUnit.Framework;
using TDengine.Data.Client;

namespace TDengine.Data.Tests
{
    public class TDengineDataReaderTesting
    {
        private DbConnection _connection;

        [SetUp]
        public void Setup()
        {
            var builder = new TDengineConnectionStringBuilder("username=root;password=taosdata");
            _connection = new TDengineConnection(builder.ConnectionString);
            _connection.Open();
        }

        [TearDown]
        public void Cleanup()
        {
            try
            {
                var cmd = _connection.CreateCommand();
                cmd.CommandText = "drop database if exists test_common";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop database if exists test_stmt";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "drop database if exists test_nano";
                cmd.ExecuteNonQuery();
            }
            finally
            {
                _connection.Close();
                _connection.Dispose();
            }
        }

        [Test]
        public void CommonExec()
        {
            var cmd = _connection.CreateCommand();
            cmd.CommandText = "drop database if exists test_common";
            var affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, affected);
            cmd.CommandText = "create database test_common";
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, affected);
            _connection.ChangeDatabase("test_common");
            cmd.CommandText =
                "create table if not exists test_types(" +
                "ts timestamp, f_int int, f_bigint bigint, f_float float, f_double double, f_binary binary(16), " +
                "f_smallint smallint, f_tinyint tinyint, f_bool bool, f_nchar nchar(16), " +
                "f_uint int unsigned,f_ubigint bigint unsigned, f_usmallint smallint unsigned,f_utinyint tinyint unsigned)";
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, affected);
            cmd.CommandText =
                "insert into test_types values(now+1s, null, null, null, null, null, null, null, null, null,null,null,null,null)," +
                "(now, 1, 2, 3000000.3, 400000000.4, '5binary', 6, 7, true, '9nchar',10,11,12,13)";
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(2, affected);
            cmd.CommandText =
                "select ts, f_int, f_bigint, f_float, f_double, f_binary, f_smallint, f_tinyint, f_bool, f_nchar,f_uint,f_ubigint,f_usmallint,f_utinyint from test_types order by ts desc limit 2";
            var reader = cmd.ExecuteReader();
            reader.Read();
            Assert.AreEqual(false,reader.IsDBNull(0));
            for (int i = 1; i < 14; i++)
            {
                Assert.AreEqual(true,reader.IsDBNull(i));
                Assert.AreEqual(DBNull.Value, reader.GetValue(i));
            }

            reader.Read();
            Assert.AreEqual(false,reader.IsDBNull(0));
            Assert.AreEqual(14, reader.FieldCount);
            Assert.AreEqual((int)1, reader.GetValue(1));
            Assert.AreEqual((int)1, reader.GetInt32(1));

            Assert.AreEqual((long)2, reader.GetValue(2));
            Assert.AreEqual((long)2, reader.GetInt64(2));

            Assert.AreEqual((float)3000000.3, reader.GetValue(3));
            Assert.AreEqual((float)3000000.3, reader.GetFloat(3));

            Assert.AreEqual((double)400000000.4, reader.GetValue(4));
            Assert.AreEqual((double)400000000.4, reader.GetDouble(4));

            Assert.AreEqual("5binary", reader.GetValue(5));
            Assert.AreEqual("5binary", reader.GetString(5));

            Assert.AreEqual((short)6, reader.GetValue(6));
            Assert.AreEqual((short)6, reader.GetInt16(6));

            Assert.AreEqual((sbyte)7, reader.GetValue(7));
            Assert.AreEqual((sbyte)7, (sbyte)reader.GetByte(7));

            Assert.AreEqual(true, reader.GetValue(8));
            Assert.AreEqual(true, reader.GetBoolean(8));

            Assert.AreEqual("9nchar", reader.GetValue(9));
            Assert.AreEqual("9nchar", reader.GetString(9));

            Assert.AreEqual((uint)10, reader.GetValue(10));
            Assert.AreEqual((uint)10, (uint)reader.GetInt32(10));

            Assert.AreEqual((ulong)11, reader.GetValue(11));
            Assert.AreEqual((ulong)11, (ulong)reader.GetInt64(11));

            Assert.AreEqual((ushort)12, reader.GetValue(12));
            Assert.AreEqual((ushort)12, (ushort)reader.GetInt16(12));

            Assert.AreEqual((byte)13, reader.GetValue(13));
            Assert.AreEqual((byte)13, reader.GetByte(13));

            reader.Close();
            cmd.CommandText = "drop database if exists test_common";
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, affected);
        }
        
        [Test]
        public void Statement()
        {
            var cmd = _connection.CreateCommand();
            cmd.CommandText = "drop database if exists test_stmt";
            var affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, affected);
            cmd.CommandText = "create database test_stmt";
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, affected);
            _connection.ChangeDatabase("test_stmt");
            cmd.CommandText =
                "create table if not exists test_stb(" +
                "ts timestamp, f_int int, f_bigint bigint, f_float float, f_double double, f_binary binary(16), " +
                "f_smallint smallint, f_tinyint tinyint, f_bool bool, f_nchar nchar(16), " +
                "f_uint int unsigned,f_ubigint bigint unsigned, f_usmallint smallint unsigned,f_utinyint tinyint unsigned) tags(val int)";
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, affected);
            cmd.CommandText =
                "insert into ? using test_stb tags (?) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?)";
            var now = DateTime.Now;
            DateTime nextSecond = now.AddSeconds(1);
            var parameters = cmd.Parameters;
            // table name;
            parameters.Add(new TDengineParameter("#table","test_types"));
            // tag name;
            parameters.Add(new TDengineParameter("$0", "123"));
            // value
            parameters.Add(new TDengineParameter("@0",now));
            parameters.Add(new TDengineParameter("@1",(int)1));
            parameters.Add(new TDengineParameter("@2",(long)2));
            parameters.Add(new TDengineParameter("@3",(float)3000000.3));
            parameters.Add(new TDengineParameter("@4",(double)400000000.4));
            parameters.Add(new TDengineParameter("@5","5binary"));
            parameters.Add(new TDengineParameter("@6",(short)6));
            parameters.Add(new TDengineParameter("@7",(sbyte)7));
            parameters.Add(new TDengineParameter("@8",true));
            parameters.Add(new TDengineParameter("@9","9nchar"));
            parameters.Add(new TDengineParameter("@10",(uint)10));
            parameters.Add(new TDengineParameter("@11",(ulong)11));
            parameters.Add(new TDengineParameter("@12",(ushort)12));
            parameters.Add(new TDengineParameter("@13",(byte)13));
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(1, affected);
            parameters.Clear();
            parameters.Add(new TDengineParameter("#table","test_types"));
            parameters.Add(new TDengineParameter("$0", "123"));
            parameters.Add(new TDengineParameter("@0",nextSecond));
            parameters.Add(new TDengineParameter("@1",null));
            parameters.Add(new TDengineParameter("@2",null));
            parameters.Add(new TDengineParameter("@3",null));
            parameters.Add(new TDengineParameter("@4",null));
            parameters.Add(new TDengineParameter("@5",null));
            parameters.Add(new TDengineParameter("@6",null));
            parameters.Add(new TDengineParameter("@7",null));
            parameters.Add(new TDengineParameter("@8",null));
            parameters.Add(new TDengineParameter("@9",null));
            parameters.Add(new TDengineParameter("@10",null));
            parameters.Add(new TDengineParameter("@11",null));
            parameters.Add(new TDengineParameter("@12",null));
            parameters.Add(new TDengineParameter("@13",null));
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(1, affected);
            parameters.Clear();
            cmd.CommandText =
                "select * from test_types where ts <= ? order by ts desc limit 2";
            parameters.Add(new TDengineParameter("@0",nextSecond.AddSeconds(1)));
            var reader = cmd.ExecuteReader();
            parameters.Clear();
            reader.Read();
            Assert.AreEqual(false,reader.IsDBNull(0));
            for (int i = 1; i < 14; i++)
            {
                Assert.AreEqual(true,reader.IsDBNull(i));
                Assert.AreEqual(DBNull.Value, reader.GetValue(i));
            }

            reader.Read();
            Assert.AreEqual(false,reader.IsDBNull(0));
            Assert.AreEqual(14, reader.FieldCount);
            Assert.AreEqual((int)1, reader.GetValue(1));
            Assert.AreEqual((int)1, reader.GetInt32(1));

            Assert.AreEqual((long)2, reader.GetValue(2));
            Assert.AreEqual((long)2, reader.GetInt64(2));

            Assert.AreEqual((float)3000000.3, reader.GetValue(3));
            Assert.AreEqual((float)3000000.3, reader.GetFloat(3));

            Assert.AreEqual((double)400000000.4, reader.GetValue(4));
            Assert.AreEqual((double)400000000.4, reader.GetDouble(4));

            Assert.AreEqual("5binary", reader.GetValue(5));
            Assert.AreEqual("5binary", reader.GetString(5));

            Assert.AreEqual((short)6, reader.GetValue(6));
            Assert.AreEqual((short)6, reader.GetInt16(6));

            Assert.AreEqual((sbyte)7, reader.GetValue(7));
            Assert.AreEqual((sbyte)7, (sbyte)reader.GetByte(7));

            Assert.AreEqual(true, reader.GetValue(8));
            Assert.AreEqual(true, reader.GetBoolean(8));

            Assert.AreEqual("9nchar", reader.GetValue(9));
            Assert.AreEqual("9nchar", reader.GetString(9));

            Assert.AreEqual((uint)10, reader.GetValue(10));
            Assert.AreEqual((uint)10, (uint)reader.GetInt32(10));

            Assert.AreEqual((ulong)11, reader.GetValue(11));
            Assert.AreEqual((ulong)11, (ulong)reader.GetInt64(11));

            Assert.AreEqual((ushort)12, reader.GetValue(12));
            Assert.AreEqual((ushort)12, (ushort)reader.GetInt16(12));

            Assert.AreEqual((byte)13, reader.GetValue(13));
            Assert.AreEqual((byte)13, reader.GetByte(13));

            reader.Close();
            cmd.CommandText = "drop database if exists test_stmt";
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, affected);
        }

        [Test]
        public void StatementNano()
        {
            var cmd = _connection.CreateCommand();
            cmd.CommandText = "drop database if exists test_nano";
            var affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, affected);
            cmd.CommandText = "create database test_nano precision 'ns'";
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, affected);
            _connection.ChangeDatabase("test_nano");
            cmd.CommandText =
                "create table if not exists test_stb(" +
                "ts timestamp, f_int int, f_bigint bigint, f_float float, f_double double, f_binary binary(16), " +
                "f_smallint smallint, f_tinyint tinyint, f_bool bool, f_nchar nchar(16), " +
                "f_uint int unsigned,f_ubigint bigint unsigned, f_usmallint smallint unsigned,f_utinyint tinyint unsigned) tags(val int)";
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, affected);
            cmd.CommandText =
                "insert into ? using test_stb tags (?) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?)";
            var now = DateTime.Now;
            DateTime nextSecond = now.AddSeconds(1);
            var parameters = cmd.Parameters;
            // table name;
            parameters.Add(new TDengineParameter("#table","test_types"));
            // tag name;
            parameters.Add(new TDengineParameter("$0", "123"));
            // value
            parameters.Add(new TDengineParameter("@0",now));
            parameters.Add(new TDengineParameter("@1",(int)1));
            parameters.Add(new TDengineParameter("@2",(long)2));
            parameters.Add(new TDengineParameter("@3",(float)3000000.3));
            parameters.Add(new TDengineParameter("@4",(double)400000000.4));
            parameters.Add(new TDengineParameter("@5","5binary"));
            parameters.Add(new TDengineParameter("@6",(short)6));
            parameters.Add(new TDengineParameter("@7",(sbyte)7));
            parameters.Add(new TDengineParameter("@8",true));
            parameters.Add(new TDengineParameter("@9","9nchar"));
            parameters.Add(new TDengineParameter("@10",(uint)10));
            parameters.Add(new TDengineParameter("@11",(ulong)11));
            parameters.Add(new TDengineParameter("@12",(ushort)12));
            parameters.Add(new TDengineParameter("@13",(byte)13));
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(1, affected);
            parameters.Clear();
            parameters.Add(new TDengineParameter("#table","test_types"));
            parameters.Add(new TDengineParameter("$0", "123"));
            parameters.Add(new TDengineParameter("@0",nextSecond));
            parameters.Add(new TDengineParameter("@1",null));
            parameters.Add(new TDengineParameter("@2",null));
            parameters.Add(new TDengineParameter("@3",null));
            parameters.Add(new TDengineParameter("@4",null));
            parameters.Add(new TDengineParameter("@5",null));
            parameters.Add(new TDengineParameter("@6",null));
            parameters.Add(new TDengineParameter("@7",null));
            parameters.Add(new TDengineParameter("@8",null));
            parameters.Add(new TDengineParameter("@9",null));
            parameters.Add(new TDengineParameter("@10",null));
            parameters.Add(new TDengineParameter("@11",null));
            parameters.Add(new TDengineParameter("@12",null));
            parameters.Add(new TDengineParameter("@13",null));
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(1, affected);
            parameters.Clear();
            cmd.CommandText =
                "select * from test_types where ts <= ? order by ts desc limit 2";
            parameters.Add(new TDengineParameter("@0",nextSecond.AddSeconds(1)));
            var reader = cmd.ExecuteReader();
            parameters.Clear();
            reader.Read();
            Assert.AreEqual(false,reader.IsDBNull(0));
            for (int i = 1; i < 14; i++)
            {
                Assert.AreEqual(true,reader.IsDBNull(i));
                Assert.AreEqual(DBNull.Value, reader.GetValue(i));
            }

            reader.Read();
            Assert.AreEqual(false,reader.IsDBNull(0));
            Assert.AreEqual(now,reader.GetDateTime(0));
            Assert.AreEqual(14, reader.FieldCount);
            Assert.AreEqual((int)1, reader.GetValue(1));
            Assert.AreEqual((int)1, reader.GetInt32(1));

            Assert.AreEqual((long)2, reader.GetValue(2));
            Assert.AreEqual((long)2, reader.GetInt64(2));

            Assert.AreEqual((float)3000000.3, reader.GetValue(3));
            Assert.AreEqual((float)3000000.3, reader.GetFloat(3));

            Assert.AreEqual((double)400000000.4, reader.GetValue(4));
            Assert.AreEqual((double)400000000.4, reader.GetDouble(4));

            Assert.AreEqual("5binary", reader.GetValue(5));
            Assert.AreEqual("5binary", reader.GetString(5));

            Assert.AreEqual((short)6, reader.GetValue(6));
            Assert.AreEqual((short)6, reader.GetInt16(6));

            Assert.AreEqual((sbyte)7, reader.GetValue(7));
            Assert.AreEqual((sbyte)7, (sbyte)reader.GetByte(7));

            Assert.AreEqual(true, reader.GetValue(8));
            Assert.AreEqual(true, reader.GetBoolean(8));

            Assert.AreEqual("9nchar", reader.GetValue(9));
            Assert.AreEqual("9nchar", reader.GetString(9));

            Assert.AreEqual((uint)10, reader.GetValue(10));
            Assert.AreEqual((uint)10, (uint)reader.GetInt32(10));

            Assert.AreEqual((ulong)11, reader.GetValue(11));
            Assert.AreEqual((ulong)11, (ulong)reader.GetInt64(11));

            Assert.AreEqual((ushort)12, reader.GetValue(12));
            Assert.AreEqual((ushort)12, (ushort)reader.GetInt16(12));

            Assert.AreEqual((byte)13, reader.GetValue(13));
            Assert.AreEqual((byte)13, reader.GetByte(13));

            reader.Close();
            cmd.CommandText = "drop database if exists test_nano";
            affected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, affected);
        }
    }
}