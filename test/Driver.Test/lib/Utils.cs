using System.Text;
using TDengineDriver;
using TDengineDriver.Impl;
using Xunit.Abstractions;
using System;
using System.Collections.Generic;
namespace Test.Utils
{
    public class Tools
    {
        /*----------------------- construct SQL ----------------------*/
        public static string CreateDB(string db)
        {
            return $"create database if not exists {db} keep 36500";
        }

        public static string UseDB(string db)
        {
            return $"use {db}";
        }

        public static string CreateTable(string table, bool ifStable = true, bool ifJson = false)
        {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.Append($"create table if not exists {table}");
            sqlBuilder.Append("(");
            sqlBuilder.Append("ts timestamp");
            sqlBuilder.Append(",v1 tinyint");
            sqlBuilder.Append(",v2 smallint");
            sqlBuilder.Append(",v4 int");
            sqlBuilder.Append(",v8 bigint");
            sqlBuilder.Append(",u1 tinyint unsigned");
            sqlBuilder.Append(",u2 smallint unsigned");
            sqlBuilder.Append(",u4 int unsigned");
            sqlBuilder.Append(",u8 bigint unsigned");
            sqlBuilder.Append(",f4 float");
            sqlBuilder.Append(",f8 double");
            sqlBuilder.Append(",bin binary(200)");
            sqlBuilder.Append(",nchr nchar(200)");
            sqlBuilder.Append(",b bool");
            sqlBuilder.Append(")");
            if (ifStable)
            {
                sqlBuilder.Append("tags(");
                if (ifJson)
                {
                    sqlBuilder.Append("json_tag json");
                }
                else
                {
                    sqlBuilder.Append("bo bool");
                    sqlBuilder.Append(",tt tinyint");
                    sqlBuilder.Append(",si smallint");
                    sqlBuilder.Append(",ii int");
                    sqlBuilder.Append(",bi bigint");
                    sqlBuilder.Append(",tu tinyint unsigned");
                    sqlBuilder.Append(",su smallint unsigned");
                    sqlBuilder.Append(",iu int unsigned");
                    sqlBuilder.Append(",bu bigint unsigned");
                    sqlBuilder.Append(",ff float");
                    sqlBuilder.Append(",dd double");
                    sqlBuilder.Append(",bb binary(200)");
                    sqlBuilder.Append(",nc nchar(200)");
                }
                sqlBuilder.Append(')');
            }
            return sqlBuilder.ToString();
        }

        public static string DropDB(string db)
        {
            return $"drop database if exists {db}";
        }

        public static string DropTable(string? db, string table)
        {
            if (string.IsNullOrEmpty(db))
            {
                return $"drop table if exists {table}";
            }
            else
            {
                return $"drop table if exists {db}.{table}";
            }

        }

        // Generate insert SQL for the with the columns' data and tags' data 
        public static string ConstructInsertSql(string table, string stable, List<Object> colData, List<Object> tagData, int numOfRows)
        {
            int numOfFields = colData.Count / numOfRows;
            StringBuilder insertSql;

            if (stable == "")
            {
                insertSql = new StringBuilder($"insert into {table} values(");
            }
            else
            {
                insertSql = new StringBuilder($"insert into {table} using {stable} tags(");

                for (int j = 0; j < tagData.Count; j++)
                {
                    if (tagData[j] is String)
                    {
                        insertSql.Append('\'');
                        insertSql.Append(tagData[j]);
                        insertSql.Append('\'');
                    }
                    else
                    {
                        insertSql.Append(tagData[j]);
                    }
                    if (j + 1 != tagData.Count)
                    {
                        insertSql.Append(',');
                    }
                }

                insertSql.Append(")values(");
            }
            for (int i = 0; i < colData.Count; i++)
            {

                if (colData[i] is String)
                {
                    insertSql.Append('\'');
                    insertSql.Append(colData[i]);
                    insertSql.Append('\'');
                }
                else
                {
                    insertSql.Append(colData[i]);
                }

                if ((i + 1) % numOfFields == 0 && (i + 1) != colData.Count)
                {
                    insertSql.Append(")(");
                }
                else if ((i + 1) == colData.Count)
                {
                    insertSql.Append(')');
                }
                else
                {
                    insertSql.Append(',');
                }
            }
            insertSql.Append(';');
            //Console.WriteLine(insertSql.ToString());

            return insertSql.ToString();
        }

        /*---------------------- warp native methods --------------*/
        public static IntPtr ExecuteQuery(IntPtr conn, String sql, ITestOutputHelper output)
        {
            IntPtr res = TDengine.Query(conn, sql);
            if (!IsValidResult(res))
            {
                throw new Exception($"execute {sql} failed.");
            }
            return res;
        }

        public static IntPtr ExecuteErrorQuery(IntPtr conn, String sql)
        {
            IntPtr res = TDengine.Query(conn, sql);
            if (!IsValidResult(res))
            {
                throw new Exception($"execute {sql} failed.");
            }
            return res;
        }

        public static void ExecuteUpdate(IntPtr conn, String sql, ITestOutputHelper output)
        {
            IntPtr res = TDengine.Query(conn, sql);
            if (!IsValidResult(res))
            {
                throw new Exception($"execute {sql} failed.");
            }
            TDengine.FreeResult(res);
        }

        public static void FreeResult(IntPtr res)
        {
            TDengine.FreeResult(res);
        }

        /*------------------------ parse TAO_RES ---------------------*/
        public static void DisplayRes(IntPtr taosRes)
        {
            if (!IsValidResult(taosRes))
            {
                ExitProgram();
            }

            List<TDengineMeta> metaList = LibTaos.GetMeta(taosRes);
            metaList.ForEach(meta => Console.Write("{0} {1}({2}) \t|", meta.name, meta.TypeName(), meta.size));
            Console.WriteLine();

            List<Object> dataList = LibTaos.GetData(taosRes);
            for (int i = 0; i < dataList.Count; i += metaList.Count)
            {
                for (int j = 0; j < metaList.Count; j++)
                {
                    Console.Write(" {0} \t|", dataList[i + j].ToString());

                }
                Console.WriteLine("");
            }

        }

        /*----------------------- For Test ------------*/
        public static TDengineMeta ConstructTDengineMeta(string name, string type)
        {
            TDengineMeta _meta = new TDengineMeta();
            _meta.name = name;
            char[] separators = new char[] { '(', ')' };
            string[] subs = type.Split(separators, StringSplitOptions.RemoveEmptyEntries);

            switch (subs[0].ToUpper())
            {
                case "BOOL":
                    _meta.type = 1;
                    _meta.size = 1;
                    break;
                case "TINYINT":
                    _meta.type = 2;
                    _meta.size = 1;
                    break;
                case "SMALLINT":
                    _meta.type = 3;
                    _meta.size = 2;
                    break;
                case "INT":
                    _meta.type = 4;
                    _meta.size = 4;
                    break;
                case "BIGINT":
                    _meta.type = 5;
                    _meta.size = 8;
                    break;
                case "TINYINT UNSIGNED":
                    _meta.type = 11;
                    _meta.size = 1;
                    break;
                case "SMALLINT UNSIGNED":
                    _meta.type = 12;
                    _meta.size = 2;
                    break;
                case "INT UNSIGNED":
                    _meta.type = 13;
                    _meta.size = 4;
                    break;
                case "BIGINT UNSIGNED":
                    _meta.type = 14;
                    _meta.size = 8;
                    break;
                case "FLOAT":
                    _meta.type = 6;
                    _meta.size = 4;
                    break;
                case "DOUBLE":
                    _meta.type = 7;
                    _meta.size = 8;
                    break;
                case "BINARY":
                    _meta.type = 8;
                    _meta.size = short.Parse(subs[1]);
                    break;
                case "TIMESTAMP":
                    _meta.type = 9;
                    _meta.size = 8;
                    break;
                case "NCHAR":
                    _meta.type = 10;
                    _meta.size = short.Parse(subs[1]);
                    break;
                case "JSON":
                    _meta.type = 15;
                    _meta.size = 4095;
                    break;
                default:
                    _meta.type = byte.MaxValue;
                    _meta.size = 0;
                    break;
            }
            return _meta;
        }

        public static List<TDengineMeta> GetMetaFromDDL(string dllStr)
        {
            var expectResMeta = new List<TDengineMeta>();
            //"CREATE TABLE meters(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(location BINARY(30), groupId INT);";
            int bracketInd = dllStr.IndexOf("(");
            //(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(location BINARY(30), groupId INT);
            string subDllStr = dllStr.Substring(bracketInd);

            String[] stableSeparators = new String[] { "tags", "TAGS" };
            //(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT)
            //(location BINARY(30), groupId INT)
            String[] dllStrElements = subDllStr.Split(stableSeparators, StringSplitOptions.RemoveEmptyEntries);
            //(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT)
            dllStrElements[0] = dllStrElements[0].Substring(1, dllStrElements[0].Length - 2);
            String[] finalStr1 = dllStrElements[0].Split(',', StringSplitOptions.RemoveEmptyEntries);
            foreach (string item in finalStr1)
            {
                //ts TIMESTAMP
                string[] itemArr = item.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
                // Console.WriteLine("GetMetaFromDLL():{0},{1}",itemArr[0],itemArr[1]);
                expectResMeta.Add(Tools.ConstructTDengineMeta(itemArr[0], itemArr[1]));
            }
            if (dllStr.Contains("TAGS") || dllStr.Contains("tags"))
            {
                //location BINARY(30), groupId INT
                dllStrElements[1] = dllStrElements[1].Substring(1, dllStrElements[1].Length - 2);
                //location BINARY(30)  groupId INT
                String[] finalStr2 = dllStrElements[1].Split(',', StringSplitOptions.RemoveEmptyEntries);
                foreach (string item in finalStr2)
                {
                    //location BINARY(30)
                    string[] itemArr = item.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
                    // Console.WriteLine("GetMetaFromDLL():{0},{1}",itemArr[0],itemArr[1]);
                    expectResMeta.Add(Tools.ConstructTDengineMeta(itemArr[0], itemArr[1]));
                }

            }
            return expectResMeta;
        }

        public static List<object> ConstructResData(List<object> colData, List<object> tagData, int numOfRows)
        {
            var list = new List<Object>();
            for (int i = 0; i < colData.Count; i++)
            {
                list.Add(colData[i]);
                if ((i + 1) % (colData.Count / numOfRows) == 0)
                {
                    for (int j = 0; j < tagData.Count; j++)
                    {
                        list.Add(tagData[j]);
                    }
                }
            }
            return list;
        }

        public static List<Object> ColumnsList(int numOfRows) 
        {
            List<object> columns = new List<object>();
            for (int i = 0; i < numOfRows; i++)
            {
                columns.Add(1659060000000 + (i * 10));
                columns.Add((sbyte)(-10+i));
                columns.Add((short)(-20+i));
                columns.Add(-30+i);
                columns.Add((long)(-40 + i));
                columns.Add((byte)i);
                columns.Add((ushort)(i+1));
                columns.Add((uint)(i + 2));
                columns.Add((ulong)(i + 3));
                columns.Add((float)(3.1415F + i));
                columns.Add((double)(3.1415926535897932D + i));
                columns.Add("binary_col_列_"+i);
                columns.Add("nchar_col_列_" + i);
                columns.Add((i & 1) == 1 ? true : false);
            }
            return columns;
        }

        public static List<Object> TagsList(int seq, bool ifJson = false)
        {
            List<object> tags = new List<object> ();
            List<Object> jTags = new List<Object>();
            if (seq > 3 || seq < 1)
            {
                throw new IndexOutOfRangeException("seq should in range 1-3");
            }
            if (ifJson)
            {
                switch (seq)
                {
                    case 1:
                        jTags.Add("{\"key1\":\"taosdata\",\"key2\":null,\"key3\":\"TDengine涛思数据\",\"key4\":1,\"key5\":true}");
                        break;
                    case 2:
                        jTags.Add("{\"key1\":\"taosdata\",\"key2\":null,\"key3\":\"TDengine涛思数据\",\"key4\":2,\"key5\":false}");
                        break;
                    case 3:
                        jTags.Add("{\"key1\":\"taosdata\",\"key2\":null,\"key3\":\"TDengine涛思数据\",\"key4\":3,\"key5\":true}");
                        break;
                    default:
                        throw new IndexOutOfRangeException("seq should in range 1-3");
                }
                return jTags;
            }
            else
            {
                tags.Add((seq&1) == 1?true:false);
                tags.Add((sbyte)(-10 + seq));
                tags.Add((short)(-20 + seq));
                tags.Add(-30 + seq);
                tags.Add((long)(-40 + seq));
                tags.Add((byte)(0 + seq));
                tags.Add((ushort)(1 + seq));
                tags.Add((uint)(2 + seq));
                tags.Add((ulong)(3 + seq));
                tags.Add((float)(3.1415F + seq));
                tags.Add((double)(3.1415926535897932D + seq));
                tags.Add("binary_tag_标签_"+seq);
                tags.Add("nchar_tag_标签_"+seq);

                return tags;
            }
 
        }
        
        /*-------------------- other utility functions -----------------*/
        public static bool IsValidResult(IntPtr res)
        {
            if ((res == IntPtr.Zero))
            {
                throw new Exception($"invalid TAOS_RES,reason {TDengine.Error(res)} code:{TDengine.ErrorNo(res)}");
            }
            return true;
        }

        public static void ExitProgram(int statusCode = 1)
        {
            TDengine.Cleanup();
            System.Environment.Exit(statusCode);
        }

    }

}

