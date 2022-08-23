using System;
using TDengineDriver;

namespace Example
{
    public class StmtBindSingleParamExample
    {
        public void RunStmtBindSingleParam(IntPtr conn, string table)
        {
            string insertSql = "insert into ? values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";//

            InitEnv.InitNTable(conn, table);

            IntPtr stmt = TDengine.StmtInit(conn);
            if (stmt == IntPtr.Zero)
            {
                Console.WriteLine("StmtInit() fail");
                System.Environment.Exit(0);
            }
            else
            {
                Console.WriteLine("StmtInit() success");
            }

            int stmtReturn = -1;
            stmtReturn = TDengine.StmtPrepare(stmt, insertSql);
            IfStmtSucc(stmtReturn, stmt, "StmtPrepare()");

            stmtReturn = TDengine.StmtSetTbname(stmt, table);
            IfStmtSucc(stmtReturn, stmt, "StmtSetTbname()");

            TAOS_MULTI_BIND[] dataBind = InitEnv.InitData();
            //stmtReturn = TDengine.StmtBindParamBatch(stmt, dataBind);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[0], 0);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[1], 1);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[2], 2);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[3], 3);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[4], 4);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[5], 5);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[6], 6);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[7], 7);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[8], 8);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[9], 9);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[10], 10);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[11], 11);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[12], 12);
            stmtReturn = TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[13], 13);
            IfStmtSucc(stmtReturn, stmt, "StmtBindParamBatch()");
            
            stmtReturn = TDengine.StmtAddBatch(stmt);
            IfStmtSucc(stmtReturn, stmt, "StmtAddBatch()");

            stmtReturn = TDengine.StmtExecute(stmt);
            IfStmtSucc(stmtReturn, stmt, "StmtExecute()");


            if (TDengine.StmtClose(stmt) == 0)
            {
                Console.WriteLine("StmtClose() success");
            }
            else
            {
                throw new Exception("StmtClose() failed");
            };

        }

        public void IfStmtSucc(int stmtReturn, IntPtr stmt, string method)
        {
            if (stmtReturn == 0)
            {
                Console.WriteLine($"{method} success");
            }
            else
            {
                throw new Exception($"{method} failed,reason:{TDengine.StmtErrorStr(stmt)}");
            }
        }
    }

    public class InitEnv
    {
        public static void InitSTable(IntPtr conn, string stable)
        {
            // string dropDB = "drop database if exists stmt30";
            // string createDB = "create database if not exists stmt30 keep 36500";
            // string selectDB = "use stmt30";
            string createTable = $"create stable {stable} (ts timestamp "
                                + ",b bool"
                                + ",v1 tinyint"
                                + ",v2 smallint"
                                + ",v4 int"
                                + ",v8 bigint"
                                + ",f4 float"
                                + ",f8 double"
                                + ",u1 tinyint unsigned"
                                + ",u2 smallint unsigned"
                                + ",u4 int unsigned"
                                + ",u8 bigint unsigned"
                                + ",vcr varchar(200)"
                                + ",ncr nchar(200)"
                                + ")tags("
                                + "bo bool"
                                // + ",tt tinyint"
                                // + ",si smallint"
                                // + ",ii int"
                                // + ",bi bigint"
                                // + ",tu tinyint unsigned"
                                // + ",su smallint unsigned"
                                // + ",iu int unsigned"
                                // + ",bu bigint unsigned"
                                // + ",ff float "
                                // + ",dd double "
                                // + ",vrc_tag varchar(200)"
                                // + ",ncr_tag nchar(200)"
                                + ")";

            string dropTable = $"drop table if exists {stable}";
            IntPtr res;
            if ((res = TDengine.Query(conn, dropTable)) != IntPtr.Zero)
            {
                Console.WriteLine($"drop table {stable} success");
                if ((res = TDengine.Query(conn, createTable)) != IntPtr.Zero)
                {
                    Console.WriteLine($"create table {stable} success");
                    TDengine.FreeResult(res);
                }
                else
                {
                    throw new Exception(TDengine.Error(res));
                }
            }
            else
            {
                throw new Exception(TDengine.Error(res));
            }
        }

        public static void InitNTable(IntPtr conn, string ntable)
        {
            string createTable = $"create table {ntable} ("
                                + "ts timestamp "
                                + ",b bool"
                                + ",v1 tinyint"
                                + ",v2 smallint"
                                + ",v4 int"
                                + ",v8 bigint"
                                + ",f4 float"
                                + ",f8 double"
                                + ",u1 tinyint unsigned"
                                + ",u2 smallint unsigned"
                                + ",u4 int unsigned"
                                + ",u8 bigint unsigned"
                                + ",vcr binary(200)"
                                + ",ncr nchar(200)"
                                + ")";

            string dropTable = $"drop table if exists {ntable}";
            IntPtr res;
            if ((res = TDengine.Query(conn, dropTable)) != IntPtr.Zero)
            {
                Console.WriteLine($"drop table {ntable} success");
                if ((res = TDengine.Query(conn, createTable)) != IntPtr.Zero)
                {
                    Console.WriteLine($"create table {ntable} success");
                    TDengine.FreeResult(res);
                }
                else
                {
                    throw new Exception(TDengine.Error(res));
                }
            }
            else
            {
                throw new Exception(TDengine.Error(res));
            }
        }
        public static TAOS_MULTI_BIND[] InitTags()
        {
            // TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[13];
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[1];
            mBinds[0] = TaosMultiBind.MultiBindBool(new bool?[] { true });
            // mBinds[1] = TaosMultiBind.MultiBindTinyInt(new sbyte?[] { 1 });
            // mBinds[2] = TaosMultiBind.MultiBindSmallInt(new short?[] { 2 });
            // mBinds[3] = TaosMultiBind.MultiBindInt(new int?[] { 3 });
            // mBinds[4] = TaosMultiBind.MultiBindBigint(new long?[] { 4 });
            // mBinds[5] = TaosMultiBind.MultiBindFloat(new float?[] { 18.58f });
            // mBinds[6] = TaosMultiBind.MultiBindDouble(new double?[] { 2020.05071858d });
            // mBinds[7] = TaosMultiBind.MultiBindUTinyInt(new byte?[] { 1 });
            // mBinds[8] = TaosMultiBind.MultiBindUSmallInt(new ushort?[] { 2 });
            // mBinds[9] = TaosMultiBind.MultiBindUInt(new uint?[] { 3 });
            // mBinds[10] = TaosMultiBind.MultiBindUBigInt(new ulong?[] { 4 });
            // mBinds[11] = TaosMultiBind.MultiBindBinary(new string?[] { "taosdata" });
            // mBinds[12] = TaosMultiBind.MultiBindNchar(new string?[] { "TDenginge" });
            return mBinds;
        }

        public static TAOS_MULTI_BIND[] InitData()
        {
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[14];
            // TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[8];
            long[] tsArr = new long[5] { 1637064040000, 1637064041000, 1637064042000, 1637064043000, 1637064044000 };
            bool?[] boolArr = new bool?[5] { true, false, null, true, true };
            sbyte?[] tinyIntArr = new sbyte?[5] { -127, 0, null, 8, 127 };
            short?[] shortArr = new short?[5] { short.MinValue + 1, -200, null, 100, short.MaxValue };
            int?[] intArr = new int?[5] { -200, -100, null, 0, 300 };
            long?[] longArr = new long?[5] { long.MinValue + 1, -2000, null, 1000, long.MaxValue };
            float?[] floatArr = new float?[5] { float.MinValue + 1, -12.1F, null, 0F, float.MaxValue };
            double?[] doubleArr = new double?[5] { double.MinValue + 1, -19.112D, null, 0D, double.MaxValue };
            byte?[] uTinyIntArr = new byte?[5] { byte.MinValue, 12, null, 89, byte.MaxValue - 1 };
            ushort?[] uShortArr = new ushort?[5] { ushort.MinValue, 200, null, 400, ushort.MaxValue - 1 };
            uint?[] uIntArr = new uint?[5] { uint.MinValue, 100, null, 2, uint.MaxValue - 1 };
            ulong?[] uLongArr = new ulong?[5] { ulong.MinValue, 2000, null, 1000, long.MaxValue - 1 };
            string[] binaryArr = new string[5] { "1234567890~!@#$%^&*()_+=-`[]{}:,./<>?", String.Empty, null, "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM", "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890~!@#$%^&*()_+=-`[]{}:,./<>?" };
            string[] ncharArr = new string[5] { "1234567890~!@#$%^&*()_+=-`[]{}:,./<>?", null, "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM", "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890~!@#$%^&*()_+=-`[]{}:,./<>?", string.Empty };
            mBinds[0] = TaosMultiBind.MultiBindTimestamp(tsArr);
            mBinds[1] = TaosMultiBind.MultiBindBool(boolArr);
            mBinds[2] = TaosMultiBind.MultiBindTinyInt(tinyIntArr);
            mBinds[3] = TaosMultiBind.MultiBindSmallInt(shortArr);
            mBinds[4] = TaosMultiBind.MultiBindInt(intArr);
            mBinds[5] = TaosMultiBind.MultiBindBigint(longArr);
            mBinds[6] = TaosMultiBind.MultiBindFloat(floatArr);
            mBinds[7] = TaosMultiBind.MultiBindDouble(doubleArr);
            mBinds[8] = TaosMultiBind.MultiBindUTinyInt(uTinyIntArr);
            mBinds[9] = TaosMultiBind.MultiBindUSmallInt(uShortArr);
            mBinds[10] = TaosMultiBind.MultiBindUInt(uIntArr);
            mBinds[11] = TaosMultiBind.MultiBindUBigInt(uLongArr);
            mBinds[12] = TaosMultiBind.MultiBindBinary(binaryArr);
            mBinds[13] = TaosMultiBind.MultiBindNchar(ncharArr);

            // mBinds[1] = TaosMultiBind.MultiBindDouble(doubleArr);
            // mBinds[2] = TaosMultiBind.MultiBindUTinyInt(uTinyIntArr);
            // mBinds[3] = TaosMultiBind.MultiBindUSmallInt(uShortArr);
            // mBinds[4] = TaosMultiBind.MultiBindUInt(uIntArr);
            // mBinds[5] = TaosMultiBind.MultiBindUBigInt(uLongArr);
            // mBinds[6] = TaosMultiBind.MultiBindBinary(binaryArr);
            // mBinds[7] = TaosMultiBind.MultiBindNchar(ncharArr);


            return mBinds;
        }
    }
}
