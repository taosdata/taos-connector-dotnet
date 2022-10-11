using System;
using TDengineDriver;
using FrameWork45.UtilTools;

namespace FrameWork45.Stmt
{
    internal static class InitEnv
    {
        static string dropDB = "drop database if exists stmt30";
        static string createDB = "create database if not exists stmt30 keep 36500";
        static string selectDB = "use stmt30";
        public static void InitSTable(IntPtr conn, string stable)
        {
            string createTable = $"create stable if not exists {stable} (ts timestamp "
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
                                 + ",tt tinyint"
                                 + ",si smallint"
                                 + ",ii int"
                                 + ",bi bigint"
                                 + ",tu tinyint unsigned"
                                 + ",su smallint unsigned"
                                 + ",iu int unsigned"
                                 + ",bu bigint unsigned"
                                 + ",ff float "
                                 + ",dd double "
                                 + ",vrc_tag varchar(200)"
                                 + ",ncr_tag nchar(200)"
                                + ")";

            string dropTable = $"drop table if exists {stable}";

            Tools.ExecuteUpdate(conn, createDB);
            Tools.ExecuteUpdate(conn, selectDB);
            Tools.ExecuteUpdate(conn, createTable);
        }

        public static void Dispose(IntPtr conn)
        {
            Tools.ExecuteUpdate(conn, dropDB);
        }

        public static void InitNTable(IntPtr conn, string ntable)
        {
            string createTable = $"create table if not exists {ntable} ("
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
            Tools.ExecuteUpdate(conn, createDB);
            Tools.ExecuteUpdate(conn, selectDB);
            Tools.ExecuteUpdate(conn, createTable);

        }
        public static TAOS_MULTI_BIND[] InitTags()
        {
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[13];

            mBinds[0] = TaosMultiBind.MultiBindBool(new bool?[] { true });
            mBinds[1] = TaosMultiBind.MultiBindTinyInt(new sbyte?[] { 1 });
            mBinds[2] = TaosMultiBind.MultiBindSmallInt(new short?[] { 2 });
            mBinds[3] = TaosMultiBind.MultiBindInt(new int?[] { 3 });
            mBinds[4] = TaosMultiBind.MultiBindBigint(new long?[] { 4 });
            mBinds[5] = TaosMultiBind.MultiBindUTinyInt(new byte?[] { 1 });
            mBinds[6] = TaosMultiBind.MultiBindUSmallInt(new ushort?[] { 2 });
            mBinds[7] = TaosMultiBind.MultiBindUInt(new uint?[] { 3 });
            mBinds[8] = TaosMultiBind.MultiBindUBigInt(new ulong?[] { 4 });
            mBinds[9] = TaosMultiBind.MultiBindFloat(new float?[] { 18.58f });
            mBinds[10] = TaosMultiBind.MultiBindDouble(new double?[] { 2020.05071858d });
            mBinds[11] = TaosMultiBind.MultiBindBinary(new string?[] { "taosdata" });
            mBinds[12] = TaosMultiBind.MultiBindNchar(new string?[] { "TDenginge" });

            return mBinds;
        }

        public static TAOS_MULTI_BIND[] InitData()
        {
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[14];

            long[] tsArr = new long[5] { 1656677700000, 1656677710000, 1656677720000, 1656677730000, 1656677700000 };
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

            return mBinds;
        }
    }
}