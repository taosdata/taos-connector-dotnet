using System.Runtime.InteropServices;
using TDengineDriver;
using TDengineWS.Impl;

namespace Examples.WS
{
    internal class WebSocketSTMT
    {

        WSPrepareData wsPrepareData;
        public WebSocketSTMT(string db, string table, bool ifStable)
        {

            this.wsPrepareData = new WSPrepareData(db, table, ifStable);
        }

        public void RunSTMT(string dsn)
        {
            IntPtr wsConn = LibTaosWS.WSConnectWithDSN(dsn);
            try
            {
                IntPtr wsRes = LibTaosWS.WSQuery(wsConn,wsPrepareData.CreateDB());
                ValidQuery(wsRes);
                LibTaosWS.WSFreeResult(wsRes);

                wsRes = LibTaosWS.WSQuery(wsConn, wsPrepareData.CreateTable());
                ValidQuery(wsRes);
                LibTaosWS.WSFreeResult(wsRes);

                IntPtr wsStmt = LibTaosWS.WSStmtInit(wsConn);

                string insert = $"insert into {wsPrepareData.DB}.{wsPrepareData.Table}_01 using {wsPrepareData.DB}.{wsPrepareData.Table}" +
                    $" tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
                    $" values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                Console.WriteLine("====={0}", insert);
                int code = LibTaosWS.WSStmtPrepare(wsStmt, insert);
                ValidStmtStep(code, wsStmt, "WSStmtPrepare");

                TAOS_MULTI_BIND[] wsTags = WSTags(false);
                TAOS_MULTI_BIND[] data = WSData();

                // Length Need to know 
                code = LibTaosWS.WSStmtSetTbnameTags(wsStmt, $"{wsPrepareData.DB}.{wsPrepareData.Table}_01", wsTags, 14);
                ValidStmtStep(code, wsStmt, "WSStmtSetTbnameTags");

                code = LibTaosWS.WSStmtBindParamBatch(wsStmt, data, 14);
                ValidStmtStep(code, wsStmt, "WSStmtBindParamBatch");

                code = LibTaosWS.WSStmtAddBatch(wsStmt);
                ValidStmtStep(code, wsStmt, "WSStmtAddBatch");
                IntPtr stmtAffectRowPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Int32)));
                
                code = LibTaosWS.WSStmtExecute(wsStmt, stmtAffectRowPtr);
                ValidStmtStep(code, wsStmt, "WSStmtExecute");
                Console.WriteLine("WS STMT affect rows:{0}", Marshal.ReadInt32(stmtAffectRowPtr));
                Marshal.FreeHGlobal(stmtAffectRowPtr);

                LibTaosWS.WSStmtClose(wsStmt);

                WSMultiBind.WSFreeTaosBind(wsTags);
                WSMultiBind.WSFreeTaosBind(data);

                ChecKStmt(wsConn);

            }
            finally
            {
                LibTaosWS.WSClose(wsConn);
            }
        }


        public TAOS_MULTI_BIND[] WSTags(bool ifJson)
        {
            TAOS_MULTI_BIND[] tags;
            if (ifJson)
            {
                tags = new TAOS_MULTI_BIND[1];
                string[] strs = new string[] { "\'{\"jtag_timestamp\":1656677700000,\"jtag_bool\":false,\"jtag_num\":3.141592653,\"jtag_str\":\"beijing\",\"jtag_null\":null}\'" };
                tags[0] = WSMultiBind.WSBindJSON(strs);
            }
            else
            {
                tags = new TAOS_MULTI_BIND[14];
                tags[0] = WSMultiBind.WSBindTimestamp(new long[] { 1656677700000 });
                tags[1] = WSMultiBind.WSBindBool(new bool?[] { true });
                tags[2] = WSMultiBind.WSBindTinyInt(new sbyte?[] { -1 });
                tags[3] = WSMultiBind.WSBindSmallInt(new short?[] { -2 });
                tags[4] = WSMultiBind.WSBindInt(new int?[] { -3 });
                tags[5] = WSMultiBind.WSBindBigInt(new long?[] { -4 });
                tags[6] = WSMultiBind.WSBindUTinyInt(new byte?[] { 1 });
                tags[7] = WSMultiBind.WSBindUSmallInt(new ushort?[] { 2 });
                tags[8] = WSMultiBind.WSBindUInt(new uint?[] { 3 });
                tags[9] = WSMultiBind.WSBindUBigInt(new ulong?[] { 4 });
                tags[10] = WSMultiBind.WSBindFloat(new float?[] { 3.14125F });
                tags[11] = WSMultiBind.WSBindDouble(new double?[] { 3.141592653589793238462643D });
                tags[12] = WSMultiBind.WSBindBinary(new string[] { "binary_tag_1" });
                tags[13] = WSMultiBind.WSBindNchar(new string[] { "nchar_tag_1" });

            }
            return tags;
        }

        public TAOS_MULTI_BIND[] WSData()
        {
            TAOS_MULTI_BIND[] data = new TAOS_MULTI_BIND[14];

            data[0] = WSMultiBind.WSBindTimestamp(new long[] { 1656677700000, 1656677710000, 1656677720000, 1656677730000, 1656677740000 });
            data[1] = WSMultiBind.WSBindBool(new bool?[] { true, false, true, false, null });
            data[2] = WSMultiBind.WSBindTinyInt(new sbyte?[] { -1, -2, -3, -4, null });
            data[3] = WSMultiBind.WSBindSmallInt(new short?[] { -2, -3, -4, -5, null });
            data[4] = WSMultiBind.WSBindInt(new int?[] { -3, -4, -5, -6, null });
            data[5] = WSMultiBind.WSBindBigInt(new long?[] { -4, -5, -7, -8, null });
            data[6] = WSMultiBind.WSBindUTinyInt(new byte?[] { 1, 2, 3, 4, null });
            data[7] = WSMultiBind.WSBindUSmallInt(new ushort?[] { 2, 3, 4, 5, null });
            data[8] = WSMultiBind.WSBindUInt(new uint?[] { 3, 4, 5, 6, null });
            data[9] = WSMultiBind.WSBindUBigInt(new ulong?[] { 4, 5, 6, 7, null });
            data[10] = WSMultiBind.WSBindFloat(new float?[] { 3.14125F, 3.14125F * 2, 3.14125F * 3, 3.14125F * 4, null });
            data[11] = WSMultiBind.WSBindDouble(new double?[] { 3.141592653589793238462643D, 3.141592653589793238462643D * 2, 3.141592653589793238462643D * 3, 3.141592653589793238462643D * 4, null });
            data[12] = WSMultiBind.WSBindBinary(new string[] { "binary_col_1", "binary_col_2", "binary_col_3", "binary_col_4",String.Empty });
            data[13] = WSMultiBind.WSBindNchar(new string[] { "nchar_col_1", "nchar_col_2", "nchar_col_3", "nchar_col_4",String.Empty });

            return data;

        }

        public void ValidStmtStep(int code, IntPtr wsStmt, string method)
        {
            if (code != 0)
            {
                throw new Exception($"{method} failed,reason: {LibTaosWS.WSErrorStr(wsStmt)}, code: {code}");
            }
            else
            {
                Console.WriteLine("{0} success", method);
            }
        }
        internal void ValidQuery(IntPtr wsRes)
        {
            int code = LibTaosWS.WSErrorNo(wsRes);
            if (code != 0)
            {
                throw new Exception($"execute SQL failed: reason: {LibTaosWS.WSErrorStr(wsRes)}, code:{code}");
            }
        }
        public void ChecKStmt(IntPtr wsConn)
        {

            IntPtr wsRes = IntPtr.Zero;
            string select = wsPrepareData.SelectTable();
            try
            {
                Console.WriteLine(select);
                wsRes = LibTaosWS.WSQuery(wsConn, select);
                ValidQuery(wsRes);
                Display(wsRes);             
            }
            finally
            {
                LibTaosWS.WSFreeResult(wsRes);
            }
        }

        internal void Display(IntPtr wsRes)
        {

            if (LibTaosWS.WSIsUpdateQuery(wsRes) == true)
            {
                Console.WriteLine($"affect {LibTaosWS.WSAffectRows(wsRes)} rows");
            }
            else
            {
                // Must get fields first.
                List<TDengineMeta> metas = LibTaosWS.WSGetFields(wsRes);
                foreach (var meta in metas)
                {
                    Console.Write("{0} {1}({2}) \t|\t", meta.name, meta.TypeName(), meta.size);
                }
                Console.WriteLine("");
                // Get retrieved data,result set will be free after calling this interface.
                List<object> dataSet = LibTaosWS.WSGetData(wsRes);
                for (int i = 0; i < dataSet.Count;)
                {
                    for (int j = 0; j < metas.Count; j++)
                    {
                        Console.Write("{0}\t|\t", dataSet[i]);
                        i++;
                    }
                    Console.WriteLine("");
                }
            }
            Console.WriteLine("");
        }
    }
}
