using TDengineDriver;
namespace Benchmark
{
    internal class Stmt
    {
        string Host { get; set; }
        short Port { get; set; }
        string Username { get; set; }
        string Password { get; set; }
        string db = "benchmark";
        string sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        string jtable = "jtb_1";
        string stable = "stb_1";
        public Stmt(string host, string userName, string passwd, short port)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
        }
        public void Run(string types, int times)
        {
            IntPtr res;
            IntPtr conn = TDengineDriver.TDengine.Connect(Host, Username, Password, db, Port);
            if (conn != IntPtr.Zero)
            {
                res = TDengineDriver.TDengine.Query(conn, $"use {db}");
                IfTaosQuerySucc(res, $"use {db}");
                TDengineDriver.TDengine.FreeResult(res);

                // begin stmt
                IntPtr stmt = TDengineDriver.TDengine.StmtInit(conn);
                if (stmt != IntPtr.Zero)
                {
                    int i = 0;
                    while (i < times)
                    {
                        if (types == "normal")
                        { 
                            StmtBindBatch(stmt,sql,stable); 
                        }
                        if(types == "json")
                        { 
                            StmtBindBatch(stmt,sql,jtable); 
                        } 
                        i++;
                    }

                }
                else
                {
                    throw new Exception("init stmt failed.");
                }
                TDengineDriver.TDengine.StmtClose(stmt);
            }
            else
            {
                throw new Exception("create TD connection failed");
            }
            TDengineDriver.TDengine.Close(conn);
        }

        public void StmtBindBatch(IntPtr stmt, string sql,string table)
        {
            int stmtRes = TDengineDriver.TDengine.StmtPrepare(stmt, sql);
            IfStmtSucc(stmtRes,stmt, "StmtPrepare");

            stmtRes = TDengineDriver.TDengine.StmtSetTbname(stmt,table);
            IfStmtSucc(stmtRes, stmt, "StmtSetTbname");

            TAOS_MULTI_BIND[] dataBind =StmtData();

            stmtRes = TDengineDriver.TDengine.StmtBindParamBatch(stmt, dataBind);
            IfStmtSucc(stmtRes, stmt, "StmtBindParamBatch");

            stmtRes = TDengineDriver.TDengine.StmtAddBatch(stmt);
            IfStmtSucc(stmtRes, stmt, "StmtAddBatch");

            stmtRes = TDengineDriver.TDengine.StmtExecute(stmt);
            IfStmtSucc(stmtRes, stmt, "StmtExecute");

            TaosMultiBind.FreeTaosBind(dataBind);
        }

        public bool IfTaosQuerySucc(IntPtr res, string sql)
        {
            if (TDengineDriver.TDengine.ErrorNo(res) == 0)
            {
                return true;
            }
            else
            {
                throw new Exception($"execute {sql} failed,reason {TDengineDriver.TDengine.Error(res)}, code{TDengineDriver.TDengine.ErrorNo(res)}");
            }
        }

        public void IfStmtSucc(int stmtReturn, IntPtr stmt, string method)
        {
            if (stmtReturn != 0)
            {
                throw new Exception($"{method} failed,reason:{TDengineDriver.TDengine.StmtErrorStr(stmt)}");
            }
        }

        public TAOS_MULTI_BIND[] StmtData()
        {
            var mBinds = new TAOS_MULTI_BIND[14];

            long[] tsArr = new long[1] { 1659283200000 };
            bool?[] boolArr = new bool?[1] { true };
            sbyte?[] tinyIntArr = new sbyte?[1] { -1 };
            short?[] shortArr = new short?[1] {-2 };
            int?[] intArr = new int?[1] { -3 };
            long?[] longArr = new long?[1] { -4 };
            byte?[] uTinyIntArr = new byte?[1] { 1};
            ushort?[] uShortArr = new ushort?[1] { 2 };
            uint?[] uIntArr = new uint?[1] { 3 };
            ulong?[] uLongArr = new ulong?[1] { 4 };
            float?[] floatArr = new float?[1] { 3.1415f };
            double?[] doubleArr = new double?[1] { 3.14159265358979d };   
            string[] binaryArr = new string[1] { "bnr_col_1" };
            string[] ncharArr = new string[1] { "ncr_col_1" };

            mBinds[0] = TaosMultiBind.MultiBindTimestamp(tsArr);
            mBinds[1] = TaosMultiBind.MultiBindBool(boolArr);
            mBinds[2] = TaosMultiBind.MultiBindTinyInt(tinyIntArr);
            mBinds[3] = TaosMultiBind.MultiBindSmallInt(shortArr);
            mBinds[4] = TaosMultiBind.MultiBindInt(intArr);
            mBinds[5] = TaosMultiBind.MultiBindBigint(longArr);
            mBinds[6] = TaosMultiBind.MultiBindUTinyInt(uTinyIntArr);
            mBinds[7] = TaosMultiBind.MultiBindUSmallInt(uShortArr);
            mBinds[8] = TaosMultiBind.MultiBindUInt(uIntArr);
            mBinds[9] = TaosMultiBind.MultiBindUBigInt(uLongArr);
            mBinds[10] = TaosMultiBind.MultiBindFloat(floatArr);
            mBinds[11] = TaosMultiBind.MultiBindDouble(doubleArr);
            mBinds[12] = TaosMultiBind.MultiBindBinary(binaryArr);
            mBinds[13] = TaosMultiBind.MultiBindNchar(ncharArr);

            return mBinds;
        }
    }
}