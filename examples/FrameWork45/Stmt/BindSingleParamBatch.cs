using System;
using TDengineDriver;
using FrameWork45.UtilTools;

namespace FrameWork45.Stmt
{
    internal class BindSingleParamBatch
    {
        public void RunStmtBindSingleParam(IntPtr conn, string table)
        {
            string insertSql = "insert into ? values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            string selectSql = $"select * from {table};";

            InitEnv.InitNTable(conn, table);
            Console.WriteLine(insertSql);

            IntPtr stmt = TDengineDriver.TDengine.StmtInit(conn);
            if (stmt == IntPtr.Zero)
            {
                Console.WriteLine("StmtInit() fail");
                System.Environment.Exit(1);
            }
            else
            {
                Console.WriteLine("StmtInit() success");
            }

            int stmtReturn = -1;
            stmtReturn = TDengineDriver.TDengine.StmtPrepare(stmt, insertSql);
            IfStmtSucc(stmtReturn, stmt, "StmtPrepare()");

            stmtReturn = TDengineDriver.TDengine.StmtSetTbname(stmt, table);
            IfStmtSucc(stmtReturn, stmt, "StmtSetTbname()");

            TAOS_MULTI_BIND[] dataBind = InitEnv.InitData();
            stmtReturn = TDengineDriver.TDengine.StmtBindParamBatch(stmt, dataBind);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[0], 0);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[1], 1);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[2], 2);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[3], 3);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[4], 4);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[5], 5);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[6], 6);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[7], 7);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[8], 8);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[9], 9);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[10], 10);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[11], 11);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[12], 12);
            stmtReturn = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref dataBind[13], 13);
            IfStmtSucc(stmtReturn, stmt, "StmtBindParamBatch()");

            stmtReturn = TDengineDriver.TDengine.StmtAddBatch(stmt);
            IfStmtSucc(stmtReturn, stmt, "StmtAddBatch()");

            stmtReturn = TDengineDriver.TDengine.StmtExecute(stmt);
            IfStmtSucc(stmtReturn, stmt, "StmtExecute()");


            if (TDengineDriver.TDengine.StmtClose(stmt) == 0)
            {
                Console.WriteLine("StmtClose() success");
            }
            else
            {
                throw new Exception("StmtClose() failed");
            };
            IntPtr res = Tools.ExecuteQuery(conn, selectSql);
            Tools.DisplayRes(res);
            TaosMultiBind.FreeTaosBind(dataBind);
            Tools.FreeTaosRes(res);
            InitEnv.Dispose(conn);
        }


        public void IfStmtSucc(int stmtReturn, IntPtr stmt, string method)
        {
            if (stmtReturn == 0)
            {
                Console.WriteLine($"{method} success");
            }
            else
            {
                throw new Exception($"{method} failed,reason:{TDengineDriver.TDengine.StmtErrorStr(stmt)}");
            }
        }
    }
}
