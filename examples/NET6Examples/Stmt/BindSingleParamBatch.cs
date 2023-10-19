using System;
using System.Collections.Generic;
using TDengineDriver;
using Examples.UtilsTools;
using TDengine.Driver;
using TDengine.Driver.Impl.NativeMethods;

namespace Examples.Stmt
{
    internal class BindSingleParamBatch
    {
        public void RunStmtBindSingleParam(IntPtr conn, string table)
        {
            string insertSql = "insert into ? values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            string selectSql = $"select * from {table};";

            InitEnv.InitNTable(conn, table);
            Console.WriteLine(insertSql);

            IntPtr stmt = NativeMethods.StmtInit(conn);
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
            stmtReturn = NativeMethods.StmtPrepare(stmt, insertSql);
            IfStmtSucc(stmtReturn, stmt, "StmtPrepare()");

            stmtReturn = NativeMethods.StmtSetTbname(stmt, table);
            IfStmtSucc(stmtReturn, stmt, "StmtSetTbname()");

            TAOS_MULTI_BIND[] dataBind = InitEnv.InitData();
            stmtReturn = NativeMethods.StmtBindParamBatch(stmt, dataBind);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[0], 0);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[1], 1);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[2], 2);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[3], 3);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[4], 4);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[5], 5);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[6], 6);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[7], 7);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[8], 8);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[9], 9);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[10], 10);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[11], 11);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[12], 12);
            stmtReturn = NativeMethods.StmtBindSingleParamBatch(stmt, ref dataBind[13], 13);
            IfStmtSucc(stmtReturn, stmt, "StmtBindParamBatch()");

            stmtReturn = NativeMethods.StmtAddBatch(stmt);
            IfStmtSucc(stmtReturn, stmt, "StmtAddBatch()");

            stmtReturn = NativeMethods.StmtExecute(stmt);
            IfStmtSucc(stmtReturn, stmt, "StmtExecute()");


            if (NativeMethods.StmtClose(stmt) == 0)
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
                throw new Exception($"{method} failed,reason:{NativeMethods.StmtErrorStr(stmt)}");
            }
        }
    }
}
