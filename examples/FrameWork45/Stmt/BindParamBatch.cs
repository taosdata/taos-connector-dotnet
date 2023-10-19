using System;
using System.Collections.Generic;
using FrameWork45.UtilTools;
using TDengine.Driver;
using TDengine.Driver.Impl.NativeMethods;

namespace FrameWork45.Stmt
{
    public class BindParamBatch
    {
        public void RunStmtBindParamBatch(IntPtr conn, string stable)
        {
            // string stable = stable;
            string subTable = stable + "_s01";
            InitEnv.InitSTable(conn, stable);
            string insertSql = $"insert into ? using {stable} tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            Console.WriteLine(insertSql);

            string querySql = $"select * from {stable}";

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

            NativeMethods.LoadTableInfo(conn, new string[] { stable });
            TAOS_MULTI_BIND[] tagBind = InitEnv.InitTags();
            stmtReturn = NativeMethods.StmtSetTbnameTags(stmt, subTable, tagBind);
            IfStmtSucc(stmtReturn, stmt, "StmtSetTbnameTags()");

            TAOS_MULTI_BIND[] dataBind = InitEnv.InitData();
            stmtReturn = NativeMethods.StmtBindParamBatch(stmt, dataBind);
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

            IntPtr res = Tools.ExecuteQuery(conn, querySql);
            Tools.DisplayRes(res);

            Tools.FreeTaosRes(res);
            TaosMultiBind.FreeTaosBind(tagBind);
            TaosMultiBind.FreeTaosBind(dataBind);
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
