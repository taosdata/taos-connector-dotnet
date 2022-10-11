using System;
using System.Collections.Generic;
using TDengineDriver;
using FrameWork45.UtilTools;

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

            IntPtr stmt = TDengine.StmtInit(conn);
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

            stmtReturn = TDengine.StmtPrepare(stmt, insertSql);
            IfStmtSucc(stmtReturn, stmt, "StmtPrepare()");

            TDengine.LoadTableInfo(conn, new string[] { stable });
            TAOS_MULTI_BIND[] tagBind = InitEnv.InitTags();
            stmtReturn = TDengine.StmtSetTbnameTags(stmt, subTable, tagBind);
            IfStmtSucc(stmtReturn, stmt, "StmtSetTbnameTags()");

            TAOS_MULTI_BIND[] dataBind = InitEnv.InitData();
            stmtReturn = TDengine.StmtBindParamBatch(stmt, dataBind);
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
                throw new Exception($"{method} failed,reason:{TDengine.StmtErrorStr(stmt)}");
            }
        }
    }
}
