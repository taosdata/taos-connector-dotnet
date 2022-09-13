using System;
using TDengineDriver;
using System.Runtime.InteropServices;

namespace Test.UtilsTools
{
    public class StmtUtilTools
    {
        public static IntPtr StmtInit(IntPtr conn)
        {
            IntPtr stmt = TDengine.StmtInit(conn);
            if (stmt == IntPtr.Zero)
            {
                throw new Exception("Init stmt failed");
            }
            else
            {
                Console.WriteLine("Init stmt success");
            }
            return stmt;
        }

        public static void StmtPrepare(IntPtr stmt, string sql)
        {
            int res = TDengine.StmtPrepare(stmt, sql);
            if (res == 0)
            {
                Console.WriteLine("stmt prepare success");
            }
            else
            {
                throw new Exception("stmt prepare failed " + TDengine.StmtErrorStr(stmt));
            }
        }

        public static void SetTableName(IntPtr stmt, String tableName)
        {
            int res = TDengine.StmtSetTbname(stmt, tableName);
            if (res == 0)
            {
                Console.WriteLine("set_tbname success");
            }
            else
            {
                throw new Exception("set_tbname failed, " + TDengine.StmtErrorStr(stmt));
            }
        }

        public static void SetTableNameTags(IntPtr stmt, String tableName, TAOS_BIND[] tags)
        {
            int res = TDengine.StmtSetTbnameTags(stmt, tableName, tags);
            if (res == 0)
            {
                Console.WriteLine("set tbname && tags success");

            }
            else
            {
                throw new Exception("set tbname && tags failed, " + TDengine.StmtErrorStr(stmt));
            }
        }

        public static void SetSubTableName(IntPtr stmt, string name)
        {
            int res = TDengine.StmtSetSubTbname(stmt, name);
            if (res == 0)
            {
                Console.WriteLine("set subtable name success");
            }
            else
            {
                throw new Exception("set subtable name failed, " + TDengine.StmtErrorStr(stmt));
            }

        }

        public static void BindParam(IntPtr stmt, TAOS_BIND[] binds)
        {
            int res = TDengine.StmtBindParam(stmt, binds);
            if (res == 0)
            {
                Console.WriteLine("bind  para success");
            }
            else
            {
                throw new Exception("bind  para failed, " + TDengine.StmtErrorStr(stmt));
            }
        }

        public static void BindSingleParamBatch(IntPtr stmt, TAOS_MULTI_BIND bind, int index)
        {
            int res = TDengine.StmtBindSingleParamBatch(stmt, ref bind, index);
            if (res == 0)
            {
                Console.WriteLine("single bind  batch success");
            }
            else
            {
                //Console.Write("single bind  batch failed: " + TDengine.StmtErrorStr(stmt));
                throw new Exception("single bind  batch failed: " + TDengine.StmtErrorStr(stmt) + $" {res}");
            }
        }

        public static void BindParamBatch(IntPtr stmt, TAOS_MULTI_BIND[] bind)
        {
            int res = TDengine.StmtBindParamBatch(stmt, bind);
            if (res == 0)
            {
                Console.WriteLine("bind  parameter batch success");
            }
            else
            {
                throw new Exception("bind  parameter batch failed, " + TDengine.StmtErrorStr(stmt));
            }
        }

        public static void AddBatch(IntPtr stmt)
        {
            int res = TDengine.StmtAddBatch(stmt);
            if (res == 0)
            {
                Console.WriteLine("stmt add batch success");
            }
            else
            {
                throw new Exception("stmt add batch failed,reason: " + TDengine.StmtErrorStr(stmt));
            }
        }
        public static void StmtExecute(IntPtr stmt)
        {
            int res = TDengine.StmtExecute(stmt);
            if (res == 0)
            {
                Console.WriteLine("Execute stmt success");
            }
            else
            {
                throw new Exception("Execute stmt failed,reason: " + TDengine.StmtErrorStr(stmt));
            }
        }
        public static void StmtClose(IntPtr stmt)
        {
            int res = TDengine.StmtClose(stmt);
            if (res == 0)
            {
                Console.WriteLine("close stmt success");
            }
            else
            {
                throw new Exception("close stmt failed, " + TDengine.StmtErrorStr(stmt));
            }
        }

        public static IntPtr StmtUseResult(IntPtr stmt)
        {
            IntPtr res = TDengine.StmtUseResult(stmt);
            if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
            {
                if (res != IntPtr.Zero)
                {
                    throw new Exception("reason: " + TDengine.Error(res));
                }
            }
            else
            {
                Console.WriteLine("StmtUseResult success");

            }
            return res;
        }

        public static void loadTableInfo(IntPtr conn, string[] arr)
        {
            if (TDengine.LoadTableInfo(conn, arr) == 0)
            {
                Console.WriteLine("load table info success");
            }
            else
            {
                Console.WriteLine("load table info failed");
            }
        }

    }
}