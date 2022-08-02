using TDengineDriver;
using System.Runtime.InteropServices;
using System;

namespace Test.Utils.Stmt
{
    public class StmtTools
    {
        public static IntPtr StmtInit(IntPtr conn)
        {
            IntPtr stmt = TDengine.StmtInit(conn);
            if (stmt == IntPtr.Zero)
            {
                throw new Exception("StmtInit failed.");
            }
            return stmt;
        }

        public static void StmtPrepare(IntPtr stmt, string sql)
        {
            int res = TDengine.StmtPrepare(stmt, sql);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtPrepare failed,reason:${TDengine.StmtErrorStr(stmt)}");
            }
        }

        public static void SetTableName(IntPtr stmt, String tableName)
        {
            int res = TDengine.StmtSetTbname(stmt, tableName);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"set_tbname failed,reason:{TDengine.StmtErrorStr(stmt)}");
            }
        }

        public static void SetTag(IntPtr stmt, TAOS_MULTI_BIND[] tags)
        {
            int res = TDengine.StmtSetTags(stmt, tags);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"set_tbname failed,reason:{TDengine.StmtErrorStr(stmt)}");
            }

        }
        public static void SetTableNameTags(IntPtr stmt, String tableName, TAOS_MULTI_BIND[] tags)
        {
            int res = TDengine.StmtSetTbnameTags(stmt, tableName, tags);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtSetTbnameTags failed,reason:{TDengine.StmtErrorStr(stmt)}.");
            }
        }

        public static void SetSubTableName(IntPtr stmt, string name)
        {
            int res = TDengine.StmtSetSubTbname(stmt, name);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"SetSubTableName failed, reason:{TDengine.StmtErrorStr(stmt)}");
            }
        }

        public static void BindSingleParamBatch(IntPtr stmt, TAOS_MULTI_BIND bind, int index)
        {
            int res = TDengine.StmtBindSingleParamBatch(stmt, ref bind, index);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtBindSingleParamBatch failed, reason:{TDengine.StmtErrorStr(stmt)}");
            }
        }

        public static void BindParamBatch(IntPtr stmt, TAOS_MULTI_BIND[] bind)
        {
            int res = TDengine.StmtBindParamBatch(stmt, bind);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtBindParamBatch failed, reason:{TDengine.StmtErrorStr(stmt)}");
            }
        }

        public static void AddBatch(IntPtr stmt)
        {
            int res = TDengine.StmtAddBatch(stmt);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtAddBatch failed,reason: reason:{TDengine.StmtErrorStr(stmt)}");
            }
        }
        public static void StmtExecute(IntPtr stmt)
        {
            int res = TDengine.StmtExecute(stmt);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtExecute failed,reason:{TDengine.StmtErrorStr(stmt)}");
            }
        }
        public static void StmtClose(IntPtr stmt)
        {
            int res = TDengine.StmtClose(stmt);
            if (res != 0)
            {
                throw new Exception($"StmtClose failed,reason:{TDengine.StmtErrorStr(stmt)}");
            }
        }

        public static IntPtr StmtUseResult(IntPtr stmt)
        {
            IntPtr res = TDengine.StmtUseResult(stmt);
            if (res == IntPtr.Zero)
            {
                StmtClose(stmt);
                throw new Exception($"StmtUseResult failed,reason:{TDengine.Error(res)} code:{(TDengine.ErrorNo(res) != 0)}");
            }
            return res;
        }

        public static void LoadTableInfo(IntPtr conn, string[] arr)
        {
            if (TDengine.LoadTableInfo(conn, arr) != 0)
            {
                throw new Exception("load table info failed");
            }
        }

    }
}