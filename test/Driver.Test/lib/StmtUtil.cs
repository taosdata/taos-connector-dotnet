using TDengineDriver;
using System.Runtime.InteropServices;
using System;

namespace Test.Utils.Stmt
{
    public class StmtTools
    {
        public static IntPtr StmtInit(IntPtr conn)
        {
            IntPtr stmt = TDengineDriver.TDengine.StmtInit(conn);
            if (stmt == IntPtr.Zero)
            {
                throw new Exception("StmtInit failed.");
            }
            return stmt;
        }

        public static void StmtPrepare(IntPtr stmt, string sql)
        {
            int res = TDengineDriver.TDengine.StmtPrepare(stmt, sql);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtPrepare failed,reason:${TDengineDriver.TDengine.StmtErrorStr(stmt)}");
            }
        }

        public static void SetTableName(IntPtr stmt, String tableName)
        {
            int res = TDengineDriver.TDengine.StmtSetTbname(stmt, tableName);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"set_tbname failed,reason:{TDengineDriver.TDengine.StmtErrorStr(stmt)}");
            }
        }

        public static void SetTag(IntPtr stmt, TAOS_MULTI_BIND[] tags)
        {
            int res = TDengineDriver.TDengine.StmtSetTags(stmt, tags);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"set_tbname failed,reason:{TDengineDriver.TDengine.StmtErrorStr(stmt)}");
            }

        }
        public static void SetTableNameTags(IntPtr stmt, String tableName, TAOS_MULTI_BIND[] tags)
        {
            int res = TDengineDriver.TDengine.StmtSetTbnameTags(stmt, tableName, tags);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtSetTbnameTags failed,reason:{TDengineDriver.TDengine.StmtErrorStr(stmt)}.");
            }
        }

        public static void SetSubTableName(IntPtr stmt, string name)
        {
            int res = TDengineDriver.TDengine.StmtSetSubTbname(stmt, name);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"SetSubTableName failed, reason:{TDengineDriver.TDengine.StmtErrorStr(stmt)}");
            }
        }

        public static void BindSingleParamBatch(IntPtr stmt, TAOS_MULTI_BIND bind, int index)
        {
            int res = TDengineDriver.TDengine.StmtBindSingleParamBatch(stmt, ref bind, index);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtBindSingleParamBatch failed, reason:{TDengineDriver.TDengine.StmtErrorStr(stmt)}");
            }
        }

        public static void BindParamBatch(IntPtr stmt, TAOS_MULTI_BIND[] bind)
        {
            int res = TDengineDriver.TDengine.StmtBindParamBatch(stmt, bind);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtBindParamBatch failed, reason:{TDengineDriver.TDengine.StmtErrorStr(stmt)}");
            }
        }

        public static void AddBatch(IntPtr stmt)
        {
            int res = TDengineDriver.TDengine.StmtAddBatch(stmt);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtAddBatch failed,reason: reason:{TDengineDriver.TDengine.StmtErrorStr(stmt)}");
            }
        }
        public static void StmtExecute(IntPtr stmt)
        {
            int res = TDengineDriver.TDengine.StmtExecute(stmt);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtExecute failed,reason:{TDengineDriver.TDengine.StmtErrorStr(stmt)}");
            }
        }
        public static void StmtClose(IntPtr stmt)
        {
            int res = TDengineDriver.TDengine.StmtClose(stmt);
            if (res != 0)
            {
                throw new Exception($"StmtClose failed,reason:{TDengineDriver.TDengine.StmtErrorStr(stmt)}");
            }
        }

        public static IntPtr StmtUseResult(IntPtr stmt)
        {
            IntPtr res = TDengineDriver.TDengine.StmtUseResult(stmt);
            if (res == IntPtr.Zero)
            {
                StmtClose(stmt);
                throw new Exception($"StmtUseResult failed,reason:{TDengineDriver.TDengine.Error(res)} code:{(TDengineDriver.TDengine.ErrorNo(res) != 0)}");
            }
            return res;
        }

        public static void LoadTableInfo(IntPtr conn, string[] arr)
        {
            if (TDengineDriver.TDengine.LoadTableInfo(conn, arr) != 0)
            {
                throw new Exception("load table info failed");
            }
        }

    }
}