using TDengine.Driver;
using System.Runtime.InteropServices;
using System;
using NativeMethods = TDengine.Driver.Impl.NativeMethods.NativeMethods;

namespace Test.Utils.Stmt
{
    public class StmtTools
    {
        public static IntPtr StmtInit(IntPtr conn)
        {
            IntPtr stmt = NativeMethods.StmtInit(conn);
            if (stmt == IntPtr.Zero)
            {
                throw new Exception("StmtInit failed.");
            }
            return stmt;
        }

        public static IntPtr StmtInit(IntPtr conn, long reqId)
        {
            IntPtr stmt = NativeMethods.StmtInitWithReqid(conn,reqId);
            if (stmt == IntPtr.Zero)
            {
                throw new Exception("StmtInit failed.");
            }
            return stmt;
        }

        public static void StmtPrepare(IntPtr stmt, string sql)
        {
            int res = NativeMethods.StmtPrepare(stmt, sql);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtPrepare failed,reason:${NativeMethods.StmtErrorStr(stmt)}");
            }
        }

        public static void SetTableName(IntPtr stmt, String tableName)
        {
            int res = NativeMethods.StmtSetTbname(stmt, tableName);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"set_tbname failed,reason:{NativeMethods.StmtErrorStr(stmt)}");
            }
        }

        public static void SetTag(IntPtr stmt, TAOS_MULTI_BIND[] tags)
        {
            int res = NativeMethods.StmtSetTags(stmt, tags);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"set_tbname failed,reason:{NativeMethods.StmtErrorStr(stmt)}");
            }

        }
        public static void SetTableNameTags(IntPtr stmt, String tableName, TAOS_MULTI_BIND[] tags)
        {
            int res = NativeMethods.StmtSetTbnameTags(stmt, tableName, tags);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtSetTbnameTags failed,reason:{NativeMethods.StmtErrorStr(stmt)}.");
            }
        }

        public static void SetSubTableName(IntPtr stmt, string name)
        {
            int res = NativeMethods.StmtSetSubTbname(stmt, name);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"SetSubTableName failed, reason:{NativeMethods.StmtErrorStr(stmt)}");
            }
        }

        public static void BindSingleParamBatch(IntPtr stmt, TAOS_MULTI_BIND bind, int index)
        {
            int res = NativeMethods.StmtBindSingleParamBatch(stmt, bind, index);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtBindSingleParamBatch failed, reason:{NativeMethods.StmtErrorStr(stmt)}");
            }
        }

        public static void BindParamBatch(IntPtr stmt, TAOS_MULTI_BIND[] bind)
        {
            int res = NativeMethods.StmtBindParamBatch(stmt, bind);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtBindParamBatch failed, reason:{NativeMethods.StmtErrorStr(stmt)}");
            }
        }

        public static void AddBatch(IntPtr stmt)
        {
            int res = NativeMethods.StmtAddBatch(stmt);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtAddBatch failed,reason: reason:{NativeMethods.StmtErrorStr(stmt)}");
            }
        }
        public static void StmtExecute(IntPtr stmt)
        {
            int res = NativeMethods.StmtExecute(stmt);
            if (res != 0)
            {
                StmtClose(stmt);
                throw new Exception($"StmtExecute failed,reason:{NativeMethods.StmtErrorStr(stmt)}");
            }
        }
        public static void StmtClose(IntPtr stmt)
        {
            int res = NativeMethods.StmtClose(stmt);
            if (res != 0)
            {
                throw new Exception($"StmtClose failed,reason:{NativeMethods.StmtErrorStr(stmt)}");
            }
        }

        public static IntPtr StmtUseResult(IntPtr stmt)
        {
            IntPtr res = NativeMethods.StmtUseResult(stmt);
            if (res == IntPtr.Zero)
            {
                StmtClose(stmt);
                throw new Exception($"StmtUseResult failed,reason:{NativeMethods.Error(res)} code:{(NativeMethods.ErrorNo(res) != 0)}");
            }
            return res;
        }

        public static void LoadTableInfo(IntPtr conn, string[] arr)
        {
            if (NativeMethods.LoadTableInfo(conn, arr) != 0)
            {
                throw new Exception("load table info failed");
            }
        }

    }
}