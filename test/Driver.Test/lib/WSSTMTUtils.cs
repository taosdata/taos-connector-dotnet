using System;
using System.Runtime.InteropServices;
using TDengineDriver;
using TDengineWS.Impl;

namespace Test.Utils
{
    internal class WSSTMTTools
    {
        public static IntPtr WSStmtInit(IntPtr wsConn)
        {
            IntPtr wsStmt = LibTaosWS.WSStmtInit(wsConn);
            if (wsStmt == IntPtr.Zero)
            {
                throw new Exception("init WSStmt failed");
            }
            return wsStmt;
        }

        public static void WSStmtPrepare(IntPtr wsStmt, string sql)
        {
            int code = LibTaosWS.WSStmtPrepare(wsStmt, sql);
            ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod()!.Name);
        }

        public static void WSStmtSetTbname(IntPtr wsStmt, string table)
        {
            int code = LibTaosWS.WSStmtSetTbname(wsStmt, table);
            ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod()!.Name);
        }

        public static void WSStmtSetTags(IntPtr wsStmt, TAOS_MULTI_BIND[] wsMBind, int len)
        {
            int code = LibTaosWS.WSStmtSetTags(wsStmt, wsMBind, len);
            ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod()!.Name);
        }

        public static void WSStmtSetTbnameTags(IntPtr wsStmt, string table, TAOS_MULTI_BIND[] wsMBinds, int len)
        {
            int code = LibTaosWS.WSStmtSetTbnameTags(wsStmt, table, wsMBinds, len);
            ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod()!.Name);
        }

        public static void WSStmtBindParamBatch(IntPtr wsStmt, TAOS_MULTI_BIND[] wsMBind, int len)
        {
            int code = LibTaosWS.WSStmtBindParamBatch(wsStmt, wsMBind, len);
            ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod()!.Name);
        }

        public static void WSStmtAddBatch(IntPtr wsStmt)
        {
            int code = LibTaosWS.WSStmtAddBatch(wsStmt);
            ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod()!.Name);
        }

        public static int WSStmtExecute(IntPtr wsStmt)
        {
            IntPtr affectRowsPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Int32)));
            try
            {
                int code = LibTaosWS.WSStmtExecute(wsStmt, affectRowsPtr);
                ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod()!.Name);
                return Marshal.ReadInt32(affectRowsPtr);
            }
            finally
            {
                Marshal.FreeHGlobal(affectRowsPtr);
            }

        }
        public static void WSFreeMBind(TAOS_MULTI_BIND[] mBinds)
        {
            WSMultiBind.WSFreeTaosBind(mBinds);
        }

        public static void WSCloseSTMT(IntPtr wsStmt)
        {
            LibTaosWS.WSStmtClose(wsStmt);
        }

        public static void ValidSTMTStep(int code, IntPtr wsStmt, string ? stmtMethodName)
        {
            if (code != 0)
            {
                throw new Exception($"{stmtMethodName} failed,reason:{LibTaosWS.WSStmtErrorStr(wsStmt)},code:{code}");
            }
        }


    }
}
