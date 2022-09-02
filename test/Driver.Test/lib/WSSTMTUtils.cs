using System;
using System.Runtime.InteropServices;
using TDengineDriver;
using TDengineWS.Impl;

namespace Test.Utils
{
    internal class WSSTMTTools
    {
        public static nint WSStmtInit(nint wsConn)
        {
            nint wsStmt = LibTaosWS.WSStmtInit(wsConn);
            if (wsStmt == IntPtr.Zero)
            {
                throw new Exception("init WSStmt faield");
            }
            return wsStmt;
        }

        public static void WSStmtPrepare(nint wsStmt, string sql)
        {
            int code = LibTaosWS.WSStmtPrepare(wsStmt, sql);
            ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod().Name);
        }

        public static void WSStmtSetTbname(nint wsStmt, string table)
        {
            int code = LibTaosWS.WSStmtSetTbname(wsStmt, table);
            ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod().Name);
        }

        public static void WSStmtSetTags(nint wsStmt, TAOS_MULTI_BIND[] wsMBind, int len)
        {
            int code = LibTaosWS.WSStmtSetTags(wsStmt, wsMBind, len);
            ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod().Name);
        }

        public static void WSStmtSetTbnameTags(nint wsStmt, string table, TAOS_MULTI_BIND[] wsMBinds, int len)
        {
            int code = LibTaosWS.WSStmtSetTbnameTags(wsStmt, table, wsMBinds, len);
            ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod().Name);
        }

        public static void WSStmtBindParamBatch(nint wsStmt, TAOS_MULTI_BIND[] wsMBind, int len)
        {
            int code = LibTaosWS.WSStmtBindParamBatch(wsStmt, wsMBind, len);
            ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod().Name);
        }

        public static void WSStmtAddBatch(nint wsStmt)
        {
            int code = LibTaosWS.WSStmtAddBatch(wsStmt);
            ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod().Name);
        }

        public static int WSStmtExecute(nint wsStmt)
        {
            IntPtr affectRowsPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Int32)));
            try
            {
                int code = LibTaosWS.WSStmtExecute(wsStmt, affectRowsPtr);
                ValidSTMTStep(code, wsStmt, System.Reflection.MethodBase.GetCurrentMethod().Name);
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

        public static void WSCloseSTMT(nint wsStmt)
        {
            LibTaosWS.WSStmtClose(wsStmt);
        }

        public static void ValidSTMTStep(int code, nint wsStmt, string stmtMethodName)
        {
            if (code != 0)
            {
                throw new Exception($"{stmtMethodName} failed,reason:{LibTaosWS.WSStmtErrorStr(wsStmt)},code:{code}");
            }
        }


    }
}
