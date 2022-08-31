using System;
using System.Collections.Generic;
using TDengineDriver;
using TDengineWS.Impl;
using Test.Utils.ResultSet;

namespace Test.Utils
{
    internal class WSTools
    {
        readonly string dsn = "ws://127.0.0.1:6041/rest/sql/";
        public nint WSConnect(string dsn)
        {
            nint wsConn = LibTaosWS.WSConnectWithDSN(dsn);
            ValidWSconnect(wsConn);
            return wsConn;
        }

        public static ResultSet.ResultSet WSExecuteQuery(nint wsConn, string sql)
        {
            nint wsRes;
            ResultSet.ResultSet resultSet;


            wsRes = LibTaosWS.WSQuery(wsConn, sql);
            ValidWSQuery(wsRes);
            
            List<object> result = LibTaosWS.WSGetData(wsRes);
            List<TDengineMeta> metas = LibTaosWS.WSGetFields(wsRes);
            resultSet = new ResultSet.ResultSet(metas,result);

            FreeWSResult(wsRes);
            return resultSet;

        }

        public static void WSExecuteUpdate(nint wsConn, string sql)
        {
            nint wsRes = IntPtr.Zero;
            try
            {
                wsRes = LibTaosWS.WSQuery(wsConn, sql);
                ValidWSQuery(wsRes);
            }
            finally
            {
                FreeWSResult(wsRes);
            }
        }

        public void CloseWSConnect(nint wsConn)
        {
            LibTaosWS.WSClose(wsConn);
        }

        public static void FreeWSResult(nint wsRes)
        {
            LibTaosWS.WSFreeResult(wsRes);
        }

        public static void ValidWSQuery(nint wsRes)
        {
            int code = LibTaosWS.WSErrorNo(wsRes);
            if (code != 0)
            {
                throw new Exception($"WSQuery failed,reason:{LibTaosWS.WSErrorStr(wsRes)},code:{LibTaosWS.WSErrorNo(wsRes)}");
            }
        }

        public static void ValidWSconnect(nint wsConn)
        {
            if (wsConn == IntPtr.Zero)
            {
                throw new Exception($"Get WS connect failed,reason:{LibTaosWS.WSErrorStr(wsConn)},code:{LibTaosWS.WSErrorNo(wsConn)}");
            }
        }
    }
}
