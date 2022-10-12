using System;
using TDengineWS.Impl;

namespace FrameWork45.WS
{
    internal class CloudExample
    {
        public IntPtr Connect(string dsn)
        {

            IntPtr conn = LibTaosWS.WSConnectWithDSN(dsn);
            if (conn == IntPtr.Zero)
            {
                throw new Exception($"get connection failed,reason:{LibTaosWS.WSErrorStr(conn)},code:{LibTaosWS.WSErrorNo(conn)}");
            }
            return conn;
        }

    }
}
