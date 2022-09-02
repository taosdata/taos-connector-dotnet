using System;
using TDengineWS.Impl;
using Test.Utils;

namespace Test.WSFixture
{
    public class WSDataBaseFixture : IDisposable
    {
        public IntPtr WSConn { get; set; }
        readonly string db = "ws_db_test";
        public WSDataBaseFixture()
        {
            string ip = "127.0.0.1";
            string? ENV_HOST = Environment.GetEnvironmentVariable("TEST_HOST");
            ip = string.IsNullOrEmpty(ENV_HOST) == true ? "127.0.0.1" : ENV_HOST;
            string dsn = $"ws://{ip}:6041";

            WSConn = LibTaosWS.WSConnectWithDSN(dsn);
            WSTools.WSExecuteUpdate(WSConn, $"create database if not exists {db}");

        }

        public void Dispose()
        {
            //WSTools.WSExecuteUpdate(WSConn, $"drop database if exists {db}");
            LibTaosWS.WSClose(WSConn);
        }

    }
}