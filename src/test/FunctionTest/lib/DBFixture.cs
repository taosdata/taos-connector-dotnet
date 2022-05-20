using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Runtime.InteropServices;
using TDengineDriver;

namespace Test.Fixture
{
    public class DatabaseFixture : IDisposable
    {
        public IntPtr conn { get; set; }

        private string user = "root";
        private string password = "taosdata";
        private string ip = "my-ali-cloud";
        private short port = 0;

        private string db = "xunit_test_fixture";
        public DatabaseFixture()
        {
            TDengine.Options((int)TDengineInitOption.TSDB_OPTION_CONFIGDIR, GetConfigPath());
            TDengine.Options((int)TDengineInitOption.TSDB_OPTION_SHELL_ACTIVITY_TIMER, "90");
            TDengine.Options((int)TDengineInitOption.TSDB_OPTION_CHARSET, "UTF-8");
            TDengine.Init();
            conn = TDengine.Connect(ip, user, password, "", port);
            IntPtr res;
            if (conn != IntPtr.Zero)
            {
                if ((res = TDengine.Query(conn, $"create database if not exists {db} keep 3650")) != IntPtr.Zero)
                {
                    if ((res = TDengine.Query(conn, $"use {db}")) != IntPtr.Zero)
                    {
                        Console.WriteLine("Get connection success");
                    }
                    else
                    {
                        throw new Exception(TDengine.Error(res));
                    }
                }
                else
                {
                    throw new Exception(TDengine.Error(res));
                }
            }
            else
            {
                throw new Exception("Get TDConnection failed");
            }
        }

        // public IntPtr TDConnection { get;  }

        public void Dispose()
        {
            TDengine.Close(conn);

            IntPtr res;
            if (conn != IntPtr.Zero)
            {
                if ((res = TDengine.Query(conn, $"drop database if exists {db}")) != IntPtr.Zero)
                {
                    TDengine.Close(conn);
                    Console.WriteLine("close connection success");

                }
                else
                {
                    throw new Exception(TDengine.Error(res));
                }
            }
            else
            {
                throw new Exception("connection if already null");
            }

        }
        private string GetConfigPath()
        {
            string configDir = "";
            if (OperatingSystem.IsOSPlatform("Windows"))
            {
                configDir = "C:/TDengine/cfg";
            }
            else if (OperatingSystem.IsOSPlatform("Linux"))
            {
                configDir = "/etc/taos";
            }
            else if (OperatingSystem.IsOSPlatform("macOS"))
            {
                configDir = "/usr/local/etc/taos";
            }
            return configDir;
        }
    }
}
