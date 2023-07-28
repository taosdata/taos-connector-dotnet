using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Runtime.InteropServices;
using TDengineDriver;

namespace Test.Fixture
{
    public class DatabaseFixture : IDisposable
    {
        public IntPtr Conn { get; set; }
        readonly string db = "csharp_test";
        public DatabaseFixture()
        {

            string user = "root";
            string password = "taosdata";
            string ip;
            short port = 0;


            TDengineDriver.TDengine.Options((int)TDengineInitOption.TSDB_OPTION_CONFIGDIR, GetConfigPath());
            TDengineDriver.TDengine.Options((int)TDengineInitOption.TSDB_OPTION_SHELL_ACTIVITY_TIMER, "90");
            TDengineDriver.TDengine.Options((int)TDengineInitOption.TSDB_OPTION_LOCALE, "C");
            TDengineDriver.TDengine.Options((int)TDengineInitOption.TSDB_OPTION_CHARSET, "UTF-8");
            string? ENV_HOST = Environment.GetEnvironmentVariable("TEST_HOST");
            ip = string.IsNullOrEmpty(ENV_HOST) == true ? "127.0.0.1" : ENV_HOST;
            this.Conn = TDengineDriver.TDengine.Connect(ip, user, password, "", port);
            IntPtr res;
            if (Conn != IntPtr.Zero)
            {
                if ((res = TDengineDriver.TDengine.Query(Conn, $"create database if not exists {db} keep 3650")) != IntPtr.Zero)
                {
                    if ((res = TDengineDriver.TDengine.Query(Conn, $"use {db}")) != IntPtr.Zero)
                    {
                        Console.WriteLine("Get connection success");
                    }
                    else
                    {
                        throw new Exception(TDengineDriver.TDengine.Error(res));
                    }
                }
                else
                {
                    throw new Exception(TDengineDriver.TDengine.Error(res));
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
            //TDengineDriver.TDengine.Close(Conn);

            IntPtr res = IntPtr.Zero;
            if (Conn != IntPtr.Zero)
            {
                res = TDengineDriver.TDengine.Query(Conn, $"drop database if exists {db}");
                if (res != IntPtr.Zero)
                {
                    TDengineDriver.TDengine.Close(Conn);
                    Console.WriteLine("close connection success");

                }
                else
                {
                    TDengineDriver.TDengine.Close(Conn);
                    throw new Exception(TDengineDriver.TDengine.Error(res));
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
