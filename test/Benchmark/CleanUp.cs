using System;
using TDengineDriver;

namespace Benchmark
{
    internal class CleanUp
    {
        string Host { get; set; }
        short Port { get; set; }
        string Username { get; set; }
        string Password { get; set; }
        string dropDb = $"drop database if exists benchmark";

        public CleanUp(string host, string userName, string passwd, short port)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
        }
        public void Run()
        {
            //Console.WriteLine("cleanup ...");

            IntPtr conn = TDengine.Connect(Host, Username, Password, "", Port);
            IntPtr res;
            if (conn != IntPtr.Zero)
            {
                res = TDengine.Query(conn, dropDb);
                IfTaosQuerySucc(res, dropDb);
                TDengine.FreeResult(res);
            }
            else
            {
                throw new Exception("create TD connection failed");
            }
            TDengine.Close(conn);
        }

        public bool IfTaosQuerySucc(IntPtr res, string sql)
        {
            if (TDengine.ErrorNo(res) == 0)
            {
                return true;
            }
            else
            {
                throw new Exception($"execute {sql} failed,reason {TDengine.Error(res)}, code{TDengine.ErrorNo(res)}");
            }
        }
    }
}
