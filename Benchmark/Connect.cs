using System;
using TDengineDriver;

namespace Benchmark
{
    internal class Connect
    {
        string Host { get; set; }
        short Port { get; set; }
        string Username { get; set; }
        string Password { get; set; }
        string db = "benchmark";

        public Connect(string host, string userName, string passwd, short port)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
        }
        public void Run(string type)
        {
            Console.WriteLine("Connection ... ",type);
            IntPtr conn = TDengine.Connect(Host,Username,Password,"",Port);
            if (conn != IntPtr.Zero)
            {
                TDengine.Close(conn);
            }
            else
            {
                throw new Exception("create TD connection failed");
            }
        }
    }
}
