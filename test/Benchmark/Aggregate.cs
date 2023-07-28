using System;
using TDengineDriver;
using TDengineDriver.Impl;

namespace Benchmark
{
    internal class Aggregate
    {
        string Host { get; set; }
        short Port { get; set; }
        string Username { get; set; }
        string Password { get; set; }
        readonly string db = "benchmark";
        readonly string avgStb = "select avg(d64) from stb;";
        readonly string avgJtb = "select avg(d64) from jtb;";


        public Aggregate(string host, string userName, string passwd, short port)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
        }
        public void Run(string types, int times)
        {
            //Console.WriteLine("Aggregate {0} ...", types);

            IntPtr conn = TDengineDriver.TDengine.Connect(Host, Username, Password, db, Port);
            IntPtr res;
            if (conn != IntPtr.Zero)
            {
                res = TDengineDriver.TDengine.Query(conn, $"use {db}");
                IfTaosQuerySucc(res, $"use {db}");
                TDengineDriver.TDengine.FreeResult(res);

                if (types == "normal")
                {
                    AggregateLoop(conn, times, avgStb);
                }
                if (types == "json")
                {
                    AggregateLoop(conn, times, avgJtb);
                }
            }
            else
            {
                throw new Exception("create TD connection failed");
            }
            TDengineDriver.TDengine.Close(conn);

        }
        public void AggregateLoop(IntPtr conn, int times, string sql)
        {
            IntPtr res;
            int i = 0;
            while (i < times)
            {
                res = TDengineDriver.TDengine.Query(conn, sql);
                IfTaosQuerySucc(res, sql);
                LibTaos.GetMeta(res);
                LibTaos.GetData(res);
                TDengineDriver.TDengine.FreeResult(res);
                i++;
            }
            //Console.WriteLine("last time:{0}", i);
        }

        public bool IfTaosQuerySucc(IntPtr res, string sql)
        {
            if (TDengineDriver.TDengine.ErrorNo(res) == 0)
            {
                return true;
            }
            else
            {
                throw new Exception($"execute {sql} failed,reason {TDengineDriver.TDengine.Error(res)}, code{TDengineDriver.TDengine.ErrorNo(res)}");
            }
        }
    }
}
