using System;
using TDengineDriver;
using System.Threading;

namespace Benchmark
{
    internal class Insert
    {
        string Host { get; set; }
        short Port { get; set; }
        string Username { get; set; }
        string Password { get; set; }
        readonly string db = "benchmark";
        readonly string stb = "stb";
        readonly string jtb = "jtb";
        int MaxSqlLength = 5000;

        public Insert(string host, string userName, string passwd, short port, int maxSqlLength)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
            MaxSqlLength = maxSqlLength;
        }
        public void Run(string types, int recordNum, int tableCnt, int loopTime)
        {
            // Console.WriteLine("Insert {0} ... ", types);
            IntPtr conn = TDengine.Connect(Host, Username, Password, db, Port);
            IntPtr res;
            if (conn != IntPtr.Zero)
            {
                res = TDengine.Query(conn, $"use {db}");
                IfTaosQuerySucc(res, $"use {db}");
                TDengine.FreeResult(res);

                if (types == "normal")
                {
                    InsertLoop(conn, tableCnt, recordNum, stb, loopTime);
                }
                if (types == "json")
                {
                    InsertLoop(conn, tableCnt, recordNum, jtb, loopTime);
                }
            }
            else
            {
                throw new Exception("create TD connection failed");
            }
            TDengine.Close(conn);
        }

        public void InsertLoop(IntPtr conn, int tableCnt, int recordCnt, string prefix, int times)
        {
            // IntPtr res;
            // int i = 0;
            // while (i < times)
            // {
            //     res = TDengine.Query(conn, sql);
            //     IfTaosQuerySucc(res, sql);
            //     TDengine.FreeResult(res);
            //     i++;
            // }
            // Console.WriteLine("last time:{0}", i);
            int j = 0;
            while (j < times)
            {
                for (int i = 0; i < tableCnt; i++)
                {
                    RunContext context = new RunContext($"prefix_{i}", recordCnt, conn);
                    InsertGenerator generator = new InsertGenerator(1659283200000, MaxSqlLength);
                    Console.WriteLine(context.ToString());
                    ThreadPool.QueueUserWorkItem(generator.BuildSql, context);
                }
                j++;
            }



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
