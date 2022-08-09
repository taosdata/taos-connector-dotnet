using System;
using TDengineDriver;

namespace Benchmark
{
    internal class Insert
    {
        string Host { get; set; }
        short Port { get; set; }
        string Username { get; set; }
        string Password { get; set; }
        readonly string db = "benchmark";
        readonly string insertStb = "insert into stb_1 values(1659283200000" +
                                    ",true" +
                                    ",-1" +
                                    ",-2" +
                                    ",-3" +
                                    ",-4" +
                                    ",1" +
                                    ",2" +
                                    ",3" +
                                    ",4" +
                                    ",3.1415" +
                                    ",3.14159265358979" +
                                    ",'bnr_tag_1'" +
                                    ",'ncr_tag_1')";
        readonly string insertJtb = "insert into jtb_1 values(1659283200000" +
                                    ",true" +
                                    ",-1" +
                                    ",-2" +
                                    ",-3" +
                                    ",-4" +
                                    ",1" +
                                    ",2" +
                                    ",3" +
                                    ",4" +
                                    ",3.1415" +
                                    ",3.14159265358979" +
                                    ",'bnr_col_1'" +
                                    ",'ncr_col_1');";


        public Insert(string host, string userName, string passwd, short port)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
        }
        public void Run(string types, int times)
        {
            Console.WriteLine("Insert ... ", types);

            IntPtr conn = TDengine.Connect(Host, Username, Password, db, Port);
            IntPtr res;
            if (conn != IntPtr.Zero)
            {
                res = TDengine.Query(conn, $"use {db}");
                IfTaosQuerySucc(res, $"use {db}");
                TDengine.FreeResult(res);

                if (types == "normal")
                {
                    InsertLoop(conn, times, insertStb);
                }
                if (types == "json")
                {
                    InsertLoop(conn, times, insertJtb);
                }
            }
            else
            {
                throw new Exception("create TD connection failed");
            }
            TDengine.Close(conn);
        }

        public void InsertLoop(IntPtr conn,int times, string sql)
        {
            IntPtr res;
            int i = 0;
            while (i < times)
            {
                res = TDengine.Query(conn, sql) ;
                IfTaosQuerySucc(res, sql);
                TDengine.FreeResult(res);
                i++;
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
