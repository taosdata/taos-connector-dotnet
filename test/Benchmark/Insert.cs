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
        readonly int recordNum = 1;
        readonly int loopTime = 1;
        readonly long begineTime = 1659283200000;

        int _numOfThreadsNotYetCompleted = 1;
        ManualResetEvent _doneEvent = new ManualResetEvent(false);

        public Insert(string host, string userName, string passwd, short port, int maxSqlLength)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
            MaxSqlLength = maxSqlLength;
        }
        public void Run(string types, int tableCnt)
        {
            IntPtr conn = TDengine.Connect(Host, Username, Password, db, Port);
            IntPtr res;

            _numOfThreadsNotYetCompleted = tableCnt;
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
            Console.WriteLine("======TDengine.Close(conn);");
        }

        public void InsertLoop(IntPtr conn, int tableCnt, int recordCnt, string prefix, int times)
        {

            _numOfThreadsNotYetCompleted = tableCnt;
            for (int i = 0; i <tableCnt; i++)
            {
                RunContext context = new RunContext($"{prefix}_{i}", recordCnt, tableCnt, conn);
                ThreadPool.QueueUserWorkItem(RunInsertSQL, context);
            }
            _doneEvent.WaitOne();
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

        public void RunInsertSQL(object status)
        {
            RunContext context = (RunContext)status;

            try
            {
                string sql = $"insert into {context.tableName} values({begineTime},true,-1,-2,-3,-4,1,2,3,4,3.1415,3.14159265358979,'bnr_col_1','ncr_col_1')";
                // Console.WriteLine("sql:{0}", sql);
                IntPtr res = TDengine.Query(context.conn, sql);
                IfTaosQuerySucc(res, sql);
                TDengine.FreeResult(res);
            }
            finally
            {

                if (Interlocked.Decrement(ref _numOfThreadsNotYetCompleted) == 0)
                    _doneEvent.Set();
            }

        }
    }

    public struct RunManualRestContext
    {
        public RunContext RunContext { get; set; }
        public ManualResetEvent ManualReset { get; set; }

        public RunManualRestContext(RunContext runContext, ManualResetEvent manualResetEvent)
        {
            RunContext = runContext;
            ManualReset = manualResetEvent;
        }
    }
}
