using System;
using System.Collections.Generic;
using TDengineDriver;
using TDengineDriver.Impl;
using Examples.UtilsTools;
using Examples.Data;

namespace Examples.AsyncQuery
{
    internal class QueryAsync
    {

        public void RunQueryAsync(IntPtr conn, string table)
        {
            string db = "query_a_db";
            InitData data = new InitData();
            data.Create(conn, db, table, true);
            data.InsertData(conn, db, table, "s_01", 10);

            QueryAsyncCallback queryAsyncCallback = new QueryAsyncCallback(QueryCallback);
            Console.WriteLine($"Start calling QueryAsync(),query {table}'s data asynchronously.");
            TDengine.QueryAsync(conn, $"select * from {table}", queryAsyncCallback, IntPtr.Zero);
            
            data.InsertData(conn, db, table, "s_02", 10);
            Thread.Sleep(100);
            data.InsertData(conn, db, table, "s_03", 10);
            Thread.Sleep(100);
            data.InsertData(conn, db, table, "s_04", 10);
     
            Thread.Sleep(2000);
            Console.WriteLine("QueryAsync done.");

            data.Drop(conn, db, null);
        }


        public void QueryCallback(IntPtr param, IntPtr taosRes, int code)
        {
            if (code == 0 && taosRes != IntPtr.Zero)
            {
                FetchRawBlockAsyncCallback fetchRowAsyncCallback = new FetchRawBlockAsyncCallback(FetchRawBlockCallback);
                TDengine.FetchRawBlockAsync(taosRes, fetchRowAsyncCallback, param);
            }
            else
            {
                Console.WriteLine($"async query data failed, failed code {code}");
            }
        }

        // Iteratively call this interface until "numOfRows" is no greater than 0.
        public void FetchRawBlockCallback(IntPtr param, IntPtr taosRes, int numOfRows)
        {
            if (numOfRows > 0)
            {
                Console.WriteLine($"{numOfRows} rows async retrieved");
                IntPtr pdata = TDengine.GetRawBlock(taosRes);
                List<TDengineMeta> metaList = TDengine.FetchFields(taosRes);
                LibTaos.ReadRawBlock(pdata,metaList,numOfRows);
                Tools.DisplayRes(taosRes);
                TDengine.FetchRawBlockAsync(taosRes, FetchRawBlockCallback, param);
            }
            else
            {
                if (numOfRows == 0)
                {
                    Console.WriteLine("async retrieve complete.");
                }
                else
                {
                    Console.WriteLine($"FetchRawBlockCallback callback error, error code {numOfRows}");
                }
                TDengine.FreeResult(taosRes);
            }
        }
    }
}
