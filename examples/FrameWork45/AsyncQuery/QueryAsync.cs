using System;
using System.Collections.Generic;
using FrameWork45.Data;
using System.Threading;
using TDengine.Driver;
using TDengine.Driver.Impl.NativeMethods;

namespace FrameWork45.AsyncQuery
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
            NativeMethods.QueryAsync(conn, $"select * from {table}", queryAsyncCallback, IntPtr.Zero);

            data.InsertData(conn, db, table, "s_02", 10);
            Thread.Sleep(100);
            data.InsertData(conn, db, table, "s_03", 10);
            Thread.Sleep(100);
            data.InsertData(conn, db, table, "s_04", 10);

            Thread.Sleep(3000);
            Console.WriteLine("QueryAsync done.");

            data.Drop(conn, db, null);
        }


        public void QueryCallback(IntPtr param, IntPtr taosRes, int code)
        {
            if (code == 0 && taosRes != IntPtr.Zero)
            {
                FetchRawBlockAsyncCallback fetchRowAsyncCallback = new FetchRawBlockAsyncCallback(FetchRawBlockCallback);
                NativeMethods.FetchRawBlockAsync(taosRes, fetchRowAsyncCallback, param);
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
                IntPtr pdata = NativeMethods.GetRawBlock(taosRes);
                List<TDengineMeta> metaList = NativeMethods.FetchFields(taosRes);
                List<object> dataList = ReadRawBlock(pdata, metaList, numOfRows);

                for (int i = 0; i < metaList.Count; i++)
                {
                    Console.Write("{0} {1}({2}) \t|", metaList[i].name, metaList[i].type, metaList[i].size);
                }
                Console.WriteLine();
                for (int i = 0; i < dataList.Count; i++)
                {
                    if (i != 0 && i % metaList.Count == 0)
                    {
                        Console.WriteLine("{0}\t|", dataList[i]);
                    }
                    Console.Write("{0}\t|", dataList[i]);
                }
                NativeMethods.FetchRawBlockAsync(taosRes, FetchRawBlockCallback, param);
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
                NativeMethods.FreeResult(taosRes);
            }
        }
        
        private static List<object> ReadRawBlock(IntPtr pData, List<TDengineMeta> metaList, int numOfRows)
        {
            var list = new List<object>(metaList.Count * numOfRows);
            byte[] colType = new byte[metaList.Count];
            for (int i = 0; i < metaList.Count; i++)
            {
                colType[i] = metaList[i].type;
            }

            var br = new BlockReader(0, metaList.Count, colType);
            br.SetBlockPtr(pData, numOfRows);
            for (int rowIndex = 0; rowIndex < numOfRows; rowIndex++)
            {
                for (int colIndex = 0; colIndex < metaList.Count; colIndex++)
                {
                    list.Add(br.Read(rowIndex, colIndex));
                }
            }

            return list;
        }
    }
}
