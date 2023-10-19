using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using FrameWork45.UtilTools;
using FrameWork45.Data;
using TDengine.Driver;
using TDengine.Driver.Impl.NativeMethods;

namespace FrameWork45.Examples
{
    internal static class Query
    {
        public static void QueryData(IntPtr conn, string db, string stable, string table, int numOfRows)
        {
            InitData data = new InitData();
            string tmp = String.IsNullOrEmpty(stable) ? table : stable;
            data.Create(conn, db, tmp, !string.IsNullOrEmpty(stable));
            data.InsertData(conn, db, stable, table, numOfRows);
            IntPtr res = Tools.ExecuteQuery(conn, $"select * from {tmp} ");
            // IntPtr res = Tools.ExecuteQuery(conn, $"select * from benchmark.stb limit 10 ");
            List<TDengineMeta> resMeta = NativeMethods.FetchFields(res);
            List<Object> resData = GetData(res);

            foreach (var meta in resMeta)
            {
                Console.Write($"\t|{meta.name} {meta.TypeName()} ({meta.size})\t|");
            }

            Console.WriteLine("");

            for (int i = 0; i < resData.Count; i++)
            {
                Console.Write($"|{resData[i].ToString()} \t");
                //Console.WriteLine("{0},{1},{2}", i, resMeta.Count, (i) % resMeta.Count);
                if (((i + 1) % resMeta.Count == 0))
                {
                    Console.WriteLine("");
                }
            }

            Console.WriteLine("");

            Tools.FreeTaosRes(res);
            data.Drop(conn, db, null);
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

        public static List<object> GetData(IntPtr taosRes)
        {
            List<TDengineMeta> metaList = NativeMethods.FetchFields(taosRes);
            List<Object> list = new List<object>();

            IntPtr numOfRowsPrt = Marshal.AllocHGlobal(sizeof(Int32));
            IntPtr pDataPtr = Marshal.AllocHGlobal(IntPtr.Size);
            IntPtr pData;
            try
            {
                byte[] colType = new byte[metaList.Count];
                for (int i = 0; i < metaList.Count; i++)
                {
                    colType[i] = metaList[i].type;
                }

                while (true)
                {
                    int code = NativeMethods.FetchRawBlock(taosRes, numOfRowsPrt, pDataPtr);
                    if (code != 0)
                    {
                        throw new Exception(
                            $"fetch_raw_block failed,code {code} reason:{NativeMethods.Error(taosRes)}");
                    }

                    int numOfRows = Marshal.ReadInt32(numOfRowsPrt);
                    if (numOfRows == 0)
                    {
                        break;
                    }

                    pData = Marshal.ReadIntPtr(pDataPtr);
                    list.AddRange(ReadRawBlock(pData, metaList, numOfRows));
                }

                return list;
            }
            finally
            {
                Marshal.FreeHGlobal(numOfRowsPrt);
                Marshal.FreeHGlobal(pDataPtr);
            }
        }
    }
}