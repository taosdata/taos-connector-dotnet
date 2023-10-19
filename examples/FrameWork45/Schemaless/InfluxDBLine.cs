using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using FrameWork45.UtilTools;
using TDengine.Driver;
using TDengine.Driver.Impl.NativeMethods;

namespace FrameWork45.Schemaless
{
    internal class InfuxDBLine
    {
        string[] lines =  {
                "influx_line,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
                "influx_line,location=California.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
                "influx_line,location=California.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
                "influx_line,location=California.LosAngeles,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250"
            };
        string selectSql = "select * from influx_line";


        public void RunInfuxDBLine(IntPtr conn)
        {
            Console.WriteLine("RunInfuxDBLine ...");

            // InfluxdbLine protocol
            IntPtr res = NativeMethods.SchemalessInsert(conn, lines, lines.Length, (int)TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL, (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS);
            if (NativeMethods.ErrorNo(res) != 0)
            {
                throw new Exception($"SchemalessInsert failed，reason:{NativeMethods.Error(res)}, code:{NativeMethods.ErrorNo(res)}");
            }
            else
            {
                int affectedRows = NativeMethods.AffectRows(res);
                Console.WriteLine($"SchemalessInsert success, affected {affectedRows} rows");
            }

            //free res
            NativeMethods.FreeResult(res);

            //check insert

            res = Tools.ExecuteQuery(conn, selectSql);
            List<TDengineMeta> metaList = NativeMethods.FetchFields(res);
            List<Object> dataList = GetData(res);

            metaList.ForEach(meta =>
            {
                Console.Write("{0} {1}({2}) \t|", meta.name, meta.TypeName(), meta.size);
            });
            Console.WriteLine("");

            for (int i = 0; i < dataList.Count; i++)
            {
                if (i > 0 && (i + 1) % metaList.Count == 0)
                {
                    Console.WriteLine("{0}\t|", dataList[i]);
                }
                else
                {
                    Console.Write("{0}\t|", dataList[i]);
                }
            }

            NativeMethods.FreeResult(res);

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
