using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using TDengine.Driver;
using TDengine.Driver.Impl.NativeMethods;

namespace FrameWork45.UtilTools
{
    public class Tools
    {

        static string ip = "127.0.0.1";
        static string user = "root";
        static string password = "taosdata";
        static string db = "";
        static ushort port = 0;
        //get a TDengine connection
        public static IntPtr TDConnection()
        {
            NativeMethods.Options((int)TDengineInitOption.TSDB_OPTION_CONFIGDIR, GetConfigPath());
            NativeMethods.Options((int)TDengineInitOption.TSDB_OPTION_SHELL_ACTIVITY_TIMER, "60");

            IntPtr conn = NativeMethods.Connect(ip, user, password, db, port);

            //Tools.ExecuteUpdate(conn, $"drop database if  exists {dbName}");
            //Tools.ExecuteUpdate(conn, $"create database if not exists {dbName} keep 3650");
            //Tools.ExecuteUpdate(conn, $"use {dbName}");

            return conn;
        }
        //get taos.cfg file based on different os
        public static string GetConfigPath()
        {
            return "C:/TDengine/cfg";
        }

        public static IntPtr ExecuteQuery(IntPtr conn, String sql)
        {
            IntPtr res = NativeMethods.Query(conn, sql);
            if (!IsValidResult(res))
            {
                Console.Write(sql.ToString() + " failure, ");
                ExitProgram();
            }
            else
            {
                Console.WriteLine(sql.ToString() + " success");
            }
            return res;
        }

        public static IntPtr ExecuteErrorQuery(IntPtr conn, String sql)
        {
            IntPtr res = NativeMethods.Query(conn, sql);
            if (!IsValidResult(res))
            {
                Console.Write(sql.ToString() + " failure, ");
                ExitProgram();
            }
            else
            {
                Console.WriteLine(sql.ToString() + " success");

            }
            return res;
        }

        public static void ExecuteUpdate(IntPtr conn, String sql)
        {
            IntPtr res = NativeMethods.Query(conn, sql);
            if (!IsValidResult(res))
            {
                Console.Write(sql.ToString() + " failure, ");
                ExitProgram();
            }
            else
            {
                Console.WriteLine(sql.ToString() + " success");

            }
            NativeMethods.FreeResult(res);
        }

        public static void DisplayRes(IntPtr res)
        {
            if (!IsValidResult(res))
            {
                ExitProgram();
            }

            Dictionary<List<TDengineMeta>, List<Object>> taosResult = GetResultSet(res);
            foreach (var kv in taosResult)
            {
                foreach (TDengineMeta k in kv.Key)
                {
                    Console.Write($"\t|{k.name} {k.TypeName()} ({k.size})\t|");
                }
                Console.WriteLine("");

                for (int i = 0; i < kv.Value.Count; i++)
                {

                    Console.Write($"|{kv.Value[i].ToString()} \t");
                    //Console.WriteLine("{0},{1},{2}", i, resMeta.Count, (i) % resMeta.Count);
                    if (((i + 1) % kv.Key.Count == 0))
                    {
                        Console.WriteLine("");
                    }
                }
                Console.WriteLine("");

            }


        }

        public static Dictionary<List<TDengineMeta>, List<Object>> GetResultSet(IntPtr res)
        {


            if (!IsValidResult(res))
            {
                ExitProgram();
            }

            List<TDengineMeta> resMeta = NativeMethods.FetchFields(res);
            List<Object> resData = GetData(res);
            Dictionary<List<TDengineMeta>, List<Object>> result = new Dictionary<List<TDengineMeta>, List<Object>>(1);
            result.Add(resMeta, resData);
            return result;
        }

        public static bool IsValidResult(IntPtr res)
        {
            if ((res == IntPtr.Zero) || (NativeMethods.ErrorNo(res) != 0))
            {
                if (res != IntPtr.Zero)
                {
                    Console.Write("reason: " + NativeMethods.Error(res));
                    return false;
                }
                Console.WriteLine("");
                return false;
            }
            return true;
        }
        public static void CloseConnection(IntPtr conn)
        {
            if (conn != IntPtr.Zero)
            {
                NativeMethods.Close(conn);
                Console.WriteLine("close connection success");
            }
            else
            {
                throw new Exception("connection if already null");
            }
        }
        public static List<TDengineMeta> GetResField(IntPtr res)
        {
            List<TDengineMeta> meta = NativeMethods.FetchFields(res);
            return meta;
        }

        // Only for exceptional exit.
        public static void ExitProgram()
        {
            NativeMethods.Cleanup();
            Environment.Exit(1);
        }

        public static void FreeTaosRes(IntPtr taosRes)
        {
            NativeMethods.FreeResult(taosRes);
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

