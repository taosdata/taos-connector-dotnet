using System;
using System.Collections.Generic;
using TDengineDriver;
using TDengineDriver.Impl;

namespace FrameWork45.UtilTools
{
    public class Tools
    {

        static string ip = "127.0.0.1";
        static string user = "root";
        static string password = "taosdata";
        static string db = "";
        static short port = 0;
        //get a TDengine connection
        public static IntPtr TDConnection()
        {
            TDengine.Options((int)TDengineInitOption.TSDB_OPTION_CONFIGDIR, GetConfigPath());
            TDengine.Options((int)TDengineInitOption.TSDB_OPTION_SHELL_ACTIVITY_TIMER, "60");

            IntPtr conn = TDengine.Connect(ip, user, password, db, port);

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
            IntPtr res = TDengine.Query(conn, sql);
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
            IntPtr res = TDengine.Query(conn, sql);
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
            IntPtr res = TDengine.Query(conn, sql);
            if (!IsValidResult(res))
            {
                Console.Write(sql.ToString() + " failure, ");
                ExitProgram();
            }
            else
            {
                Console.WriteLine(sql.ToString() + " success");

            }
            TDengine.FreeResult(res);
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

            List<TDengineMeta> resMeta = LibTaos.GetMeta(res);
            List<Object> resData = LibTaos.GetData(res);
            Dictionary<List<TDengineMeta>, List<Object>> result = new Dictionary<List<TDengineMeta>, List<Object>>(1);
            result.Add(resMeta, resData);
            return result;
        }

        public static bool IsValidResult(IntPtr res)
        {
            if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
            {
                if (res != IntPtr.Zero)
                {
                    Console.Write("reason: " + TDengine.Error(res));
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
                TDengine.Close(conn);
                Console.WriteLine("close connection success");
            }
            else
            {
                throw new Exception("connection if already null");
            }
        }
        public static List<TDengineMeta> GetResField(IntPtr res)
        {
            List<TDengineMeta> meta = TDengine.FetchFields(res);
            return meta;
        }

        // Only for exceptional exit.
        public static void ExitProgram()
        {
            TDengine.Cleanup();
            System.Environment.Exit(1);
        }

        public static void FreeTaosRes(IntPtr taosRes)
        {
            TDengine.FreeResult(taosRes);
        }

    }
}

