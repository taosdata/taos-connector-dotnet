using System;
using System.Collections.Generic;
using TDengineDriver.Impl;
using Examples.UtilsTools;
using Examples.Data;

namespace Examples
{
    internal static class Query
    {
        public static void QueryData(IntPtr conn, string db, string? stable, string? table, int numOfRows)
        {
            InitData data = new InitData();
            string tmp = String.IsNullOrEmpty(stable) ? table : stable;
            data.Create(conn, db, tmp, string.IsNullOrEmpty(stable));
            data.InsertData(conn, db, stable, table, numOfRows);
            IntPtr res = Tools.ExecuteQuery(conn, $"select * from {tmp} ");
            List<Object> resData = LibTaos.GetData(res);
            List<TDengineDriver.TDengineMeta> resMeta = LibTaos.GetMeta(res);

            foreach (var meta in resMeta)
            {
                Console.Write($"{meta.name} {meta.TypeName} ({meta.size}) \n");
            }

            for (int i = 0; i < resData.Count; i++)
            {
                Console.Write($"{resData[i]}\t|");
                if ((i + 1) % resMeta.Count == 0)
                {
                    Console.WriteLine("");
                }
            }
            Tools.FreeTaosRes(res);
            data.Drop(conn, db, null);
        }
    }
}
