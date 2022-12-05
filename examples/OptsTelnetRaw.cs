using Sample.UtilsTools;
using System;
using TDengineDriver;

namespace Examples.SchemalessRaw
{
    internal class OptsTelnetRaw
    {
        const string table = "sml_telnet_raw";
        string[] lines = {
                $"{table} 1648432611249 10.3 location=Ca\0lifornia.SanFrancisco groupid=2",
                $"{table} 1648432611250 12.6 location=Ca\\0lifornia.SanFrancisco groupid=2",
                $"{table} 1648432611249 10.8 location=California.LosAngeles groupid=3",
                $"{table} 1648432611250 11.3 location=California.LosAngeles groupid=3",
                $"{table} 1648432611249 219 location=北京\0.朝阳 groupid=1",
                $"{table} 1648432611250 218 location=北京\\0.海淀 groupid=1",
                $"{table} 1648432611249 221 location=北京.顺义 groupid=4",
                $"{table} 1648432611250 217 location=北京.顺义 groupid=4",
            };
        string selectSql = $"select * from {table}";
        public void RunOptsTelnetRaw(IntPtr conn)
        {
            Console.WriteLine("RunOptsTelnetRaw ...");

            int rows = TDengine.SchemalessInsertRaw(conn, lines,TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL,TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
            Console.WriteLine("SchemalessInsertRaw using TSDB_SML_TELNET_PROTOCOL insert {0} rows...", rows);

            //check insert

            IntPtr res = UtilsTools.ExecuteQuery(conn, selectSql);
            //check insert
            UtilsTools.DisplayRes(res);

            //free res
            TDengine.FreeResult(res);
        }
    }
}
