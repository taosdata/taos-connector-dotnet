using Sample.UtilsTools;
using System;
using TDengineDriver;

namespace Examples.SchemalessRaw
{
    internal class InfluxDBLineRaw
    {
        const string table = "sml_line_raw";
        string[] lines =  {
                $"{table},location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
                $"{table},location=Ca\0l0ifornia.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
                $"{table},location=Ca\\0lifornia.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
                $"{table},location=北京\0.朝阳,groupid=3 current=11.0,voltage=220,phase=0.36 1648432611251",
                $"{table},location=北京.顺义,groupid=3 current=11.1,voltage=220,phase=0.35 1648432611252"
            };
        string selectSql = $"select * from {table}";


        public void RunInfluxDBLineRaw(IntPtr conn)
        {
            Console.WriteLine("RunInfluxDBLineRaw ...");

            // InfluxdbLine protocol
            int rows = TDengine.SchemalessInsertRaw(conn, lines, TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL, TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS);

            Console.WriteLine("SchemalessInsertRaw using InfluxDBLine insert {0} rows...", rows);
            //check insert

            IntPtr res = UtilsTools.ExecuteQuery(conn, selectSql);
            //check insert
            UtilsTools.DisplayRes(res);

            //free res
            TDengine.FreeResult(res);

        }
    }
}
