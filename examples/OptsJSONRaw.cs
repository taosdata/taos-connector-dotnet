using Sample.UtilsTools;
using TDengineDriver;
using System.Runtime.InteropServices;
using System;

namespace Examples.SchemalessRaw
{
    internal class OptsJSONRaw
    {
        const string table = "sml_json_raw";
        string[] lines = { "[{\"metric\": \"sml_json_raw\", \"timestamp\": 1648432611249, \"value\": 10.3, \"tags\": {\"location\": \"California.SanFrancisco\", \"groupid\": 2}}," +
                " {\"metric\": \"sml_json_raw\", \"timestamp\": 1648432611249, \"value\": 219, \"tags\": {\"location\": \"Ca0lifornia.LosAngeles\", \"groupid\": 1}}, " +
                "{\"metric\": \"sml_json_raw\", \"timestamp\": 1648432611250, \"value\": 12.6, \"tags\": {\"location\": \"California.SanFrancisco\", \"groupid\": 2}}," +
                " {\"metric\": \"sml_json_raw\", \"timestamp\": 1648432611251, \"value\": 220, \"tags\": {\"location\": \"北京.朝阳\", \"groupid\": 3}},"+
                " {\"metric\": \"sml_json_raw\", \"timestamp\": 1648432611252, \"value\": 220, \"tags\": {\"location\": \"北京.顺义\", \"groupid\": 3}}]"
            };
        string selectSql = $"select * from {table}";
        public void RunOptsJSONRaw(IntPtr conn)
        {
            Console.WriteLine("RunOptsJSONRaw ...");

            // OptsJSON protocol
            int rows = TDengine.SchemalessInsertRaw(conn, lines, TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL, TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED);

            Console.WriteLine("SchemalessInsertRaw using TSDB_SML_JSON_PROTOCOL insert {0} rows...", rows);

            //check insert
            IntPtr res = UtilsTools.ExecuteQuery(conn, selectSql);
            //check insert
            UtilsTools.DisplayRes(res);

            //free res
            TDengine.FreeResult(res);
        }
    }
}
