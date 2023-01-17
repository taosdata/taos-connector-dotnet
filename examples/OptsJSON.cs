using Sample.UtilsTools;
using TDengineDriver;
using System.Runtime.InteropServices;
using System;

namespace Examples.Schemaless
{
    internal class OptsJSON
    {
        string[] lines = { "[{\"metric\": \"meters\", \"timestamp\": 1648432611249, \"value\": 10.3, \"tags\": {\"location\": \"California.SanFrancisco\", \"groupid\": 2}}," +
                " {\"metric\": \"meters\", \"timestamp\": 1648432611249, \"value\": 219, \"tags\": {\"location\": \"California.LosAngeles\", \"groupid\": 1}}, " +
                "{\"metric\": \"meters\", \"timestamp\": 1648432611250, \"value\": 12.6, \"tags\": {\"location\": \"California.SanFrancisco\", \"groupid\": 2}}," +
                " {\"metric\": \"meters\", \"timestamp\": 1648432611250, \"value\": 221, \"tags\": {\"location\": \"California.LosAngeles\", \"groupid\": 1}}]"
            };
        string selectSql = "select * from meters";
        public void RunOptsJSON(IntPtr conn)
        {
            Console.WriteLine("RunOptsJSON ...");

            // OptsJSON protocol
            IntPtr res = TDengine.SchemalessInsert(conn, lines, 1, (int)TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL, (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception($"SchemalessInsert failed，reason:{TDengine.Error(res)}, code:{TDengine.ErrorNo(res)}");
            }
            else
            {
                int affectedRows = TDengine.AffectRows(res);
                Console.WriteLine($"SchemalessInsert success, affected {affectedRows} rows");
            }

            //free res
            TDengine.FreeResult(res);

            //check insert
            res = UtilsTools.ExecuteQuery(conn, selectSql);
            //check insert
            UtilsTools.DisplayRes(res);

            //free res
            TDengine.FreeResult(res);
        }
    }
}
