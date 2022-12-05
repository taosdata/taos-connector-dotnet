using Sample.UtilsTools;
using System;
using TDengineDriver;

namespace Examples.Schemaless
{

    internal class OptsTelnet
    {
        string[] lines = {
                "meters_telnet 1648432611249 10.3 location=California.SanFrancisco groupid=2",
                "meters_telnet 1648432611250 12.6 location=California.SanFrancisco groupid=2",
                "meters_telnet 1648432611249 10.8 location=California.LosAngeles groupid=3",
                "meters_telnet 1648432611250 11.3 location=California.LosAngeles groupid=3",
                "meters_telnet 1648432611249 219 location=California.SanFrancisco groupid=2",
                "meters_telnet 1648432611250 218 location=California.SanFrancisco groupid=2",
                "meters_telnet 1648432611249 221 location=California.LosAngeles groupid=3",
                "meters_telnet 1648432611250 217 location=California.LosAngeles groupid=3",
            };
        string selectSql = "select * from meters_telnet";
        public void RunOptsTelnet(IntPtr conn)
        {
            Console.WriteLine("RunOptsTelnet ...");

            IntPtr res = TDengine.SchemalessInsert(conn, lines, lines.Length, (int)TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL, (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
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
