using Examples.UtilsTools;
using TDengine.Driver;
using TDengine.Driver.Impl.NativeMethods;

namespace Examples.Schemaless
{
    internal class OptsTelnet
    {
        string[] lines =
        {
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

            IntPtr res = NativeMethods.SchemalessInsert(conn, lines, lines.Length,
                (int)TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL,
                (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
            if (NativeMethods.ErrorNo(res) != 0)
            {
                throw new Exception(
                    $"SchemalessInsert failed，reason:{NativeMethods.Error(res)}, code:{NativeMethods.ErrorNo(res)}");
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
            List<Object> dataList = NativeMethods.GetData(res);

            metaList.ForEach(meta => { Console.Write("{0} {1}({2}) \t|", meta.name, meta.TypeName(), meta.size); });
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
    }
}