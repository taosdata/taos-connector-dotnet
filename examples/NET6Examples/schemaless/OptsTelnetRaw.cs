using Examples.UtilsTools;
using TDengine.Driver;
using TDengine.Driver.Impl.NativeMethods;

namespace Examples.SchemalessRaw
{
    internal class OptsTelnetRaw
    {
        const string table = "sml_telnet_raw";

        string[] lines =
        {
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

            int rows = NativeMethods.SchemalessInsertRaw(conn, lines,
                TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL,
                TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
            Console.WriteLine("SchemalessInsertRaw using TSDB_SML_TELNET_PROTOCOL insert {0} rows...", rows);

            //check insert

            IntPtr res = Tools.ExecuteQuery(conn, selectSql);
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