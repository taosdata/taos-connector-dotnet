
using System;
using System.Text;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Collections;
using Examples.UtilsTools;
using TDengineDriver;

namespace Examples
{
    class SchemalessSample
    {

        private IntPtr conn = IntPtr.Zero;
        private string dbName = "csharp_schemaless_example";
        public void RunSchemaless()
        {
            string[] lines = {
                "stg,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "stg,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833641000000"
            };
            string[] jsonStr = {
                "{"
                   +"\"metric\": \"stb0_0\","
                   +"\"timestamp\": 1626006833,"
                   +"\"value\": 10,"
                   +"\"tags\": {"
                       +" \"t1\": true,"
                       +"\"t2\": false,"
                       +"\"t3\": 10,"
                       +"\"t4\": \"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>\""
                    +"}"
                +"}"
            };
            StringBuilder querySql = new StringBuilder();
            Console.WriteLine(querySql.ToString());
            this.conn = Tools.TDConnection();

            schemalessInsert(lines, 2, (int)TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL, (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NANO_SECONDS);
            querySql.Append("select * from ").Append(this.dbName).Append(".").Append("stg");
            Tools.DisplayRes(Tools.ExecuteQuery(this.conn, querySql.ToString()));

            schemalessInsert(jsonStr, 1, (int)TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL, (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_SECONDS);
            querySql.Clear();
            querySql.Append("select * from ").Append(this.dbName).Append(".").Append("stb0_0");
            Tools.DisplayRes(Tools.ExecuteQuery(this.conn, querySql.ToString()));

            querySql.Clear();
            querySql.Append("drop database if exists ").Append(this.dbName);
            Tools.ExecuteUpdate(this.conn, querySql.ToString());
            Tools.CloseConnection(this.conn);

        }
        public void schemalessInsert(string[] sqlstr, int lineCnt, int protocol, int precision)
        {

            IntPtr res = TDengine.SchemalessInsert(this.conn, sqlstr, lineCnt, protocol, precision);

            if (TDengine.ErrorNo(res) != 0)
            {
                Console.WriteLine("schemaless_insert failed:{0}", TDengine.Error(res));
                Console.WriteLine("line string:{0}", sqlstr);
                Console.WriteLine("");
                System.Environment.Exit(0);
            }
            else
            {
                Console.WriteLine("else");
                Console.WriteLine("schemaless insert success:{0}", TDengine.ErrorNo(res));
            }

        }

    }
}
