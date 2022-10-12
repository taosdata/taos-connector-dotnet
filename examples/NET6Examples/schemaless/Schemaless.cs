using System;
using Examples.UtilsTools;

namespace Examples.Schemaless
{
    internal class SchemalessExample
    {
        InfuxDBLine Line { get; set; }
        OptsTelnet Telnet { get; set; }
        OptsJSON JSON { get; set; }
        
        public SchemalessExample()
        {
            this.Line = new InfuxDBLine();
            this.Telnet = new OptsTelnet();
            this.JSON = new OptsJSON();
        }

        public void RunSchemaless(IntPtr conn)
        {
            Tools.ExecuteUpdate(conn, "create database if not exists sml_db");
            Tools.ExecuteUpdate(conn, "use sml_db");

            this.Line.RunInfuxDBLine(conn);
            this.JSON.RunOptsJSON(conn);
            this.Telnet.RunOptsTelnet(conn);
            Tools.ExecuteUpdate(conn, "drop database if exists sml_db");

        }
    }
}
