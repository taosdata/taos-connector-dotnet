using System;
using Examples.SchemalessRaw;
using Examples.UtilsTools;

namespace Examples.SchemalessRaw
{
    internal class SchemalessRawExample
    {
        InfluxDBLineRaw Line { get; set; }
        OptsJSONRaw optsJSONRaw { get; set; }
        OptsTelnetRaw optsTelnetRaw { get; set; }

        const string DB = "sml_raw_db";

        public SchemalessRawExample()
        {
            this.Line = new InfluxDBLineRaw();
            this.optsJSONRaw = new OptsJSONRaw();
            this.optsTelnetRaw = new OptsTelnetRaw();
        }

        public void RunSchemalessRaw(IntPtr conn)
        {
            Tools.ExecuteUpdate(conn, $"create database if not exists {DB} keep 3650");
            Tools.ExecuteUpdate(conn, $"use {DB}");

            this.Line.RunInfluxDBLineRaw(conn);
            this.optsJSONRaw.RunOptsJSONRaw(conn);
            this.optsTelnetRaw.RunOptsTelnetRaw(conn);
            Tools.ExecuteUpdate(conn, $"drop database if exists {DB}");

        }
    }
}
