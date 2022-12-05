using System;
using Sample.UtilsTools;

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
            UtilsTools.ExecuteUpdate(conn, $"create database if not exists {DB} keep 3650");
            UtilsTools.ExecuteUpdate(conn, $"use {DB}");

            this.Line.RunInfluxDBLineRaw(conn);
            this.optsJSONRaw.RunOptsJSONRaw(conn);
            this.optsTelnetRaw.RunOptsTelnetRaw(conn);
            UtilsTools.ExecuteUpdate(conn, $"drop database if exists {DB}");

        }
    }
}
