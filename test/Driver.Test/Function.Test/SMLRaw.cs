using System;
using TDengineDriver;
using Test.Case.Attributes;
using Test.Fixture;
using Xunit;
using Xunit.Abstractions;


namespace Function.Test.SML
{
    [TestCaseOrderer("XUnit.Case.Orderers.TestExeOrderer", "Cases.ExeOrder")]
    [Collection("Database collection")]
    public class SMLRaw
    {
        readonly DatabaseFixture database;
        private readonly ITestOutputHelper _output;
        public SMLRaw(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this.database = fixture;
            this._output = output;
        }

        /// <author>xiaolei</author>
        /// <Name>SMLRaw.LineProtocol</Name>
        /// <describe>Insert data through TSDB_SML_LINE_PROTOCOL protocol.</describe>
        /// <filename>SMLRaw.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "SMLRaw.TSDB_SML_LINE_PROTOCOL"), TestExeOrder(1), Trait("Category", "SMLRaw")]
        public void LineProtocol()
        {
            IntPtr conn = database.Conn;
            string table = "sml_line_ms";

            string[] lines = {
                $"{table},location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
                $"{table},location=Ca\0l0ifornia.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
                $"{table},location=Ca\\0lifornia.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
                $"{table},location=北京\0.朝阳,groupid=3 current=11.0,voltage=220,phase=0.36 1648432611251",
                $"{table},location=北京.顺义,groupid=3 current=11.1,voltage=220,phase=0.35 1648432611252"
            };

            int rows = TDengine.SchemalessInsertRaw(conn, lines, TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL, TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS);
            Assert.Equal(5, rows);

        }

        /// <author>xiaolei</author>
        /// <Name>SMLRaw.TelnetProtocol</Name>
        /// <describe>Insert data through TSDB_SML_TELNET_PROTOCOL protocol.</describe>
        /// <filename>SMLRaw.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "SMLRaw.TSDB_SML_TELNET_PROTOCOL"), TestExeOrder(1), Trait("Category", "SMLRaw")]
        public void TelnetProtocol()
        {
            IntPtr conn = database.Conn;
            string table = "sml_telnet_raw";
            string[] lines = {
                $"{table} 1648432611249 10.3 location=Ca\0lifornia.SanFrancisco groupid=2",
                $"{table} 1648432611250 12.6 location=Ca\\0lifornia.SanFrancisco groupid=2",
                $"{table} 1648432611249 10.8 location=California.LosAngeles groupid=3",
                $"{table} 1648432611250 11.3 location=California.LosAngeles groupid=3",
                $"{table} 1648432611249 219 location=北京\0.朝阳 groupid=1",
                $"{table} 1648432611250 218 location=北京\\0.海淀 groupid=1",
                $"{table} 1648432611249 221 location=北京.顺义 groupid=4",
                $"{table} 1648432611250 217 location=北京.顺义 groupid=4",
            };
            int rows = TDengine.SchemalessInsertRaw(conn, lines, TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL, TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
            Assert.Equal(8, rows);
        }

        /// <author>xiaolei</author>
        /// <Name>SMLRaw.JSONProtocol</Name>
        /// <describe>Insert data TSDB_SML_JSON_PROTOCOL protocol.</describe>
        /// <filename>SMLRaw.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "SMLRaw.TSDB_SML_JSON_PROTOCOL"), TestExeOrder(1), Trait("Category", "SMLRaw")]
        public void JSONProtocol()
        {
            IntPtr conn = database.Conn;
            string[] lines = {
                 "[{\"metric\": \"sml_raw_telnet\", \"timestamp\": 1648432611249, \"value\": 10.3, \"tags\": {\"location\": \"California.SanFrancisco\", \"groupid\": 2}}," +
                " {\"metric\": \"sml_raw_telnet\", \"timestamp\": 1648432611249, \"value\": 219, \"tags\": {\"location\": \"Ca0lifornia.LosAngeles\", \"groupid\": 1}}, " +
                "{\"metric\": \"sml_raw_telnet\", \"timestamp\": 1648432611250, \"value\": 12.6, \"tags\": {\"location\": \"California.SanFrancisco\", \"groupid\": 2}}," +
                " {\"metric\": \"sml_raw_telnet\", \"timestamp\": 1648432611251, \"value\": 220, \"tags\": {\"location\": \"北京.朝阳\", \"groupid\": 3}},"+
                " {\"metric\": \"sml_raw_telnet\", \"timestamp\": 1648432611252, \"value\": 220, \"tags\": {\"location\": \"北京.顺义\", \"groupid\": 3}}]"
            };
            int rows = TDengine.SchemalessInsertRaw(conn, lines, TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL, TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
            Assert.Equal(1, rows);
        }

    }
}
