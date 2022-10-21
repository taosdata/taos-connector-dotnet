using System;
using Test.Utils;
using TDengineDriver;
using TDengineDriver.Impl;
using Xunit;
using System.Collections.Generic;
using Test.Utils.ResultSet;
using Test.Case.Attributes;
using Test.Fixture;
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
        /// <Name>SMLRaw.LineProtocal</Name>
        /// <describe>Insert data through TSDB_SML_LINE_PROTOCOL protocol.</describe>
        /// <filename>SMLRaw.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "SMLRaw.TSDB_SML_LINE_PROTOCOL"), TestExeOrder(1), Trait("Category", "SMLRaw")]
        public void LineProtocol()
        {
            IntPtr conn = database.Conn;
            string tableName = "sml_line_ms";

            string[] lines = { $"{tableName},id=pnnqhsa,t0=t,t1=127i8 c11=L\"ncharColValue\",c0=t,c1=127i8 1626006833639\n{tableName},id=pnnhsa,t0=t,t1=127i8 c11=L\"ncharColValue\",c0=t,c1=127i8 1626006833639\n#comment\n{tableName},id=pnqhsa,t0=t,t1=127i8 c11=L\"ncharColValue\",c0=t,c1=127i8 1626006833639", };

            int rows= TDengine.SchemalessInsertRaw(conn, lines, TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL, TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS);
            _output.WriteLine("insert rows={0}", rows);

            IntPtr res = Tools.ExecuteQuery(conn, $"select * from {tableName}",_output);

            var metas = LibTaos.GetMeta(res);
            var datas = LibTaos.GetData(res);

            metas.ForEach(meta =>
            {
                _output.WriteLine("name:{0},type:{1},size:{2}", meta.name, meta.type, meta.size);
            });

            datas.ForEach(data =>
            {
                _output.WriteLine("{0}|\t",data);
            });
            //_output.WriteLine("Assert meta");
            //expectResMeta.ForEach(meta =>
            //{
            //    Assert.Equal(meta.name, actualResMeta[expectResMeta.IndexOf(meta)].name);
            //    Assert.Equal(meta.type, actualResMeta[expectResMeta.IndexOf(meta)].type);
            //    Assert.Equal(meta.size, actualResMeta[expectResMeta.IndexOf(meta)].size);
            //});

            //_output.WriteLine("Assert data");
            //for (int i = 0; i < columns.Count; i++)
            //{
            //    //_output.WriteLine("{0},{1},{2}",i, columns[i], acutalResData[i]);
            //    Assert.Equal(columns[i], acutalResData[i]);
            //}
            Tools.FreeResult(res);
        }

        /// <author>xiaolei</author>
        /// <Name>SMLRaw.TelnetProtocal</Name>
        /// <describe>Insert data into normal table and query data.</describe>
        /// <filename>SMLRaw.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "SMLRaw.TSDB_SML_TELNET_PROTOCOL"), TestExeOrder(1), Trait("Category", "SMLRaw")]
        public void TelnetProtocol()
        {
            IntPtr conn = database.Conn;
            string tableName = "sml_telnet";
            string[] lines = { ""}

            //_output.WriteLine("Assert meta");
            //expectResMeta.ForEach(meta =>
            //{
            //    Assert.Equal(meta.name, actualResMeta[expectResMeta.IndexOf(meta)].name);
            //    Assert.Equal(meta.type, actualResMeta[expectResMeta.IndexOf(meta)].type);
            //    Assert.Equal(meta.size, actualResMeta[expectResMeta.IndexOf(meta)].size);
            //});

            //_output.WriteLine("Assert data");
            //for (int i = 0; i < columns.Count; i++)
            //{
            //    //_output.WriteLine("{0},{1},{2}",i, columns[i], acutalResData[i]);
            //    Assert.Equal(columns[i], acutalResData[i]);
            //}
            Tools.FreeResult(res);

        }

        /// <author>xiaolei</author>
        /// <Name>SMLRaw.JSONProtocal</Name>
        /// <describe>Insert data into normal table and query data.</describe>
        /// <filename>SMLRaw.cs</filename>
        /// <result>pass or failed </result> 
        //[Fact(DisplayName = "SMLRaw.TSDB_SML_JSON_PROTOCOL"), TestExeOrder(1), Trait("Category", "SMLRaw")]
        //public void JSONProtocol()
        //{
        //IntPtr conn = database.Conn;
        //string tableName = "query_tn";

        //_output.WriteLine("Assert meta");
        //expectResMeta.ForEach(meta =>
        //{
        //    Assert.Equal(meta.name, actualResMeta[expectResMeta.IndexOf(meta)].name);
        //    Assert.Equal(meta.type, actualResMeta[expectResMeta.IndexOf(meta)].type);
        //    Assert.Equal(meta.size, actualResMeta[expectResMeta.IndexOf(meta)].size);
        //});

        //_output.WriteLine("Assert data");
        //for (int i = 0; i < columns.Count; i++)
        //{
        //    //_output.WriteLine("{0},{1},{2}",i, columns[i], acutalResData[i]);
        //    Assert.Equal(columns[i], acutalResData[i]);
        //}
        //Tools.FreeResult(res);
        //    Assert.Equal(1, 1);
        //}

    }
}
