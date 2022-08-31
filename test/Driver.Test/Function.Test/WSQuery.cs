using System;
using System.Collections.Generic;
using TDengineDriver;
using Test.Case.Attributes;
using Test.Utils;
using Test.WSFixture;
using Xunit;
using Xunit.Abstractions;

namespace Function.Test.TaosWS
{
    [TestCaseOrderer("XUnit.Case.Orderers.TestExeOrderer", "Cases.ExeOrder")]
    [Collection("WS Database collection")]
    public class WSQuery
    {
        private readonly ITestOutputHelper _output;
        readonly WSDataBaseFixture _wsDatabase;
        readonly string db = "ws_db_test";
        public WSQuery(ITestOutputHelper output, WSDataBaseFixture wsDatabase)
        {
            _output = output;
            _wsDatabase = wsDatabase;
        }
        /// <author>xiaolei</author>
        /// <Name>WebSocket.Query.NormalTable</Name>
        /// <describe>Using WebSocket to insert data into normal table and query data.</describe>
        /// <filename>WSQuery.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "WebSocket.Query.NormalTable"), TestExeOrder(1), Trait("Category", "NormalTable")]
        public void NormalTable()
        {
            nint wsConn = _wsDatabase.WSConn;
            string tableName = $"{db}.query_tn";
            string createTable = Tools.CreateTable(tableName, false, false);
            List<Object> columns = Tools.ColumnsList(5);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createTable);
            string insertSql = Tools.ConstructInsertSql(tableName, "", columns, null, 5);
            string selectSql = $"select * from {tableName}";

            WSTools.WSExecuteUpdate(wsConn, createTable);
            WSTools.WSExecuteUpdate(wsConn, insertSql);
            var wsRes = WSTools.WSExecuteQuery(wsConn, selectSql);

            List<Object> rows = wsRes.ResultData;
            var metas = wsRes.ResultMeta;

            _output.WriteLine("WS Assert Meta ");
            expectResMeta.ForEach(meta =>
            {
                //_output.WriteLine("wsRes.Key[expectResMeta.IndexOf(meta)].name:{0}", metas[expectResMeta.IndexOf(meta)].name);
                Assert.Equal(meta.name, metas[expectResMeta.IndexOf(meta)].name);
                Assert.Equal(meta.type, metas[expectResMeta.IndexOf(meta)].type);
                Assert.Equal(meta.size, metas[expectResMeta.IndexOf(meta)].size);
            });

            _output.WriteLine("WS Assert Data ");
            for (int i = 0; i < columns.Count; i++)
            {
                _output.WriteLine("{0},{1},{2}", i, columns[i], rows[i]);
                Assert.Equal(columns[i], rows[i]);
            }

        }

        /// <author>xiaolei</author>
        /// <Name>WebSocket.Query.STable</Name>
        /// <describe>Using WebSocket to insert data into STable table and query data.</describe>
        /// <filename>WSQuery.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "WebSocket.Query.STable"), TestExeOrder(2), Trait("Category", "STable")]
        public void STable()
        {
            IntPtr wsConn = _wsDatabase.WSConn;
            string tableName = $"{db}.query_st";
            string createSql = Tools.CreateTable(tableName, true, false);
            List<object> columns = Tools.ColumnsList(5);
            List<object> tags = Tools.TagsList(1, false);
            List<Object> expectResData = Tools.ConstructResData(columns, tags, 5);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            string insertSql = Tools.ConstructInsertSql($"{tableName}_s1", tableName, columns, tags, 5);
            string selectSql = $"select * from {tableName}";

            WSTools.WSExecuteUpdate(wsConn, createSql);
            WSTools.WSExecuteUpdate(wsConn, insertSql);

            var wsResult = WSTools.WSExecuteQuery(wsConn, selectSql);
            List<Object> rows = wsResult.ResultData;
            var metas = wsResult.ResultMeta;

            _output.WriteLine("Assert meta");
            expectResMeta.ForEach(meta =>
            {
                Assert.Equal(meta.name, metas[expectResMeta.IndexOf(meta)].name);
                Assert.Equal(meta.type, metas[expectResMeta.IndexOf(meta)].type);
                Assert.Equal(meta.size, metas[expectResMeta.IndexOf(meta)].size);
            });

            _output.WriteLine("Assert data");
            for (int i = 0; i < expectResData.Count; i++)
            {
                _output.WriteLine("{0},{1},{2}", i, expectResData[i], rows[i]);
                Assert.Equal(expectResData[i], rows[i]);
            }

        }

        /// <author>xiaolei</author>
        /// <Name>WebSocket.Query.JSONTag</Name>
        /// <describe>Using WebSocket to insert data into table which has JSON tag and query data.</describe>
        /// <filename>WSQuery.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "WebSocket.Query.JSONTag"), TestExeOrder(3), Trait("Category", "JSONTag")]
        public void JSONTag()
        {
            IntPtr wsConn = _wsDatabase.WSConn;
            string tableName = $"{db}.query_sj";
            string createSql = Tools.CreateTable(tableName, true, true);
            List<object> columns = Tools.ColumnsList(5);
            List<object> tags = Tools.TagsList(1, true);
            List<Object> expectResData = Tools.ConstructResData(columns, tags, 5);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            string insertSql = Tools.ConstructInsertSql($"{tableName}_j1", tableName, columns, tags, 5);
            string selectSql = $"select * from {tableName}";

            WSTools.WSExecuteUpdate(wsConn, createSql);
            WSTools.WSExecuteUpdate(wsConn, insertSql);

            var wsResult = WSTools.WSExecuteQuery(wsConn, selectSql);
            List<Object> rows = wsResult.ResultData;
            var metas = wsResult.ResultMeta;

            _output.WriteLine("Assert meta");
            expectResMeta.ForEach(meta =>
            {
                Assert.Equal(meta.name, metas[expectResMeta.IndexOf(meta)].name);
                Assert.Equal(meta.type, metas[expectResMeta.IndexOf(meta)].type);
                Assert.Equal(meta.size, metas[expectResMeta.IndexOf(meta)].size);
            });

            _output.WriteLine("Assert data");
            for (int i = 0; i < expectResData.Count; i++)
            {
                _output.WriteLine("{0},{1},{2}", i, expectResData[i], rows[i]);
                Assert.Equal(expectResData[i], rows[i]);
            }

        }
    }
}