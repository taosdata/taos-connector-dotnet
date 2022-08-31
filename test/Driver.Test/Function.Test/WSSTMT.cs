using System;
using System.Collections.Generic;
using TDengineDriver;
using Test.Case.Attributes;
using Test.Utils;
using Test.Utils.DataSource;
using Test.WSFixture;
using Xunit;
using Xunit.Abstractions;
using TDengineWS.Impl;
using System.Linq;
using Test.Utils.ResultSet;

namespace Function.Test.TaosWS
{
    [TestCaseOrderer("XUnit.Case.Orderers.TestExeOrderer", "Cases.ExeOrder")]
    [Collection("WS Database collection")]
    public class WSSTMT
    {
        private readonly ITestOutputHelper _output;
        readonly WSDataBaseFixture _wsDatabase;
        readonly string db = "ws_db_test";
        public WSSTMT(ITestOutputHelper output, WSDataBaseFixture wsDatabase)
        {
            _output = output;
            _wsDatabase = wsDatabase;
            LibTaosWS.WSEnableLog();
        }

        /// <author>xiaolei</author>
        /// <Name>WebSocket.STMT.NormalTable</Name>
        /// <describe>Using WebSocket STMT to insert into normal table.</describe>
        /// <filename>WSSTMT.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(Skip = = "WebSocket.STMT.NormalTable"), TestExeOrder(1), Trait("Category", "NormalTable")]
        public void NormalTable()
        {
            string tableName = $"{db}.bind_param_batch_n";
            string createSql = Tools.CreateTable(tableName, false, false);
            string insertSql = $"insert into ? values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            List<Object> expectResData = StmtDataSource.NTableRowData();

            TAOS_MULTI_BIND[] wsMBinds = StmtDataSource.GetWSColDataMBind();
            
            nint wsConn = _wsDatabase.WSConn;

            WSTools.WSExecuteUpdate(wsConn,createSql);

            nint wsStmt = WSSTMTTools.WSStmtInit(wsConn);
            WSSTMTTools.WSStmtPrepare(wsStmt,insertSql);
            WSSTMTTools.WSStmtSetTbname(wsStmt,tableName);
            WSSTMTTools.WSStmtBindParamBatch(wsStmt, wsMBinds,14);
            WSSTMTTools.WSStmtAddBatch(wsStmt);
            int affectRows = WSSTMTTools.WSStmtExecute(wsStmt);
            WSSTMTTools.WSCloseSTMT(wsStmt);
            WSSTMTTools.WSFreeMBind(wsMBinds);

            string querySql = $"select * from {tableName}";
            var wsResultSet = WSTools.WSExecuteQuery(wsConn, querySql);


            var metas = wsResultSet.ResultMeta;
            List<Object> rows = wsResultSet.ResultData;

            // Assert retrieve data
            _output.WriteLine("Assert retrieve data");

            for (int i = 0; i < rows.Count; i++)
            {
                //_output.WriteLine("expect:{0},actual:{1}", expectResData[i], actualResData[i]);
                Assert.Equal(expectResData[i], rows[i]);
            }
            // Assert meta data
            _output.WriteLine("Assert meta data");

            for (int i = 0; i < metas.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, metas[i].name);
                Assert.Equal(expectResMeta[i].type, metas[i].type);
                Assert.Equal(expectResMeta[i].size, metas[i].size);
            }
        }

        /// <author>xiaolei</author>
        /// <Name>WebSocket.STMT.STable</Name>
        /// <describe>Using WebSocket STMT to insert into stable.</describe>
        /// <filename>WSSTMT.cs</filename>
        /// <result>pass or failed </result>
        [Fact(DisplayName = "WebSocket.STMT.Stable"), TestExeOrder(2), Trait("Category", "STable")]
        public void STable()
        {
            string tableName = $"{db}.bind_param_batch_s";
            String createSql = Tools.CreateTable(tableName, true, false);
            String insertSql = $"insert into ? using {tableName} tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            TAOS_MULTI_BIND[] wsTags = StmtDataSource.GetWSTags(1);
            TAOS_MULTI_BIND[] wsMBinds = StmtDataSource.GetWSColDataMBind();

            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            List<Object> expectResData = StmtDataSource.STableRowData(1);

            nint wsConn = _wsDatabase.WSConn;
            WSTools.WSExecuteUpdate(wsConn,createSql);

            nint wsStmt = WSSTMTTools.WSStmtInit(wsConn);
            WSSTMTTools.WSStmtPrepare(wsStmt,insertSql);
            WSSTMTTools.WSStmtSetTbnameTags(wsStmt,$"{tableName}_s1",wsTags,wsTags.Length);
            WSSTMTTools.WSStmtBindParamBatch(wsStmt,wsMBinds,wsMBinds.Length);
            WSSTMTTools.WSStmtAddBatch(wsStmt);
            WSSTMTTools.WSStmtExecute(wsStmt);
            WSSTMTTools.WSCloseSTMT(wsStmt);

            WSSTMTTools.WSFreeMBind(wsTags);
            WSSTMTTools.WSFreeMBind(wsMBinds);

            string querySql = $"select * from {tableName};";
            var wsResult = WSTools.WSExecuteQuery(wsConn, querySql);
            var metas = wsResult.ResultMeta;
            var rows = wsResult.ResultData;

            // Assert retrieve data
            _output.WriteLine("Assert retrieve data");
            for (int i = 0; i < rows.Count; i++)
            {
                //_output.WriteLine("expect:{0},actual:{1},index:{2}", expectResData[i], rows[i], i);
                Assert.Equal(expectResData[i], rows[i]);
            }
            // Assert meta data
            _output.WriteLine("Assert meta data");

            for (int i = 0; i < metas.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, metas[i].name);
                Assert.Equal(expectResMeta[i].type, metas[i].type);
                Assert.Equal(expectResMeta[i].size, metas[i].size);
            }
        }

        /// <author>xiaolei</author>
        /// <Name>WebSocket.STMT.JSONTag</Name>
        /// <describe>Using WebSocket STMT to insert into stable, which has JSON tag. </describe>
        /// <filename>WSSTMT.cs</filename>
        /// <result>pass or failed </result>
        [Fact(DisplayName = "WebSocket.STMT.JSONTag"), TestExeOrder(3), Trait("Category", "JSONTag")]
        public void JSONTag()
        {
            string tableName = $"{db}.bind_param_batch_j";
            String createSql = Tools.CreateTable(tableName, true, true);
            String insertSql = $"insert into ? using {tableName} tags(?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String dropSql = $"drop table if exists {tableName};";

            TAOS_MULTI_BIND[] wsTags = StmtDataSource.GetWSJsonTag(1);
            TAOS_MULTI_BIND[] wsMBinds = StmtDataSource.GetWSColDataMBind();

            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            List<Object> expectResData = StmtDataSource.JsonRowData(1);

            nint wsConn = _wsDatabase.WSConn;
            _output.WriteLine("create:{0}",createSql);
            WSTools.WSExecuteUpdate(wsConn, createSql);

            nint wsStmt = WSSTMTTools.WSStmtInit(wsConn);
            WSSTMTTools.WSStmtPrepare(wsStmt,insertSql);
            _output.WriteLine("table:{0}", $"{tableName}_s1");
            //WSSTMTTools.WSStmtSetTbname(wsStmt,$"{tableName}_s1");
            //WSSTMTTools.WSStmtSetTags(wsStmt,wsTags,wsTags.Length);
            WSSTMTTools.WSStmtSetTbnameTags(wsStmt, $"{tableName}_s1",wsTags,wsTags.Length);
            WSSTMTTools.WSStmtBindParamBatch(wsStmt,wsMBinds,wsMBinds.Length);
            WSSTMTTools.WSStmtAddBatch(wsStmt);
            WSSTMTTools.WSStmtExecute(wsStmt);
            WSSTMTTools.WSCloseSTMT(wsStmt);

            WSSTMTTools.WSFreeMBind(wsTags);
            WSSTMTTools.WSFreeMBind(wsMBinds);

            string querySql = $"select * from {tableName}";
            var wsResult = WSTools.WSExecuteQuery(wsConn, querySql);
            var metas = wsResult.ResultMeta;
            var rows = wsResult.ResultData;

            // Assert retrieve data
            _output.WriteLine("Assert retrieve data");

            for (int i = 0; i < rows.Count; i++)
            {
                // _output.WriteLine("expect:{0},actual:{1}", expectResData[i], rows[i]);
                Assert.Equal(expectResData[i], rows[i]);
            }
            // Assert meta data
            _output.WriteLine("Assert meta data");

            for (int i = 0; i < metas.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, metas[i].name);
                Assert.Equal(expectResMeta[i].type, metas[i].type);
                Assert.Equal(expectResMeta[i].size, metas[i].size);
            }
        }

    }
}
