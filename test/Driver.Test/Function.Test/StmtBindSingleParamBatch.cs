using TDengineDriver;
using Test.Case.Attributes;
using Test.Fixture;
using Test.Utils;
using Test.Utils.DataSource;
using Test.Utils.ResultSet;
using Test.Utils.Stmt;
using Xunit;
using Xunit.Abstractions;

namespace Cases
{
    [TestCaseOrderer("XUnit.Case.Orderers.TestExeOrderer", "Cases.ExeOrder")]
    [Collection("Database collection")]
    public class StmtBindSingleParamBatch
    {
        readonly DatabaseFixture database;
        private readonly ITestOutputHelper _output;
        public StmtBindSingleParamBatch(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this.database = fixture;
            this._output = output;
        }
        /// <author>xiaolei</author>
        /// <Name>StmtBindSingleParamBatch.NormalTable</Name>
        /// <describe>Using bindSingleParamBatch to insert data into normal table.</describe>
        /// <filename>StmtBindSingleParamBatch.cs</filename>
        /// <result>pass or failed </result>  
        [Fact(DisplayName = "StmtBindSingleParamBatch.NormalTable"), TestExeOrder(1), Trait("Category", "NormalTable")]
        public void TestBindSingleLineCN()
        {
            string tableName = "bind_single_n";
            String createTb = Tools.CreateTable(tableName, false, false);
            string insertSql = "insert into ? values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            string dropSql = $"drop table if exists {tableName}";
            string querySql = $"select * from {tableName}";

            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createTb);
            List<Object> expectResData = StmtDataSource.NTableRowData();

            TAOS_MULTI_BIND[] columns = StmtDataSource.GetColDataMBind();

            IntPtr conn = database.Conn;
            Tools.ExecuteUpdate(conn, createTb, _output);

            IntPtr stmt = StmtTools.StmtInit(conn);
            StmtTools.StmtPrepare(stmt, insertSql);
            StmtTools.SetTableName(stmt, tableName);

            foreach (TAOS_MULTI_BIND col in columns)
            {
                StmtTools.BindSingleParamBatch(stmt, col, Array.IndexOf(columns, col));
            }
            StmtTools.AddBatch(stmt);
            StmtTools.StmtExecute(stmt);
            StmtTools.StmtClose(stmt);

            StmtDataSource.FreeTaosMBind(columns);

            IntPtr res = Tools.ExecuteQuery(conn, querySql, _output);
            ResultSet actualResult = new ResultSet(res);

            List<TDengineMeta> actualResMeta = actualResult.ResultMeta;
            List<Object> actualResData = actualResult.ResultData;

            // Assert meta data
            _output.WriteLine("Assert meta data");

            for (int i = 0; i < actualResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }

            // Assert retrieve data
            _output.WriteLine("Assert retrieve data");
            for (int i = 0; i < actualResData.Count; i++)
            {
                // _output.WriteLine("expect:{0},actual:{1}", expectResData[i], actualResData[i]);
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            _output.WriteLine("StmtBindSingleParamBatch.NormalTable() pass");

        }

        /// <author>xiaolei</author>
        /// <Name>NormalTableStmtCases.TestBindColumnCN</Name>
        /// <describe>Using bindSingleParamBatch to insert data into stable table.</describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "StmtBindSingleParamBatch.STable"), TestExeOrder(2), Trait("Category", "STable")]
        public void STable()
        {
            string tableName = "bind_single_s";
            String createTb = Tools.CreateTable(tableName, true, false);
            String insertSql = $"insert into ? using {tableName} tags(?,?,?,?,?,?,?,?,?,?,?,?,?)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createTb);
            List<Object> expectResData = StmtDataSource.STableRowData(2);

            TAOS_MULTI_BIND[] columns = StmtDataSource.GetColDataMBind();
            TAOS_MULTI_BIND[] tags = StmtDataSource.GetTags(2);

            IntPtr conn = database.Conn;
            Tools.ExecuteUpdate(conn, createTb, _output);

            IntPtr stmt = StmtTools.StmtInit(conn);
            StmtTools.StmtPrepare(stmt, insertSql);
            StmtTools.SetSubTableName(stmt, tableName + "_s1");
            StmtTools.SetTag(stmt, tags);
            foreach (TAOS_MULTI_BIND col in columns)
            {
                StmtTools.BindSingleParamBatch(stmt, col, Array.IndexOf(columns, col));
            }
            StmtTools.AddBatch(stmt);
            StmtTools.StmtExecute(stmt);
            StmtTools.StmtClose(stmt);

            StmtDataSource.FreeTaosMBind(columns);
            StmtDataSource.FreeTaosMBind(tags);

            string querySql = $"select * from {tableName}";
            IntPtr res = Tools.ExecuteQuery(conn, querySql, _output);
            ResultSet actualResult = new ResultSet(res);

            List<TDengineMeta> actualResMeta = actualResult.ResultMeta;
            List<Object> actualResData = actualResult.ResultData;

            // Assert meta data
            _output.WriteLine("Assert meta data");

            for (int i = 0; i < actualResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }

            // Assert retrieve data
            _output.WriteLine("Assert retrieve data");

            for (int i = 0; i < actualResData.Count; i++)
            {
                //_output.WriteLine("expect:{0},actual:{1}", expectResData[i], actualResData[i]);
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            _output.WriteLine("StmtBindSingleParamBatch.STable pass");

        }

        /// <author>xiaolei</author>
        /// <Name>StmtBindSingleParamBatch.JSONTag</Name>
        /// <describe>Using bindSingleParamBatch to insert data into table with JSON tag.</describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "StmtBindSingleParamBatch.JSONTag"), TestExeOrder(2), Trait("Category", "JSONTag")]
        public void JSONTag()
        {
            string tableName = "bind_single_j";
            String createTb = Tools.CreateTable(tableName, true, true);
            String insertSql = $"insert into ? using {tableName} tags(?)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createTb);
            List<Object> expectResData = StmtDataSource.JsonRowData(2);

            TAOS_MULTI_BIND[] columns = StmtDataSource.GetColDataMBind();
            TAOS_MULTI_BIND[] tags = StmtDataSource.GetJsonTag(2);

            IntPtr conn = database.Conn;
            Tools.ExecuteUpdate(conn, createTb, _output);

            IntPtr stmt = StmtTools.StmtInit(conn);
            StmtTools.StmtPrepare(stmt, insertSql);
            StmtTools.SetTableNameTags(stmt, tableName + "_s1", tags);
            foreach (TAOS_MULTI_BIND col in columns)
            {
                StmtTools.BindSingleParamBatch(stmt, col, Array.IndexOf(columns, col));
            }
            StmtTools.AddBatch(stmt);
            StmtTools.StmtExecute(stmt);
            StmtTools.StmtClose(stmt);

            StmtDataSource.FreeTaosMBind(columns);
            StmtDataSource.FreeTaosMBind(tags);

            string querySql = $"select * from {tableName}";
            IntPtr res = Tools.ExecuteQuery(conn, querySql, _output);
            ResultSet actualResult = new ResultSet(res);

            List<TDengineMeta> actualResMeta = actualResult.ResultMeta;
            List<Object> actualResData = actualResult.ResultData;

            // Assert meta data
            _output.WriteLine("Assert meta data");

            for (int i = 0; i < actualResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }

            // Assert retrieve data
            _output.WriteLine("Assert retrieve data");

            for (int i = 0; i < actualResData.Count; i++)
            {
                //_output.WriteLine("expect:{0},actual:{1}", expectResData[i], actualResData[i]);
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            _output.WriteLine("StmtBindSingleParamBatch.JSONTag pass");

        }

    }
}