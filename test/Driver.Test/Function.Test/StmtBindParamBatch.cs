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

    public class StmtBindParamBatch
    {

        readonly DatabaseFixture database;
        private readonly ITestOutputHelper _output;


        public StmtBindParamBatch(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this.database = fixture;
            this._output = output;
        }
        /// <author>xiaolei</author>
        /// <Name>StmtBindParamBatchCases.NormalTable</Name>
        /// <describe>Using bindParamBatch to insert into normal table.</describe>
        /// <filename>StmtBindParamBatch.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "StmtBindParamBatch.NormalTable"),TestExeOrder(1), Trait("Category", "NormalTable")]
        public void NormalTable()
        {
            string tableName = "bind_param_batch_n";
            string createSql = Tools.CreateTable(tableName, false, false);
            string insertSql = $"insert into ? values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            List<Object> expectResData = StmtDataSource.NTableRowData();

            TAOS_MULTI_BIND[] mBinds = StmtDataSource.GetColDataMBind();

            IntPtr conn = database.Conn;
            Tools.ExecuteUpdate(conn, createSql, _output);

            IntPtr stmt = StmtTools.StmtInit(conn);
            StmtTools.StmtPrepare(stmt, insertSql);
            StmtTools.SetTableName(stmt, tableName);
            StmtTools.BindParamBatch(stmt, mBinds);
            StmtTools.AddBatch(stmt);
            StmtTools.StmtExecute(stmt);
            StmtTools.StmtClose(stmt);

            StmtDataSource.FreeTaosMBind(mBinds);

            string querySql = $"select * from {tableName}";
            IntPtr res = Tools.ExecuteQuery(conn, querySql, _output);

            ResultSet actualResult = new ResultSet(res);

            List<TDengineMeta> actualResMeta = actualResult.ResultMeta;
            List<Object> actualResData = actualResult.ResultData;

            // Assert retrieve data
            _output.WriteLine("Assert retrieve data");

            for (int i = 0; i < actualResData.Count; i++)
            {
                //_output.WriteLine("expect:{0},actual:{1}", expectResData[i], actualResData[i]);
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            // Assert meta data
            _output.WriteLine("Assert meta data");

            for (int i = 0; i < actualResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }
            _output.WriteLine("StmtBindParamBatch.NormalTable pass");

        }

        /// <author>xiaolei</author>
        /// <Name>StmtBindParamBatch.STable</Name>
        /// <describe>Using bindParamBatch to insert into stable.</describe>
        /// <filename>StmtBindParamBatch.cs</filename>
        /// <result>pass or failed </result>
        [Fact(DisplayName = "StmtBindParamBatch.Stable"), TestExeOrder(2), Trait("Category", "STable")]
        public void STable()
        {
            string tableName = "bind_param_batch_s";
            String createSql = Tools.CreateTable(tableName, true, false);
            String insertSql = $"insert into ? using {tableName} tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            TAOS_MULTI_BIND[] tags = StmtDataSource.GetTags(1);
            TAOS_MULTI_BIND[] mBinds = StmtDataSource.GetColDataMBind();

            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            List<Object> expectResData = StmtDataSource.STableRowData(1);

            IntPtr conn = database.Conn;
            Tools.ExecuteUpdate(conn, createSql, _output);

            IntPtr stmt = StmtTools.StmtInit(conn);
            StmtTools.StmtPrepare(stmt, insertSql);
            StmtTools.SetTableNameTags(stmt, tableName + "_s1", tags);
            StmtTools.BindParamBatch(stmt, mBinds);
            StmtTools.AddBatch(stmt);
            StmtTools.StmtExecute(stmt);
            StmtTools.StmtClose(stmt);

            StmtDataSource.FreeTaosMBind(tags);
            StmtDataSource.FreeTaosMBind(mBinds);

            string querySql = $"select * from {tableName};";
            IntPtr res = Tools.ExecuteQuery(conn, querySql, _output);
            ResultSet actualResult = new ResultSet(res);

            List<TDengineMeta> actualResMeta = actualResult.ResultMeta;
            List<Object> actualResData = actualResult.ResultData;
            
            // Assert retrieve data
            _output.WriteLine("Assert retrieve data");
            for (int i = 0; i < actualResData.Count; i++)
            {
                //_output.WriteLine("expect:{0},actual:{1},index:{2}", expectResData[i], actualResData[i], i);
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            // Assert meta data
            _output.WriteLine("Assert meta data");

            for (int i = 0; i < actualResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }
            _output.WriteLine("StmtBindParamBatch.STable pass");
        }


        /// <author>xiaolei</author>
        /// <Name>StmtBindParamBatch.JSONTag</Name>
        /// <describe>Using bindParamBatch to insert into stable, which has JSON tag. </describe>
        /// <filename>StmtSTable.cs</filename>
        /// <result>pass or failed </result>
        [Fact(DisplayName = "StmtBindParamBatch.JSONTag"), TestExeOrder(3), Trait("Category", "JSONTag")]
        public void JSONTag()
        {
            string tableName = "bind_param_batch_j";
            String createSql = Tools.CreateTable(tableName, true, true);
            String insertSql = $"insert into ? using {tableName} tags(?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String dropSql = $"drop table if exists {tableName};";

            TAOS_MULTI_BIND[] tags = StmtDataSource.GetJsonTag(1);
            TAOS_MULTI_BIND[] mBinds = StmtDataSource.GetColDataMBind();

            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            List<Object> expectResData = StmtDataSource.JsonRowData(1);

            IntPtr conn = database.Conn;
            Tools.ExecuteUpdate(conn, dropSql, _output);
            Tools.ExecuteUpdate(conn, createSql, _output);

            IntPtr stmt = StmtTools.StmtInit(conn);
            StmtTools.StmtPrepare(stmt, insertSql);

            //StmtTools.SetTableNameTags(stmt, tableName + "_s1", tags);
            StmtTools.SetSubTableName(stmt, tableName + "_s1");
            StmtTools.SetTag(stmt,tags);
            StmtTools.BindParamBatch(stmt, mBinds);
            StmtTools.AddBatch(stmt);
            StmtTools.StmtExecute(stmt);

            StmtTools.StmtClose(stmt);
            StmtDataSource.FreeTaosMBind(tags);
            StmtDataSource.FreeTaosMBind(mBinds);

            string querySql = $"select * from {tableName}";
            IntPtr res = Tools.ExecuteQuery(conn, querySql, _output);
            ResultSet actualResult = new ResultSet(res);

            List<TDengineMeta> actualResMeta = actualResult.ResultMeta;
            List<Object> actualResData = actualResult.ResultData;

            // Assert retrieve data
            _output.WriteLine("Assert retrieve data");

            for (int i = 0; i < actualResData.Count; i++)
            {
                // _output.WriteLine("expect:{0},actual:{1}", expectResData[i], actualResData[i]);
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            // Assert meta data
            _output.WriteLine("Assert meta data");

            for (int i = 0; i < actualResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }
            _output.WriteLine("StmtBindParamBatch.JSONTag pass");
        }
        
    }
}