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
    public class StmtQuery
    {
        readonly DatabaseFixture database;
        private readonly ITestOutputHelper _output;

        public StmtQuery(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this.database = fixture;
            this._output = output;

        }

        /// <author>xiaolei</author>
        /// <Name>StmtQuery.NormalTable</Name>
        /// <describe>Using STMT query data from normal table.</describe>
        /// <filename>StmtQuery.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "StmtQuery.NormalTable"), TestExeOrder(1), Trait("Category", "NormalTable")]
        public void NormalTable()
        {
            const string table = "stmt_query_n";
            string selectSql = $"SELECT * FROM {table} where ts>? " +
                $"and v1>? and v2>? and v4>? and v8>=? " +
                $"and u1>? and u2>=? and u4>? and u8>? " +
                $"and f4>? and f8>? and bin=? and nchr=? and b=?";

            string selectExpectSql = $"SELECT * FROM {table} where ts>1659060000000 " +
                $"and v1>-10 and v2>-20 and v4>-30 and v8>=-40 " +
                $"and u1>0 and u2>=1 and u4>2 and u8>3 " +
                $"and f4>3.1415 and f8>3.1415926535897932 and bin=\'binary_col_列_3\' " +
                $"and nchr=\'nchar_col_列_3\' and b=true";

            string createTable = Tools.CreateTable(table, false, false);
            List<Object> columns = Tools.ColumnsList(5);
            string insertSql = Tools.ConstructInsertSql(table, "", columns, null, 5);

            IntPtr conn = database.Conn;

            Tools.ExecuteUpdate(conn, createTable, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);

            List<object> conditionList = new List<object>(){
                1659060000000
                , (sbyte)-10
                , (short)-20
                , -30
                , (long)-40
                , (byte)0
                , (ushort)1
                , (uint)2
                , (ulong)3
                , 3.1415F
                , 3.1415926535897932D
                , "binary_col_列_3"
                , "nchar_col_列_3"
                , true
            };

            TAOS_MULTI_BIND[] conditions = StmtDataSource.GetQueryConditionMBind(conditionList);

            IntPtr stmt = StmtTools.StmtInit(conn);
            StmtTools.StmtPrepare(stmt, selectSql);
            StmtTools.BindParamBatch(stmt, conditions);
            StmtTools.StmtExecute(stmt);
            IntPtr res = StmtTools.StmtUseResult(stmt);
            ResultSet actualResult = new ResultSet(res);
            StmtTools.StmtClose(stmt);


            List<TDengineMeta> actualResMeta = actualResult.ResultMeta;
            List<Object> actualResData = actualResult.ResultData;

            IntPtr expectRes = Tools.ExecuteQuery(conn, selectExpectSql, _output);
            ResultSet expectResult = new ResultSet(expectRes);
            List<TDengineMeta> expectResMeta = expectResult.ResultMeta;
            List<Object> expectResData = expectResult.ResultData;

            // Assert meta data
            _output.WriteLine("Assert meta data");
            for (int i = 0; i < expectResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }
            // assert data
            _output.WriteLine("Assert meta data");
            for (int i = 0; i < expectResData.Count; i++)
            {
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            StmtDataSource.FreeTaosMBind(conditions);
        }

        /// <author>xiaolei</author>
        /// <Name>StmtQuery.STable</Name>
        /// <describe>Using STMT query data from super table.</describe>
        /// <filename>StmtQuery.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "StmtQuery.STable"), TestExeOrder(2), Trait("Category", "STable")]
        public void STable()
        {
            const string table = "stmt_query_s";
            string selectSql = $"SELECT * FROM {table} where ts>? " +
                $"and v1>? and v2>? and v4>? and v8>=? " +
                $"and u1>? and u2>=? and u4>? and u8>? " +
                $"and ff>? and dd>? and bb=? and nc=? and bo=?";

            string selectExpectSql = $"SELECT * FROM {table} where ts>1659060000000 " +
                $"and v1>-10 and v2>-20 and v4>-30 and v8>=-40 " +
                $"and u1>0 and u2>=1 and u4>2 and u8>3 " +
                $"and ff>3.1415 and dd>3.1415926535897932 " +
                $"and bb=\'binary_tag_标签_1\' and nc=\'nchar_tag_标签_1\' and bo=true";


            string createTable = Tools.CreateTable(table, true, false);
            List<Object> columns = Tools.ColumnsList(5);
            List<Object> tags = Tools.TagsList(1, false);
            string insertSql = Tools.ConstructInsertSql(table + "_s1", table, columns, tags, 5);

            IntPtr conn = database.Conn;
            Tools.ExecuteUpdate(conn, createTable, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);

            List<object> conditionList = new List<object>(){
                1659060000000
                , (sbyte)-10
                , (short)-20
                , -30
                , (long)-40
                , (byte)0
                , (ushort)1
                , (uint)2
                , (ulong)3
                , 3.1415F
                , 3.1415926535897932D
                , "binary_tag_标签_1"
                , "nchar_tag_标签_1"
                , true
            };

            TAOS_MULTI_BIND[] conditions = StmtDataSource.GetQueryConditionMBind(conditionList);

            IntPtr stmt = StmtTools.StmtInit(conn);
            StmtTools.StmtPrepare(stmt, selectSql);
            StmtTools.BindParamBatch(stmt, conditions);
            StmtTools.StmtExecute(stmt);
            IntPtr res = StmtTools.StmtUseResult(stmt);
            ResultSet actualResult = new ResultSet(res);
            StmtTools.StmtClose(stmt);


            List<TDengineMeta> actualResMeta = actualResult.ResultMeta;
            List<Object> actualResData = actualResult.ResultData;

            IntPtr expectRes = Tools.ExecuteQuery(conn, selectExpectSql, _output);
            ResultSet expectResult = new ResultSet(expectRes);
            List<TDengineMeta> expectResMeta = expectResult.ResultMeta;
            List<Object> expectResData = expectResult.ResultData;

            // Assert meta data
            _output.WriteLine("Assert meta data");
            for (int i = 0; i < expectResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }
            // assert data
            _output.WriteLine("Assert meta data");
            for (int i = 0; i < expectResData.Count; i++)
            {
                //_output.WriteLine("i:{0},expectResData:{1},actualResData:{2}", i, expectResData[i], actualResData[i]);
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            StmtDataSource.FreeTaosMBind(conditions);
        }

        /// <author>xiaolei</author>
        /// <Name>StmtQuery.JSONTag</Name>
        /// <describe>Using STMT query data from table with JSON tags.</describe>
        /// <filename>StmtQuery.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "StmtQuery.JSONTag"), TestExeOrder(1), Trait("Category", "JSONTag")]
        public void JSONTag()
        {
            const string table = "stmt_query_j";
            string selectSql = $"SELECT * FROM {table} where ts>? " +
                $"and v1>? and  v2>? and json_tag->\'key4\'>? and v8>=? " +
                $"and u1>? and u2>=? and u4>? and u8>? " +
                $"and f4>? and f8>? " +
                $"and json_tag->\'key3\'=? and nchr=? and json_tag->\'key5\'=?";

            string selectExpectSql = $"SELECT * FROM {table} where ts>1659060000000 " +
                $"and v1>-10 and v2>-20 and json_tag->\'key4\'>-30 and v8>=-40 " +
                $"and u1>0 and u2>=1 and u4>2 and u8>3 " +
                $"and f4>3.1415 and f8>3.1415926535897932 " +
                $"and json_tag->\'key3\'=\'TDengine涛思数据\'and nchr=\'nchar_col_列_3\' and json_tag->\'key5\'=false";


            string createTable = Tools.CreateTable(table, true, true);

            IntPtr conn = database.Conn;
            Tools.ExecuteUpdate(conn, createTable, _output);

            for (int i = 1; i < 4; i++)
            {
                List<Object> columns = Tools.ColumnsList(5);
                List<Object> tags = Tools.TagsList(i, true);
                string insertSql = Tools.ConstructInsertSql($"{table}_s_{i}", table, columns, tags, 5);
                Tools.ExecuteUpdate(conn, insertSql, _output);
            }

            List<object> conditionList = new List<object>(){
                1659060000000
                , (sbyte)-10
                , (short)-20
                , -30
                , (long)-40
                , (byte)0
                , (ushort)1
                , (uint)2
                , (ulong)3
                , 3.1415F
                , 3.1415926535897932D
                , "TDengine涛思数据"
                , "nchar_col_列_3"
                , false
            };

            TAOS_MULTI_BIND[] conditions = StmtDataSource.GetQueryConditionMBind(conditionList);

            IntPtr stmt = StmtTools.StmtInit(conn);
            StmtTools.StmtPrepare(stmt, selectSql);
            StmtTools.BindParamBatch(stmt, conditions);
            StmtTools.StmtExecute(stmt);
            IntPtr res = StmtTools.StmtUseResult(stmt);
            ResultSet actualResult = new ResultSet(res);
            StmtTools.StmtClose(stmt);


            List<TDengineMeta> actualResMeta = actualResult.ResultMeta;
            List<Object> actualResData = actualResult.ResultData;

            IntPtr expectRes = Tools.ExecuteQuery(conn, selectExpectSql, _output);
            ResultSet expectResult = new ResultSet(expectRes);
            List<TDengineMeta> expectResMeta = expectResult.ResultMeta;
            List<Object> expectResData = expectResult.ResultData;

            // Assert meta data
            _output.WriteLine("Assert meta data");
            for (int i = 0; i < expectResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }
            // assert data
            _output.WriteLine("Assert data");
            for (int i = 0; i < expectResData.Count; i++)
            {
                //_output.WriteLine("i:{0},expectResData:{1},actualResData:{2}", i, expectResData[i], actualResData[i]);
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            StmtDataSource.FreeTaosMBind(conditions);

        }

    }

}