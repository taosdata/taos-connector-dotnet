using TDengine.Driver;
using Xunit;

namespace Driver.Test.Client.Query
{
    public partial class Client
    {
        [Fact]
        public void NativeTokenQueryMSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "query_test_ms_token";
            this.QueryTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeTokenQueryUSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "query_test_us_token";
            this.QueryTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeTokenQueryNSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "query_test_ns_token";
            this.QueryTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeTokenQueryWithReqIDMSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "query_test_reqid_ms_token";
            this.QueryWithReqIDTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeTokenQueryWithReqIDUSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "query_test_reqid_us_token";
            this.QueryWithReqIDTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeTokenQueryWithReqIDNSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "query_test_reqid_ns_token";
            this.QueryWithReqIDTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeTokenStmtMSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_test_ms_token";
            this.StmtTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeTokenStmtUSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_test_us_token";
            this.StmtTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeTokenStmtNSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_test_ns_token";
            this.StmtTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeTokenStmtWithReqIDMSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_test_req_ms_token";
            this.StmtWithReqIDTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeTokenStmtWithReqIDUSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_test_req_us_token";
            this.StmtWithReqIDTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeTokenStmtWithReqIDNSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_test_req_ns_token";
            this.StmtWithReqIDTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeTokenStmtColumnsMSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_columns_test_ms_token";
            this.StmtBindColumnsTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeTokenStmtColumnsUSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_columns_test_us_token";
            this.StmtBindColumnsTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeTokenStmtColumnsNSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_columns_test_ns_token";
            this.StmtBindColumnsTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeTokenVarbinaryTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "varbinary_test_token";
            this.VarbinaryTest(this._nativeTokenConnectString, db);
        }

        [Fact]
        public void NativeTokenInfluxDBTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "influxdb_test_token";
            this.InfluxDBTest(this._nativeTokenConnectString, db);
        }

        [Fact]
        public void NativeTokenTelnetTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "telnet_test_token";
            this.TelnetTest(this._nativeTokenConnectString, db);
        }

        [Fact]
        public void NativeTokenSMLJsonTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "sml_json_test_token";
            this.SMLJsonTest(this._nativeTokenConnectString, db);
        }

        [Fact]
        public void NativeTokenQueryConcurrencyTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "query_concurrency_test_token";
            this.QueryConcurrencyTest(this._nativeTokenConnectString, db);
        }
        
        [Fact]
        public void NativeTokenQueryWithConnectionTimezoneMSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "query_conn_tz_ms_test_token";
            QueryWithConnectionTimezoneTest(this._nativeTokenConnectString, "Europe/Paris", db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }
        
        [Fact]
        public void NativeTokenQueryWithConnectionTimezoneUSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "query_conn_tz_us_test_token";
            QueryWithConnectionTimezoneTest(this._nativeTokenConnectString, "Europe/Paris", db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }
        
        [Fact]
        public void NativeTokenQueryWithConnectionTimezoneNSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "query_conn_tz_ns_test_token";
            QueryWithConnectionTimezoneTest(this._nativeTokenConnectString, "Europe/Paris", db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }
        
        [Fact]
        public void NativeTokenStmtMSBindTimestampTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_bind_stmt_test_ms_token";
            this.StmtBindTimestampTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void NativeTokenStmtUSBindTimestampTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_bind_stmt_test_us_token";
            this.StmtBindTimestampTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void NativeTokenStmtNSBindTimestampTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_bind_stmt_test_ns_token";
            this.StmtBindTimestampTest(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }
        [Fact]
        public void NativeTokenStmtTestWrongTypeMSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_wrong_test_ms_token";
            this.StmtTestWrongType(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }
        [Fact]
        public void NativeTokenStmtTestWrongTypeUSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_wrong_test_us_token";
            this.StmtTestWrongType(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }
        [Fact]
        public void NativeTokenStmtTestWrongTypeNSTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_wrong_test_ns_token";
            this.StmtTestWrongType(this._nativeTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void NativeTokenStmtTestBindTagWithoutTable()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_bind_tag_no_table_token";
            this.StmtTestBindTagWithoutTable(this._nativeTokenConnectString, db);
        }
        
        [Fact]
        public void NativeTokenStmtQuery()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_query_test_token";
            this.StmtQuery(this._nativeTokenConnectString,db);
        }

        [Fact]
        public void NativeTokenStmtErrorProcessTest()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_error_process_test_token";
            this.StmtErrorProcessTest(this._nativeTokenConnectString, db);
        }

        [Fact]
        public void NativeTokenStmtBindTags()
        { 
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "stmt_bind_tags_test_token";
            this.StmtBindTagsTest(this._nativeTokenConnectString, db);
        }
    }
}