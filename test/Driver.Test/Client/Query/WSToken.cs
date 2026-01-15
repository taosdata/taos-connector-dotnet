using TDengine.Driver;
using Xunit;

namespace Driver.Test.Client.Query
{
    public partial class Client
    {
        [Fact]
        public void WebSocketTokenQueryMSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_query_test_ms_token";
            this.QueryTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void WebSocketTokenQueryUSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_query_test_us_token";
            this.QueryTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void WebSocketTokenQueryNSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_query_test_ns_token";
            this.QueryTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void WebSocketTokenQueryWithReqIDMSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_query_test_reqid_ms_token";
            this.QueryWithReqIDTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void WebSocketTokenQueryWithReqIDUSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_query_test_reqid_us_token";
            this.QueryWithReqIDTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void WebSocketTokenQueryWithReqIDNSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_query_test_reqid_ns_token";
            this.QueryWithReqIDTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void WebSocketTokenStmtMSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_test_ms_token";
            this.StmtTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void WebSocketTokenStmtUSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_test_us_token";
            this.StmtTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void WebSocketTokenStmtNSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_test_ns_token";
            this.StmtTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void WebSocketTokenStmtWithReqIDMSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_test_req_ms_token";
            this.StmtWithReqIDTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void WebSocketTokenStmtWithReqIDUSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_test_req_us_token";
            this.StmtWithReqIDTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void WebSocketTokenStmtWithReqIDNSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_test_req_ns_token";
            this.StmtWithReqIDTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void WebSocketTokenStmtColumnsMSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_columns_test_ms_token";
            this.StmtBindColumnsTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void WebSocketTokenStmtColumnsUSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_columns_test_us_token";
            this.StmtBindColumnsTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void WebSocketTokenStmtColumnsNSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_columns_test_ns_token";
            this.StmtBindColumnsTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }

        [Fact]
        public void WebSocketTokenVarbinaryTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_varbinary_test_token";
            this.VarbinaryTest(this._wsTokenConnectString, db);
        }

        [Fact]
        public void WebSocketTokenInfluxDBTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_influxdb_test_token";
            this.InfluxDBTest(this._wsTokenConnectString, db);
        }

        [Fact]
        public void WebSocketTokenTelnetTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_telnet_test_token";
            this.TelnetTest(this._wsTokenConnectString, db);
        }

        [Fact]
        public void WebSocketTokenSMLJsonTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_sml_json_test_token";
            this.SMLJsonTest(this._wsTokenConnectString, db);
        }

        [Fact]
        public void WebSocketTokenQueryConcurrencyTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_query_concurrency_test_token";
            this.QueryConcurrencyTest(this._wsTokenConnectString, db);
        }

        [Fact]
        public void WebSocketTokenQueryWithConnectionTimezoneMSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_query_conn_tz_ms_test_token";
            QueryWithConnectionTimezoneTest(this._wsTokenConnectString, "Europe/Paris", db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }
        
        [Fact]
        public void WebSocketTokenQueryWithConnectionTimezoneUSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_query_conn_tz_us_test_token";
            QueryWithConnectionTimezoneTest(this._wsTokenConnectString, "Europe/Paris", db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }
        
        [Fact]
        public void WebSocketTokenQueryWithConnectionTimezoneNSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_query_conn_tz_ns_test_token";
            QueryWithConnectionTimezoneTest(this._wsTokenConnectString, "Europe/Paris", db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }
                
        [Fact]
        public void WebSocketTokenStmtMSBindTimestampTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_bind_stmt_test_ms_token";
            this.StmtBindTimestampTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }

        [Fact]
        public void WebSocketTokenStmtUSBindTimestampTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_bind_stmt_test_us_token";
            this.StmtBindTimestampTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }

        [Fact]
        public void WebSocketTokenStmtNSBindTimestampTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_bind_stmt_test_ns_token";
            this.StmtBindTimestampTest(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }
        
        [Fact]
        public void WebSocketTokenStmtTestWrongTypeMSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_wrong_test_ms_token";
            this.StmtTestWrongType(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
        }
        [Fact]
        public void WebSocketTokenStmtTestWrongTypeUSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_wrong_test_us_token";
            this.StmtTestWrongType(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_MICRO);
        }
        [Fact]
        public void WebSocketTokenStmtTestWrongTypeNSTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_wrong_test_ns_token";
            this.StmtTestWrongType(this._wsTokenConnectString, db, TDenginePrecision.TSDB_TIME_PRECISION_NANO);
        }
        [Fact]
        public void WebSocketTokenStmtTestBindTagWithoutTable()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_bind_tag_no_table_token";
            this.StmtTestBindTagWithoutTable(this._wsTokenConnectString, db);
        }
        [Fact]
        public void WebSocketTokenStmtQuery()
        {
            if (!_isEnterpriseTest)
            {
                
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_query_test_token";
            this.StmtQuery(this._wsTokenConnectString,db);
        }
        
        [Fact]
        public void WebSocketTokenStmtErrorProcessTest()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_error_process_test_token";
            this.StmtErrorProcessTest(this._wsTokenConnectString, db);
        }
        
        [Fact]
        public void WebSocketTokenStmtBindTags()
        {
            if (!_isEnterpriseTest)
            {
                _output.WriteLine("Enterprise edition is required for token-based authentication. Skipping.");
                return;
            }
            const string db = "ws_stmt_bind_tags_test_token";
            this.StmtBindTagsTest(this._wsTokenConnectString, db);
        }
    }
}