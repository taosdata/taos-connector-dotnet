using Examples.AsyncQuery;
using Examples.Stmt;
using Examples.TMQ;
using Examples.UtilsTools;
using Examples.Schemaless;
using Examples.JSONTag;
using Examples.WS;


namespace Examples
{
    public class EntryPoint
    {
        static void Main(string[] args)
        {
            WebSocketSample webSocketSample = new WebSocketSample("ws_db", "ws_stb");
            webSocketSample.RunWS("ws://127.0.0.1:6041");

            WebSocketSTMT webSocketSTMT = new WebSocketSTMT("ws_db", "ws_stmt_stb", true);
            webSocketSTMT.RunSTMT("ws://127.0.0.1:6041");

            //IntPtr conn = Tools.TDConnection();

            // Query
            //Query.QueryData(conn, "query_db", "q", "q_01", 5);

            //Tools.ExecuteUpdate(conn, "insert into ws_db.ws_stb_01 using ws_db.ws_stb tags(true,-1,-2,-3,-4,1,2,3,4,3.1415,3.14159265358979,'bnr_tag_1','ncr_tag_1')values(1658286671000,true,-1,-2,-3,-4,1,2,3,4,3.1415,3.141592654,'binary_col_1','nchar_col_1')(1658286672000,false,-2,-3,-4,-5,2,3,4,5,6.283,6.283185308,'binary_col_2','nchar_col_2')(1658286673000,true,-3,-4,-5,-6,3,4,5,6,9.4245,9.424777962,'binary_col_3','nchar_col_3')(1658286674000,false,-4,-5,-6,-7,4,5,6,7,12.566,12.566370616,'binary_col_4','nchar_col_4')(1658286675000,true,-5,-6,-7,-8,5,6,7,8,15.707500000000001,15.70796327,'binary_col_5','nchar_col_5');(1658286676000,null,null,null,null,null,null,null,null,null,null,null,null,null);");
            /*
                        //Stmt
                        BindParamBatch bindParamBatchExample = new BindParamBatch();
                        bindParamBatchExample.RunStmtBindParamBatch(conn, "bind_param_batch");

                        BindSingleParamBatch bindSingleParamBatchExample = new BindSingleParamBatch();
                        bindSingleParamBatchExample.RunStmtBindSingleParam(conn, "bind_single_param_batch");

                        //QueryAsync
                        QueryAsync queryAsyncExample = new QueryAsync();
                        queryAsyncExample.RunQueryAsync(conn, "q_tb");

                        // TMQ
                        TMQExample tmqExample = new TMQExample(conn, "topic_01", "tmq_db", "s_tmq", true);
                        tmqExample.RunConsumer();

                        // Schemaless
                        SchemalessExample schemalessExample = new SchemalessExample();
                        schemalessExample.RunSchemaless(conn);

                        // JSON Tag
                        JSONTagExample jSONTagExample = new JSONTagExample();
                        jSONTagExample.RunJSONTag(conn);
            
            Tools.CloseConnection(conn);
            */



        }
    }
}
