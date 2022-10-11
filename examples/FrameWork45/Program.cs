using FrameWork45.UtilTools;
using System;
using FrameWork45.Examples;
using FrameWork45.Stmt;
using FrameWork45.AsyncQuery;
using FrameWork45.TMQ;
using FrameWork45.Schemaless;
using FrameWork45.JSONTag;
using FrameWork45.WS;

namespace FrameWork45
{
    public class EntryPoint
    {
        static void Main(string[] args)
        {
            //WebSocketSample webSocketSample = new WebSocketSample("ws_db", "ws_stb");
            //webSocketSample.RunWS("ws://127.0.0.1:6041");

            //WebSocketSTMT webSocketSTMT = new WebSocketSTMT("ws_db", "ws_stmt_stb", true);
            //webSocketSTMT.RunSTMT("ws://127.0.0.1:6041");

            IntPtr conn = Tools.TDConnection();

            //Query
            Query.QueryData(conn, "query_db", "q", "q_01", 5);

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

        }
    }
}