using Examples.AsyncQuery;
using Examples.Stmt;
using Examples.TMQ;
using Examples.UtilsTools;
using Examples.Schemaless;
using Examples.JSONTag;

namespace Examples
{
    public class EntryPoint
    {
        static void Main(string[] args)
        {

            IntPtr conn = Tools.TDConnection();

            // Query
            // Query.QueryData(conn, "query_db", "q", "q_01", 5);

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
