using System;
using Sample.UtilsTools;
using System.Runtime.InteropServices;
using TDengineDriver;
using Example;
using System.Collections.Generic;
using examples;

namespace AsyncQueryExample
{
    public class EntryPoint
    {
        static void Main(string[] args)
        {
            IntPtr conn = UtilsTools.TDConnection();

            //AsyncQuerySample asyncQuery = new AsyncQuerySample();
            //asyncQuery.RunQueryAsync(conn, "query_async");

            //SubscribeSample subscribeSample = new SubscribeSample();
            //subscribeSample.RunSubscribeWithCallback(conn, "subscribe_with_callback");
            //subscribeSample.RunSubscribeWithoutCallback(conn, "subscribe_without_callback");
            //UtilsTools.CloseConnection(conn);

            //SchemalessSample schemalessSample = new SchemalessSample();
            //schemalessSample.RunSchemaless();

            //BasicSample basic = new BasicSample(conn,"basic");
            //basic.Run();
            //basic.CleanBasicSampleData();
            
            StmtBindSingleParamExample stmtBindSingleParamExample = new StmtBindSingleParamExample();
            stmtBindSingleParamExample.RunStmtBindSingleParam(conn,"new_utf8");

            //UtilsTools.CloseConnection(conn);
        }
    }
}
