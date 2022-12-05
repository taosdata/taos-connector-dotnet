using System;
using Sample.UtilsTools;
using System.Runtime.InteropServices;
using TDengineDriver;
using Example;
using System.Collections.Generic;
using examples;
using Examples.SchemalessRaw;

namespace AsyncQueryExample
{
    public class EntryPoint
    {
        static void Main(string[] args)
        {
            IntPtr conn = UtilsTools.TDConnection();

            AsyncQuerySample asyncQuery = new AsyncQuerySample();
            asyncQuery.RunQueryAsync(conn, "query_async");

            SubscribeSample subscribeSample = new SubscribeSample();
            subscribeSample.RunSubscribeWithCallback(conn, "subscribe_with_callback");
            subscribeSample.RunSubscribeWithoutCallback(conn, "subscribe_without_callback");

            SchemalessSample schemalessSample = new SchemalessSample();
            schemalessSample.RunSchemaless();

            SchemalessRawExample schemalessSampleRaw = new SchemalessRawExample();
            schemalessSampleRaw.RunSchemalessRaw(conn);

            BasicSample basic = new BasicSample(conn, "basic");
            basic.Run();
            basic.CleanBasicSampleData();

            UtilsTools.CloseConnection(conn);
        }
    }
}
