using TDengineDriver;

namespace Consumer
{
    internal class EntryPoint
    {
        public static async Task Main(string[] args)
        {
            IntPtr conn = TDengine.Connect("127.0.0.1", "root", "taosdata", "power", 0);
            
            Consume consumerSample = new Consume();
            consumerSample.RunConsume(conn);

            TDengine.Close(conn);

        }
    }

}
