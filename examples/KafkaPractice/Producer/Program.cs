

namespace Producer
{
    internal class EntryPoint
    {
        public static async Task Main(string[] args)
        {
            // See https://aka.ms/new-console-template for more information
            Console.WriteLine("Hello, World!");

            Produce producerSample = new Produce();
            await producerSample.ProduceAsync("bootstrap","schemaRegisterUrl","topicmeters");
        }
    }
}
