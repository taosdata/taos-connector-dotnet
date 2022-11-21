using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Logging;
using System;
using Newtonsoft.Json;
using static Confluent.Kafka.ConfigPropertyNames;


namespace Producer
{
    internal class Produce
    {
        //private readonly ILogger<ProducerSample> _logger;

        //public ProducerSample(ILogger<ProducerSample> logger)
        //{
        //    _logger = logger;
        //}

        public Produce()
        {
        }
        
        public async Task ProduceAsync(string bootstrapServer,string schemaRegisterUrl,string topicName)
        {
            var produceConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
                //BootstrapServers = bootstrapServer;
            };

            using (var p = 
                new ProducerBuilder<string, string>(produceConfig).Build())
            {
                Console.WriteLine($"{p.Name} producing on {topicName}.");

                //try
                //{
                    MessageGenerator generator = new MessageGenerator();
                while (true) 
                {
                    var dr = await p.ProduceAsync("topicmeters", new Message<string, string> { Key = JsonConvert.SerializeObject(generator.GenKey()), Value = JsonConvert.SerializeObject(generator.GenValue()) });
                    //_logger.LogInformation("{0} Delivered key:{1} value:{2} to {3}", DateTimeOffset.UtcNow, dr.Key, dr.Value, dr.TopicPartitionOffset);
                    Console.WriteLine("{0} Delivered key:{1} value:{2} to {3}", DateTimeOffset.UtcNow, dr.Key, dr.Value, dr.TopicPartitionOffset);
                    if (generator.ts > 1667232000010) 
                    {
                        break;
                    }
                }
                //}
                //catch (ProduceException<MeterTag, MeterValues> e)
                //{
                //    //_logger.LogError("Delivery failed :{0}", e.Error.Reason);

                //    Console.WriteLine("Delivery failed :{0}", e.Error.Reason);
                //    Console.WriteLine(e.StackTrace.ToString());
                //}
            }

        }

    }

}

