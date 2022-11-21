using Confluent.Kafka;
using Newtonsoft.Json;
using TDengineDriver;

namespace Consumer
{
    internal class Consume
    {
        const int consumerNum = 3;
        ConsumerConfig conf = new ConsumerConfig
        {
            GroupId = "meter_consumser",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest

        };


        public void RunConsume(IntPtr conn)
        {


            using (var c = new ConsumerBuilder<string, string>(conf).Build())
            using (var c2 = new ConsumerBuilder<string, string>(conf).Build())
            using (var c3 = new ConsumerBuilder<string, string>(conf).Build())
            {
                IConsumer<string, string>[] consumerGroup = { c, c2, c3 };

                Task[] consumeTasks = new Task[consumerNum];
                for (int i = 0; i < consumerNum; i++)
                {
                    consumeTasks[i] = Task.Factory.StartNew((Object obj) =>
                    {
                        IConsumer<string, string> consumer = obj as IConsumer<string, string>;
                        TDengineWriter writor = new TDengineWriter();
                        if (consumer == null)
                            return;
                        consumer.Subscribe("topicmeters");
                        
                        CancellationTokenSource cts = new CancellationTokenSource();

                        Console.CancelKeyPress += (_, e) =>
                        {
                            e.Cancel = true; // prevent the process from terminating.
                            cts.Cancel();
                        };

                        try
                        {
                            while (true)
                            {
                                try
                                {
                                    var cr = consumer.Consume(cts.Token);

                                    MeterTag tag = JsonConvert.DeserializeObject<MeterTag>(cr.Key);
                                    MeterValues values = JsonConvert.DeserializeObject<MeterValues>(cr.Value);

                                    //IntPtr connect = TDengine.Connect("127.0.0.1","root","taosdata","power",0);
                                    //TDengineWriter writer = new TDengineWriter();
                                    //string sql = writer.GenerateSql(tag, values);
                                    //writer.InsertData(connect, sql);

                                    //Console.WriteLine("{0} Key:{1} Value:{2} , TopicParition:{3} TopicPartitionOffset:{4}", DateTimeOffset.UtcNow, cr.Key, cr.Value, cr.TopicPartition, cr.TopicPartitionOffset);
                                    Console.WriteLine("[{0}] Thread #{4} Key:{1} Value:{2} , TopicParition:{3} TopicPartitionOffset:{4}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff:ffffff"), tag.ToString(), values.ToString(), cr.TopicPartition, cr.TopicPartitionOffset,Task.CurrentId);
                                }
                                catch (ConsumeException e)
                                {
                                    Console.WriteLine("Error occured:{0}", e.Error.Reason);
                                }
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            c.Close();
                        }
                    }, consumerGroup[i]);


                }

                //c.Subscribe("topicmeters");
                //CancellationTokenSource cts = new CancellationTokenSource();

                //Console.CancelKeyPress += (_, e) =>
                //{
                //    e.Cancel = true; // prevent the process from terminating.
                //    cts.Cancel();

                //};

                //try
                //{
                //    while (true)
                //    {
                //        try
                //        {
                //            var cr = c.Consume(cts.Token);


                //            MeterTag tag = JsonConvert.DeserializeObject<MeterTag>(cr.Key);
                //            MeterValues values = JsonConvert.DeserializeObject<MeterValues>(cr.Value);

                //            TDengineWriter writer = new TDengineWriter();
                //            string sql = writer.GenerateSql(tag, values);
                //            writer.InsertData(conn, sql);

                //            //Console.WriteLine("{0} Key:{1} Value:{2} , TopicParition:{3} TopicPartitionOffset:{4}", DateTimeOffset.UtcNow, cr.Key, cr.Value, cr.TopicPartition, cr.TopicPartitionOffset);
                //            Console.WriteLine("{0} Key:{1} Value:{2} , TopicParition:{3} TopicPartitionOffset:{4}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff:ffffff"), tag.ToString(), values.ToString(), cr.TopicPartition, cr.TopicPartitionOffset);
                //        }
                //        catch (ConsumeException e)
                //        {
                //            Console.WriteLine("Error occured:{0}", e.Error.Reason);
                //        }
                //    }
                //}
                //catch (OperationCanceledException)
                //{
                //    c.Close();
                //}
            }
        }
    }
}
