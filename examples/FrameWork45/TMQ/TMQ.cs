using FrameWork45.Data;
using FrameWork45.UtilTools;
using System;
using TDengineTMQ;
using System.Collections.Generic;

namespace FrameWork45.TMQ
{
    internal class TMQExample
    {
        internal string table;
        internal string db;
        internal bool ifStable;
        internal IntPtr conn = IntPtr.Zero;
        string topic;
        private InitData data = new InitData();
        internal TMQExample(IntPtr conn, string topic, string db = "tmq_db", string table = "tmq", bool ifStable = false)
        {
            this.db = db;
            this.table = table;
            this.ifStable = ifStable;
            this.conn = conn;
            this.topic = topic;
        }
        internal void Prepare()
        {
            this.data.Create(this.conn, this.db, this.table, this.ifStable);
            this.data.InsertData(this.conn, this.db, this.ifStable ? this.table : null, this.ifStable ? "tmq_s_01" : this.table, 5);
            this.data.InsertData(this.conn, this.db, this.ifStable ? this.table : null, this.ifStable ? "tmq_s_02" : this.table, 5);
            this.data.InsertData(this.conn, this.db, this.ifStable ? this.table : null, this.ifStable ? "tmq_s_03" : this.table, 5);
        }

        internal void CreateTopic()
        {
            Tools.ExecuteUpdate(this.conn, $"create topic if not exists {this.topic}_1 as select * from tmq_s_01 ;");
            Tools.ExecuteUpdate(this.conn, $"create topic if not exists {this.topic}_2 as select * from tmq_s_02 ;");
            Tools.ExecuteUpdate(this.conn, $"create topic if not exists {this.topic}_3 as select * from tmq_s_03 ;");
        }
        internal void Dispose()
        {
            Tools.ExecuteUpdate(this.conn, $"drop topic if exists {this.topic}_1");
            Tools.ExecuteUpdate(this.conn, $"drop topic if exists {this.topic}_2");
            Tools.ExecuteUpdate(this.conn, $"drop topic if exists {this.topic}_3");
            data.Drop(this.conn, this.db, null); ;
        }

        //build consumer 
        internal void RunConsumer()
        {
            Prepare();
            CreateTopic();

            var cfg = new ConsumerConfig
            {
                GourpId = "TDengine-TMQ-C#",
                TDConnectUser = "root",
                TDConnectPasswd = "taosdata",
                MsgWithTableName = "true",
                TDConnectIp = "127.0.0.1",
            };
            var consumer = new ConsumerBuilder(cfg)
                .Build();
            Console.WriteLine("============= consumer has been created");
            //consumer
            {

                consumer.Subscribe(new List<string> { this.topic + "_1", this.topic + "_2", this.topic + "_3" });
                Console.WriteLine("================ subscribe topic {0}", this.topic);

                try
                {
                    int i = 0;
                    while (i < 10)
                    {
                        var consumerRes = consumer.Consume(30);
                        Console.WriteLine("================ consume {0} times ", i);


                        foreach (KeyValuePair<TopicPartition, TaosResult> kv in consumerRes.Message)
                        {
                            Console.WriteLine("topic partitions:{0}", kv.Key.ToString());

                            kv.Value.Metas.ForEach(meta =>
                            {
                                Console.Write("{0} {1}({2}) \t|", meta.name, meta.TypeName(), meta.size);
                            });
                            Console.WriteLine("");
                            kv.Value.Datas.ForEach(data =>
                            {
                                Console.WriteLine(data.ToString());
                            });
                        }
                        i++;
                        consumer.Commit(consumerRes);
                        Console.WriteLine("\n================ commit ");

                    }
                    Console.WriteLine("================ subscription ");

                    List<string> topics = consumer.Subscription();
                    topics.ForEach(t => Console.WriteLine("topic name:{0}", t));
                }
                finally
                {
                    consumer.Unsubscribe();
                    Console.WriteLine("================ unsubscribe ");

                    consumer.Close();
                    Console.WriteLine("================ close consumer ");
                }
            }
            Dispose();
        }

    }
}
