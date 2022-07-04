using System;
using System.Collections.Generic;
using TDengineTMQ;
using Examples.Data;
using Examples.UtilsTools;
using TDengineDriver;
namespace Examples.TMQ
{
    internal class TMQExample
    {
        internal string table ;
        internal string db;
        internal bool ifStable ;
        internal IntPtr conn = IntPtr.Zero;
        List<string> topics;
        private InitData data = new InitData();
        internal TMQExample(IntPtr conn,string db = "tmq_db", string table = "tmq", bool ifStable = false)
        {
            this.db = db;
            this.table = table;
            this.ifStable = ifStable;
            this.conn = conn;
        }
        internal void Prepare()
        {
            this.data.Create(this.conn, this.db, this.table, this.ifStable);
            this.data.InsertData(this.conn, this.db, this.ifStable ? this.table : null, this.ifStable ? "tmq_s_01" : this.table, 5);
        }

        internal void CreateTopic(List<string> topic)
        {
            this.topics = topic;
        }
        internal void Dispose()
        {
            // drop topic??
            data.Drop(this.conn, this.db, null); ;
        }

        //build consumer 
        internal void RunConsumer(List<string> topics)
        {
            Prepare();
            CreateTopic(topics);

            var cfg = new ConsumerConfig
            {
                GourpId = "TDengine-TMQ-C#",
                TDConnectUser = "root",
                TDConnectPasswd = "taosdata",
                MsgWithTableName = true,
                EnableAutoCommit = true,
            };
            //setAutoCommitCallback
            var consumer = new ConsumerBuilder<TopicPartition, KeyValuePair<List<TDengineMeta>, List<Object>>>(cfg)
                .Build();
            //using (var consumer = new ConsumerBuilder<string,string>(cfg)
            //    .Build())
            {
                consumer.Subscribe(topics);
                try
                {
                    while (true)
                    {
                        var consumerRes = consumer.Consume(30);
                        Console.WriteLine($"consumerRes.topicPartition:{consumerRes.key.ToString()}");
                        foreach (var meta in consumerRes.value.Key)
                        {
                            Console.Write($"consumerRes.MetaList:{meta.ToString()}\t");
                        }
                        Console.WriteLine();
                        foreach (var record in consumerRes.value.Value)
                        {
                            Console.Write($"consumerRes.record:{record.ToString()}\t");
                        }

                        consumer.Commit(consumerRes);

                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    consumer.Close();
                }
            }
            Dispose();
        }

    }
}
