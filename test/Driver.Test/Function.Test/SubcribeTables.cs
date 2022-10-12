using System;
using System.Collections.Generic;
using TDengineDriver;
using TDengineTMQ;
using Test.Case.Attributes;
using Test.Fixture;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Function.Test.TMQ
{
    [TestCaseOrderer("XUnit.Case.Orderers.TestExeOrderer", "Cases.ExeOrder")]
    [Collection("Database collection")]

    public class SubscribeTable
    {
        readonly DatabaseFixture database;
        private readonly ITestOutputHelper _output;

        public SubscribeTable(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this.database = fixture;
            this._output = output;

        }
        /// <author>xiaolei</author>
        /// <Name>SubscribeTable.NormalTable</Name>
        /// <describe>Subscribe a table and consume from beginning.</describe>
        /// <filename>SubscribeTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "SubscribeTable.NormalTable()"), TestExeOrder(1), Trait("Category", "NormalTable")]
        public void NormalTable()
        {
            IntPtr conn = database.Conn;
            string tableName = "tmq_tb0";
            string topic = "topic_t0";
            string createSql = Tools.CreateTable(tableName, false, false);
            string createTopic = $"create topic if not exists {topic} as select * from {tableName}";
            string dropTopic = $"drop topic if exists {topic}";

            List<object> columns = Tools.ColumnsList(5);
            List<object> tags = Tools.TagsList(1);
            string insertSql = Tools.ConstructInsertSql($"{tableName}_s1", tableName, columns, tags, 5);

            List<object> expectResData = Tools.ConstructResData(columns, tags, 5);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);

            var cfg = new ConsumerConfig
            {
                GourpId = "c#_1",
                TDConnectUser = "root",
                TDConnectPasswd = "taosdata",
                MsgWithTableName = "true",
                TDConnectIp = "127.0.0.1",
                EnableAutoCommit = "true",
            };

            // prepare data
            Tools.ExecuteUpdate(conn, createSql, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);

            // create topic 
            Tools.ExecuteUpdate(conn, createTopic, _output);

            // consumer
            var consumer = new ConsumerBuilder(cfg).Build();
            consumer.Subscribe(topic);
            List<string> subTopics = consumer.Subscription();

            Assert.Equal(topic, subTopics[0]);

            for (int i = 0; i < 5; i++)
            {
                ConsumeResult consumerResult = consumer.Consume(200);

                foreach (KeyValuePair<TopicPartition, TaosResult> kv in consumerResult.Message)
                {
                    if (kv.Key.table == tableName)
                    {
                        // assert meta
                        for (int k = 0; k < expectResMeta.Count; k++)
                        {
                            Assert.Equal(expectResMeta[k].name, kv.Value.Metas[k].name);
                            Assert.Equal(expectResMeta[k].type, kv.Value.Metas[k].type);
                            Assert.Equal(expectResMeta[k].size, kv.Value.Metas[k].size);
                        }
                        // assert data
                        for (int j = 0; j < expectResData.Count; j++)
                        {
                            Assert.Equal(expectResData[j], kv.Value.Datas[j]);
                        }
                    }

                }
                consumer.Commit(consumerResult);
                _output.WriteLine(" ======= consume {0} done", i);
            }

            // dispose
            consumer.Unsubscribe();
            Tools.ExecuteUpdate(conn, dropTopic, _output);
            consumer.Close();

        }

        /// <author>xiaolei</author>
        /// <Name>SubscribeCases.ConsumeFromLastProgress</Name>
        /// <describe>Subscribe table from the last progress.</describe>
        /// <filename>Subscribe.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(Skip = "SubscribeTable.MultiTables()"), TestExeOrder(2), Trait("Category", "MultiTables")]
        public void MultiTables()
        {
            IntPtr conn = database.Conn;
            IntPtr _res = IntPtr.Zero;

            string[] tableName = new string[] { "tmq_tb1", "tmq_tb2", "tmq_jb1" };
            string[] topic = new string[] { "topic_t1", "topic_t2", "topic_j1" };

            string[] createSql = new string[3];
            string[] createTopic = new string[3];
            string[] insertSql = new string[3];
            string[] dropTopic = new string[3];
            List<object> columns = Tools.ColumnsList(5);

            List<List<object>> expectResData = new List<List<object>>(3);
            List<List<TDengineMeta>> expectResMeta = new List<List<TDengineMeta>>(3);
            List<List<object>> tags = new List<List<object>>(3);

            // prepare SQL
            for (int i = 0; i < 3; i++)
            {
                switch (i)
                {
                    // table 
                    case 0:
                        tags.Add(new List<object>());
                        createSql[i] = Tools.CreateTable(tableName[i], false, false);
                        insertSql[i] = Tools.ConstructInsertSql(tableName[i], "", columns, null, 5);
                        expectResData.Add(columns);
                        expectResMeta.Add(Tools.GetMetaFromDDL(createSql[i]));
                        break;
                    // stable
                    case 1:
                        tags.Add(Tools.TagsList(2, false));
                        createSql[i] = Tools.CreateTable(tableName[i], true, false);
                        insertSql[i] = Tools.ConstructInsertSql($"{tableName[i]}_s1", tableName[i], columns, tags[i], 5);
                        expectResData.Add(Tools.ConstructResData(columns, tags[i], 5));
                        expectResMeta.Add(Tools.GetMetaFromDDL(createSql[i]));
                        break;
                    // JSON
                    case 2:
                        tags.Add(Tools.TagsList(3, true));
                        createSql[i] = Tools.CreateTable(tableName[i], true, true);
                        insertSql[i] = Tools.ConstructInsertSql($"{tableName[i]}_j1", tableName[i], columns, tags[i], 5);
                        expectResData.Add(Tools.ConstructResData(columns, tags[i], 5));
                        expectResMeta.Add(Tools.GetMetaFromDDL(createSql[i]));
                        break;
                }
                createTopic[i] = $"create topic if not exists {topic[i]} as select * from {tableName[i]}";
                dropTopic[i] = $"drop topic if exists {topic[i]}";
            }

            // create table,topic

            for (int i = 0; i < 3; i++)
            {
                Tools.ExecuteUpdate(conn, createSql[i], _output);
                Tools.ExecuteUpdate(conn, insertSql[i], _output);
                Tools.ExecuteUpdate(conn, createTopic[i], _output);
            }

            var cfg = new ConsumerConfig
            {
                GourpId = "c#_2",
                TDConnectUser = "root",
                TDConnectPasswd = "taosdata",
                MsgWithTableName = "true",
                TDConnectIp = "127.0.0.1",
                EnableAutoCommit = "true",
            };


            // consumer
            var consumer = new ConsumerBuilder(cfg).Build();
            consumer.Subscribe(topic);
            List<string> subTopics = consumer.Subscription();

            subTopics.ForEach(tpc =>
            {
                Assert.Contains(tpc, topic);
            });

            for (int i = 0; i < 5; i++)
            {
                ConsumeResult consumerResult = consumer.Consume(200);
                foreach (KeyValuePair<TopicPartition, TaosResult> kv in consumerResult.Message)
                {
                    var tmpMeta = kv.Value.Metas;
                    var tmpData = kv.Value.Datas;
                    switch (kv.Key.table)
                    {
                        // if normal table 
                        case "tmq_tb1":
                            _output.WriteLine("tmq_tb1");
                            tmpMeta.ForEach(meta =>
                            {
                                Assert.Equal(expectResMeta[0][tmpMeta.IndexOf(meta)].name, meta.name);
                                Assert.Equal(expectResMeta[0][tmpMeta.IndexOf(meta)].name, meta.name);
                                Assert.Equal(expectResMeta[0][tmpMeta.IndexOf(meta)].name, meta.name);
                            });
                            tmpData.ForEach(data =>
                            {
                                Assert.Contains(data, expectResData[0]);
                            });
                            break;
                        // if stable
                        case "tmq_tb2_s1":
                            _output.WriteLine("tmq_tb2_s1");
                            tmpMeta.ForEach(meta =>
                            {
                                Assert.Equal(expectResMeta[1][tmpMeta.IndexOf(meta)].name, meta.name);
                                Assert.Equal(expectResMeta[1][tmpMeta.IndexOf(meta)].name, meta.name);
                                Assert.Equal(expectResMeta[1][tmpMeta.IndexOf(meta)].name, meta.name);
                            });
                            tmpData.ForEach(data =>
                            {
                                Assert.Contains(data, expectResData[1]);
                            });
                            break;
                        // if JSON table
                        case "tmq_jb1_j1":
                            _output.WriteLine("tmq_jb1_j1");
                            tmpMeta.ForEach(meta =>
                            {
                                Assert.Equal(expectResMeta[2][tmpMeta.IndexOf(meta)].name, meta.name);
                                Assert.Equal(expectResMeta[2][tmpMeta.IndexOf(meta)].name, meta.name);
                                Assert.Equal(expectResMeta[2][tmpMeta.IndexOf(meta)].name, meta.name);
                            });
                            tmpData.ForEach(data =>
                            {
                                Assert.Contains(data, expectResData[2]);
                            });
                            break;
                        default:
                            throw new Exception($"Unexpected table name {kv.Key.table}");
                    }

                }
                consumer.Commit(consumerResult);

                _output.WriteLine("======= consume {0} done", i);
            }
            // assert 

            // dispose
            consumer.Unsubscribe();
            foreach (var s in dropTopic)
            {
                Tools.ExecuteUpdate(conn, s, _output);
            }
            consumer.Close();
        }
    }

}