using System;
using System.Collections.Generic;
using TDengine.Driver;
using TDengine.TMQ;
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
            string insertSql = Tools.ConstructInsertSql($"{tableName}", "", columns, tags, 5);

            List<object> expectResData = Tools.ConstructResData(columns, tags, 5);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);

            var cfg = new ConsumerConfig
            {
                GroupId = "c#_1",
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
                using (ConsumeResult consumerResult = consumer.Consume(200))
                {
                    if (consumerResult == null)
                    {
                        _output.WriteLine(" ======= consume {0} done", i);
                        continue;
                    }

                    foreach (var kv in consumerResult.Message)
                    {
                        _output.WriteLine(" ======= consume {0} {1}", i, tableName);
                        if (kv.TableName == tableName)
                        {
                            // assert meta
                            for (int k = 0; k < expectResMeta.Count; k++)
                            {
                                Assert.Equal(expectResMeta[k].name, kv.Metas[k].name);
                                Assert.Equal(expectResMeta[k].type, kv.Metas[k].type);
                                Assert.Equal(expectResMeta[k].size, kv.Metas[k].size);
                            }

                            // assert data
                            for (int j = 0; j < expectResData.Count; j++)
                            {
                                switch (kv.Datas[0][j])
                                {
                                    case DateTime val:
                                        var ts = TDengineConstant.ConvertDatetimeToTick(val,
                                            TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                                        Assert.Equal(expectResData[j], ts);
                                        break;
                                    default:
                                        Assert.Equal(expectResData[j], kv.Datas[0][j]);
                                        break;
                                }
                            }
                        }
                    }

                    consumer.Commit(consumerResult);
                    _output.WriteLine(" ======= consume {0} done", i);
                }
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
        [Fact(DisplayName = "SubscribeTable.MultiTables()"), TestExeOrder(2), Trait("Category", "MultiTables")]
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
            List<object> columns = Tools.ColumnsList(1);

            List<List<object>> expectResData = new List<List<object>>(3);
            List<List<TDengineMeta>> expectResMeta = new List<List<TDengineMeta>>(3);
            List<List<object>> tags = new List<List<object>>(3);

            // prepare SQL

            // table 

            tags.Add(new List<object>());
            createSql[0] = Tools.CreateTable(tableName[0], false, false);
            insertSql[0] = Tools.ConstructInsertSql(tableName[0], "", columns, null, 1);
            expectResData.Add(columns);
            expectResMeta.Add(Tools.GetMetaFromDDL(createSql[0]));

            // stable
            tags.Add(Tools.TagsList(2, false));
            createSql[1] = Tools.CreateTable(tableName[1], true, false);
            insertSql[1] =
                Tools.ConstructInsertSql($"{tableName[1]}_s1", tableName[1], columns, tags[1], 1);
            expectResData.Add(Tools.ConstructResData(columns, tags[1], 1));
            expectResMeta.Add(Tools.GetMetaFromDDL(createSql[1]));
            // JSON
            tags.Add(Tools.TagsList(3, true));
            createSql[2] = Tools.CreateTable(tableName[2], true, true);
            insertSql[2] =
                Tools.ConstructInsertSql($"{tableName[2]}_j1", tableName[2], columns, tags[2], 1);
            expectResData.Add(Tools.ConstructResData(columns, tags[2], 1));
            expectResMeta.Add(Tools.GetMetaFromDDL(createSql[2]));

            for (int i = 0; i < 3; i++)
            {
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
                GroupId = "c#_2",
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

            subTopics.ForEach(tpc => { Assert.Contains(tpc, topic); });

            for (int i = 0; i < 5; i++)
            {
                using (ConsumeResult consumerResult = consumer.Consume(200))
                {
                    if (consumerResult == null)
                    {
                        _output.WriteLine("======= consume {0} done", i);
                        continue;
                    }

                    foreach (var kv in consumerResult.Message)
                    {
                        var tmpMeta = kv.Metas;
                        var tmpData = kv.Datas;
                        switch (consumerResult.Topic)
                        {
                            // if normal table 
                            case "topic_t1":
                                _output.WriteLine("tmq_tb1");
                                tmpMeta.ForEach(meta =>
                                {
                                    Assert.Equal(expectResMeta[0][tmpMeta.IndexOf(meta)].name, meta.name);
                                    Assert.Equal(expectResMeta[0][tmpMeta.IndexOf(meta)].name, meta.name);
                                    Assert.Equal(expectResMeta[0][tmpMeta.IndexOf(meta)].name, meta.name);
                                });
                                for (int j = 0; j < tmpData[0].Count; j++)
                                {
                                    switch (tmpData[0][j])
                                    {
                                        case DateTime val:
                                            var ts = TDengineConstant.ConvertDatetimeToTick(val,
                                                TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                                            Assert.Equal(ts, expectResData[0][j]);
                                            break;
                                        default:
                                            Assert.Equal(tmpData[0][j], expectResData[0][j]);
                                            break;
                                    }
                                }

                                break;
                            // if stable
                            case "topic_t2":
                                _output.WriteLine("tmq_tb2_s1");
                                tmpMeta.ForEach(meta =>
                                {
                                    Assert.Equal(expectResMeta[1][tmpMeta.IndexOf(meta)].name, meta.name);
                                    Assert.Equal(expectResMeta[1][tmpMeta.IndexOf(meta)].name, meta.name);
                                    Assert.Equal(expectResMeta[1][tmpMeta.IndexOf(meta)].name, meta.name);
                                });
                                for (int j = 0; j < expectResData[1].Count; j++)
                                {
                                    switch (tmpData[0][j])
                                    {
                                        case DateTime val:
                                            var ts = TDengineConstant.ConvertDatetimeToTick(val,
                                                TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                                            Assert.Equal(ts, expectResData[1][j]);
                                            break;
                                        default:
                                            Assert.Equal(tmpData[0][j], expectResData[1][j]);
                                            break;
                                    }
                                }

                                break;
                            // if JSON table
                            case "topic_j1":
                                _output.WriteLine("tmq_jb1_j1");
                                tmpMeta.ForEach(meta =>
                                {
                                    Assert.Equal(expectResMeta[2][tmpMeta.IndexOf(meta)].name, meta.name);
                                    Assert.Equal(expectResMeta[2][tmpMeta.IndexOf(meta)].name, meta.name);
                                    Assert.Equal(expectResMeta[2][tmpMeta.IndexOf(meta)].name, meta.name);
                                });
                                for (int j = 0; j < expectResData[2].Count; j++)
                                {
                                    switch (tmpData[0][j])
                                    {
                                        case DateTime val:
                                            var ts = TDengineConstant.ConvertDatetimeToTick(val,
                                                TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                                            Assert.Equal(ts, expectResData[2][j]);
                                            break;
                                        default:
                                            Assert.Equal(tmpData[0][j], expectResData[2][j]);
                                            break;
                                    }
                                }

                                break;
                            default:
                                throw new Exception($"Unexpected table name {kv.TableName}");
                        }
                    }

                    consumer.Commit(consumerResult);

                    _output.WriteLine("======= consume {0} done", i);
                }
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