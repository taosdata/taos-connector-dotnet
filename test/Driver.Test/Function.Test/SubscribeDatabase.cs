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
    public class SubscribeDatabase
    {
        readonly DatabaseFixture database;
        private readonly ITestOutputHelper _output;

        public SubscribeDatabase(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this.database = fixture;
            this._output = output;
        }

        /// <author>xiaolei</author>
        /// <Name>SubscribeTable.NormalTable</Name>
        /// <describe>Subscribe database.</describe>
        /// <filename>SubscribeDatabase.cs</filename>
        /// <result>pass or failed </result>
        [Fact(DisplayName = "SubscribeDatabase.DataBase()"), TestExeOrder(1), Trait("Category", "DataBase")]
        public void DataBase()
        {
            // constant
            IntPtr conn = this.database.Conn;
            string db = "tmq_sub_db";
            string topic = "topic_sub_db";
            string[] tables = new string[] { "tmq_tn", "tmq_ts", "tmq_tj" };
            string[] createSql = new string[3];
            string[] insertSql = new string[3];
            string createTopic = $"create topic if not exists {topic} as database {db}";
            string dropTopic = $"drop topic if exists {topic}";


            // data
            List<Object> columns = Tools.ColumnsList(5);
            List<List<object>> expectResData = new List<List<object>>(3);
            List<List<TDengineMeta>> expectResMeta = new List<List<TDengineMeta>>(3);
            List<List<object>> tags = new List<List<object>>(3);


            //SQL
            string createDB = Tools.CreateDB(db);
            ;
            string dropDB = Tools.DropDB(db);

            for (int i = 0; i < 3; i++)
            {
                switch (i)
                {
                    // normal table
                    case 0:
                        expectResData.Add(columns);
                        tags.Add(new List<object>());
                        createSql[i] = Tools.CreateTable($"{db}.{tables[i]}", false, false);
                        insertSql[i] = Tools.ConstructInsertSql($"{db}.{tables[i]}", "", columns, null, 5);
                        expectResMeta.Add(Tools.GetMetaFromDDL(createSql[0]));
                        expectResData.Add(columns);
                        break;
                    // stable
                    case 1:
                        tags.Add(Tools.TagsList(1, false));
                        createSql[i] = Tools.CreateTable($"{db}.{tables[i]}", true, false);
                        insertSql[i] = Tools.ConstructInsertSql($"{db}.{tables[i]}_s1", $"{db}.{tables[i]}", columns,
                            tags[i], 5);
                        expectResMeta.Add(Tools.GetMetaFromDDL(createSql[i]));
                        expectResData.Add(columns);
                        break;
                    // JSON
                    case 2:
                        tags.Add(Tools.TagsList(2, true));
                        createSql[i] = Tools.CreateTable($"{db}.{tables[i]}", true, true);
                        insertSql[i] = Tools.ConstructInsertSql($"{db}.{tables[i]}_j1", $"{db}.{tables[i]}", columns,
                            tags[i], 5);
                        expectResMeta.Add(Tools.GetMetaFromDDL(createSql[i]));
                        expectResData.Add(columns);
                        break;
                }
            }

            // create database,table,topic 
            Tools.ExecuteUpdate(conn, createDB, _output);
            for (int i = 0; i < 3; i++)
            {
                Tools.ExecuteUpdate(conn, createSql[i], _output);
                Tools.ExecuteUpdate(conn, insertSql[i], _output);
            }

            Tools.ExecuteUpdate(conn, createTopic, _output);

            var cfg = new ConsumerConfig
            {
                GroupId = "c#_3",
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
            Assert.Equal(subTopics[0], topic);

            for (int i = 0; i < 5; i++)
            {
                using (ConsumeResult consumeResult = consumer.Consume(100))
                {
                    if (consumeResult == null)
                    {
                        _output.WriteLine("====== consume {0} done", i);
                        continue;
                    }

                    foreach (var kv in consumeResult.Message)
                    {
                        var tmpMeta = kv.Metas;
                        var tmpData = kv.Datas;
                        switch (kv.TableName)
                        {
                            // if normal table 
                            case "tmq_tn":
                                _output.WriteLine("tmq_tn");
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
                                            Assert.Equal(ts,expectResData[0][j]);
                                            break;
                                        default:
                                            Assert.Equal(tmpData[0][j],expectResData[0][j]);
                                            break;
                                    }
                                }
                                break;
                            // if stable
                            case "tmq_ts_s1":
                                _output.WriteLine("tmq_ts_s1");
                                tmpMeta.ForEach(meta =>
                                {
                                    Assert.Equal(expectResMeta[1][tmpMeta.IndexOf(meta)].name, meta.name);
                                    Assert.Equal(expectResMeta[1][tmpMeta.IndexOf(meta)].name, meta.name);
                                    Assert.Equal(expectResMeta[1][tmpMeta.IndexOf(meta)].name, meta.name);
                                });
                                for (int j = 0; j < tmpData[0].Count; j++)
                                {
                                    switch (tmpData[0][j])
                                    {
                                        case DateTime val:
                                            var ts = TDengineConstant.ConvertDatetimeToTick(val,
                                                TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                                            Assert.Equal(ts,expectResData[1][j]);
                                            break;
                                        default:
                                            Assert.Equal(tmpData[0][j],expectResData[1][j]);
                                            break;
                                    }
                                }
                                break;
                            // if JSON table
                            case "tmq_tj_j1":
                                _output.WriteLine("tmq_tj_j1");
                                tmpMeta.ForEach(meta =>
                                {
                                    Assert.Equal(expectResMeta[2][tmpMeta.IndexOf(meta)].name, meta.name);
                                    Assert.Equal(expectResMeta[2][tmpMeta.IndexOf(meta)].name, meta.name);
                                    Assert.Equal(expectResMeta[2][tmpMeta.IndexOf(meta)].name, meta.name);
                                });
                                for (int j = 0; j < tmpData[0].Count; j++)
                                {
                                    switch (tmpData[0][j])
                                    {
                                        case DateTime val:
                                            var ts = TDengineConstant.ConvertDatetimeToTick(val,
                                                TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                                            Assert.Equal(ts,expectResData[2][j]);
                                            break;
                                        default:
                                            Assert.Equal(tmpData[0][j],expectResData[2][j]);
                                            break;
                                    }
                                }
                                break;
                            default:
                                throw new Exception($"Unexpected table name {kv.TableName}");
                        }
                    }

                    consumer.Commit(consumeResult);
                }

                _output.WriteLine("====== consume {0} done", i);
            }

            // dispose 
            consumer.Unsubscribe();
            consumer.Close();
            Tools.ExecuteUpdate(conn, dropTopic, _output);
            Tools.ExecuteUpdate(conn, dropDB, _output);
        }
    }
}