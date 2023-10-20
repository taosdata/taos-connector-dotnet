using System;
using System.Collections.Generic;
using System.Threading;
using TDengine.Driver;
using TDengine.Driver.Client;
using TDengine.TMQ;
using Xunit;
using Xunit.Abstractions;

namespace Driver.Test.Client.TMQ.Native
{
    public class Consumer
    {
        private readonly ITestOutputHelper _output;

        public Consumer(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public void NewConsumerTest()
        {
            var builder =
                new ConnectionStringBuilder("host=localhost;port=6030;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    string[] sqlCommands =
                    {
                        "drop topic if exists test_tmq_common",
                        "drop database if exists af_test_tmq",
                        "create database if not exists af_test_tmq  vgroups 2  WAL_RETENTION_PERIOD 86400",
                        "use af_test_tmq",
                        "create stable if not exists all_type (ts timestamp," +
                        "c1 bool," +
                        "c2 tinyint," +
                        "c3 smallint," +
                        "c4 int," +
                        "c5 bigint," +
                        "c6 tinyint unsigned," +
                        "c7 smallint unsigned," +
                        "c8 int unsigned," +
                        "c9 bigint unsigned," +
                        "c10 float," +
                        "c11 double," +
                        "c12 binary(20)," +
                        "c13 nchar(20)" +
                        ") tags(t1 int)",
                        "create table if not exists ct0 using all_type tags(1000)",
                        "create table if not exists ct1 using all_type tags(2000)",
                        "create table if not exists ct2 using all_type tags(3000)",
                        "create topic if not exists test_tmq_common as stable all_type"
                    };
                    foreach (var sqlCommand in sqlCommands)
                    {
                        DoRequest(client, sqlCommand);
                    }

                    DateTime dateTime = DateTime.Now;
                    DateTime now = new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour,
                        dateTime.Minute,
                        dateTime.Second, dateTime.Millisecond, dateTime.Kind);
                    for (int i = 0; i < 3; i++)
                    {
                        var sql =
                            $"insert into ct{i} values('{now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK")}',true,2,3,4,5,6,7,8,9,10,11,'1','2')";
                        DoRequest(client, sql);
                    }

                    var cfg = new Dictionary<string, string>()
                    {
                        { "group.id", "test" },
                        { "auto.offset.reset", "earliest" },
                        { "td.connect.ip", "127.0.0.1" },
                        { "td.connect.user", "root" },
                        { "td.connect.pass", "taosdata" },
                        { "td.connect.port", "6030" },
                        { "client.id", "test_tmq_c" },
                        { "enable.auto.commit", "false" },
                        { "msg.with.table.name", "true" },
                    };

                    var consumer = new ConsumerBuilder(cfg).Build();
                    consumer.Subscribe("test_tmq_common");
                    var assignment = consumer.Assignment;
                    Assert.Equal(2, assignment.Count);
                    var topics = consumer.Subscription();
                    Assert.Single(topics);
                    Assert.Equal("test_tmq_common", topics[0]);
                    _output.WriteLine(assignment.ToString());
                    var position1 = consumer.Position(assignment[0]);
                    Assert.Equal(0, position1);
                    var position2 = consumer.Position(assignment[1]);
                    Assert.Equal(0, position2);
                    var messageCount = 0;
                    for (int i = 0; i < 5; i++)
                    {
                        using (var result = consumer.Consume(100))
                        {
                            if (messageCount == 3)
                            {
                                break;
                            }

                            if (result == null)
                            {
                                continue;
                            }

                            foreach (var message in result.Message)
                            {
                                messageCount += 1;
                                _output.WriteLine(message.TableName);
                                _output.WriteLine(message.Datas.ToString());
                            }

                            consumer.Commit(new List<TopicPartitionOffset>
                            {
                                result.TopicPartitionOffset,
                            });
                            var committed = consumer.Committed(new TopicPartition[] { result.TopicPartition },
                                TimeSpan.Zero);
                            Assert.Single(committed);
                            Assert.Equal(result.TopicPartitionOffset.Offset, committed[0].Offset);
                        }
                    }

                    Assert.Equal(3, messageCount);
                    consumer.Unsubscribe();
                    consumer.Close();
                }
                finally
                {
                    Thread.Sleep(3000);
                    DoRequest(client, "drop topic if exists test_tmq_common");
                    Thread.Sleep(3000);
                    DoRequest(client, "drop database if exists af_test_tmq");
                }
            }
        }

        [Fact]
        public void ConsumerSeekTest()
        {
            var builder =
                new ConnectionStringBuilder("host=localhost;port=6030;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    string[] sqlCommands =
                    {
                        "drop topic if exists test_tmq_seek",
                        "drop database if exists af_test_tmq_seek",
                        "create database if not exists af_test_tmq_seek  vgroups 2  WAL_RETENTION_PERIOD 86400",
                        "use af_test_tmq_seek",
                        "create stable if not exists all_type (ts timestamp," +
                        "c1 bool," +
                        "c2 tinyint," +
                        "c3 smallint," +
                        "c4 int," +
                        "c5 bigint," +
                        "c6 tinyint unsigned," +
                        "c7 smallint unsigned," +
                        "c8 int unsigned," +
                        "c9 bigint unsigned," +
                        "c10 float," +
                        "c11 double," +
                        "c12 binary(20)," +
                        "c13 nchar(20)" +
                        ") tags(t1 int)",
                        "create table if not exists ct0 using all_type tags(1000)",
                        "create table if not exists ct1 using all_type tags(2000)",
                        "create table if not exists ct2 using all_type tags(3000)",
                        "create topic if not exists test_tmq_seek as stable all_type"
                    };
                    foreach (var sqlCommand in sqlCommands)
                    {
                        DoRequest(client, sqlCommand);
                    }

                    DateTime dateTime = DateTime.Now;
                    DateTime now = new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour,
                        dateTime.Minute,
                        dateTime.Second, dateTime.Millisecond, dateTime.Kind);
                    for (int i = 0; i < 3; i++)
                    {
                        var sql =
                            $"insert into ct{i} values('{now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK")}',true,2,3,4,5,6,7,8,9,10,11,'1','2')";
                        DoRequest(client, sql);
                    }

                    var cfg = new Dictionary<string, string>()
                    {
                        { "group.id", "test" },
                        { "auto.offset.reset", "earliest" },
                        { "td.connect.ip", "127.0.0.1" },
                        { "td.connect.user", "root" },
                        { "td.connect.pass", "taosdata" },
                        { "td.connect.port", "6030" },
                        { "client.id", "test_tmq_c" },
                        { "enable.auto.commit", "false" },
                        { "msg.with.table.name", "true" },
                    };

                    var consumer = new ConsumerBuilder(cfg).Build();
                    consumer.Subscribe("test_tmq_seek");
                    var assignment = consumer.Assignment;
                    Assert.Equal(2, assignment.Count);
                    var topics = consumer.Subscription();
                    Assert.Single(topics);
                    Assert.Equal("test_tmq_seek", topics[0]);
                    _output.WriteLine(assignment.ToString());
                    var position1 = consumer.Position(assignment[0]);
                    Assert.Equal(0, position1);
                    var position2 = consumer.Position(assignment[1]);
                    Assert.Equal(0, position2);
                    // poll
                    var messageCount = 0;
                    for (int i = 0; i < 5; i++)
                    {
                        using (var result = consumer.Consume(100))
                        {
                            if (messageCount == 3)
                            {
                                break;
                            }

                            if (result == null)
                            {
                                continue;
                            }

                            foreach (var message in result.Message)
                            {
                                messageCount += 1;
                                _output.WriteLine(message.TableName);
                                _output.WriteLine(message.Datas.ToString());
                            }

                            consumer.Commit(new List<TopicPartitionOffset>
                            {
                                result.TopicPartitionOffset,
                            });
                            var committed = consumer.Committed(new TopicPartition[] { result.TopicPartition },
                                TimeSpan.Zero);
                            Assert.Single(committed);
                            Assert.Equal(result.TopicPartitionOffset.Offset, committed[0].Offset);
                        }
                    }

                    Assert.Equal(3, messageCount);
                    // seek
                    foreach (var topicPartition in assignment)
                    {
                        consumer.Seek(new TopicPartitionOffset(topicPartition.Topic, topicPartition.Partition, 0));
                    }
                    // poll after seek
                    messageCount = 0;
                    for (int i = 0; i < 5; i++)
                    {
                        using (var result = consumer.Consume(100))
                        {
                            if (messageCount == 3)
                            {
                                break;
                            }

                            if (result == null)
                            {
                                continue;
                            }

                            foreach (var message in result.Message)
                            {
                                messageCount += 1;
                                _output.WriteLine(message.TableName);
                                _output.WriteLine(message.Datas.ToString());
                            }

                            consumer.Commit(new List<TopicPartitionOffset>
                            {
                                result.TopicPartitionOffset,
                            });
                            var committed = consumer.Committed(new TopicPartition[] { result.TopicPartition },
                                TimeSpan.Zero);
                            Assert.Single(committed);
                            Assert.Equal(result.TopicPartitionOffset.Offset, committed[0].Offset);
                        }
                    }

                    Assert.Equal(3, messageCount);


                    consumer.Unsubscribe();
                    consumer.Close();
                }
                finally
                {
                    Thread.Sleep(3000);
                    DoRequest(client, "drop topic if exists test_tmq_seek");
                    Thread.Sleep(3000);
                    DoRequest(client, "drop database if exists af_test_tmq_seek");
                }
            }
        }
        
        [Fact]
        public void ConsumerCommitTest()
        {
            var builder =
                new ConnectionStringBuilder("host=localhost;port=6030;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    string[] sqlCommands =
                    {
                        "drop topic if exists test_tmq_commit",
                        "drop database if exists af_test_tmq_commit",
                        "create database if not exists af_test_tmq_commit  vgroups 2  WAL_RETENTION_PERIOD 86400",
                        "use af_test_tmq_commit",
                        "create stable if not exists all_type (ts timestamp," +
                        "c1 bool," +
                        "c2 tinyint," +
                        "c3 smallint," +
                        "c4 int," +
                        "c5 bigint," +
                        "c6 tinyint unsigned," +
                        "c7 smallint unsigned," +
                        "c8 int unsigned," +
                        "c9 bigint unsigned," +
                        "c10 float," +
                        "c11 double," +
                        "c12 binary(20)," +
                        "c13 nchar(20)" +
                        ") tags(t1 int)",
                        "create table if not exists ct0 using all_type tags(1000)",
                        "create table if not exists ct1 using all_type tags(2000)",
                        "create table if not exists ct2 using all_type tags(3000)",
                        "create topic if not exists test_tmq_commit as stable all_type"
                    };
                    foreach (var sqlCommand in sqlCommands)
                    {
                        DoRequest(client, sqlCommand);
                    }

                    DateTime dateTime = DateTime.Now;
                    DateTime now = new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour,
                        dateTime.Minute,
                        dateTime.Second, dateTime.Millisecond, dateTime.Kind);
                    for (int i = 0; i < 3; i++)
                    {
                        var sql =
                            $"insert into ct{i} values('{now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK")}',true,2,3,4,5,6,7,8,9,10,11,'1','2')";
                        DoRequest(client, sql);
                    }

                    var cfg = new Dictionary<string, string>()
                    {
                        { "group.id", "test" },
                        { "auto.offset.reset", "earliest" },
                        { "td.connect.ip", "127.0.0.1" },
                        { "td.connect.user", "root" },
                        { "td.connect.pass", "taosdata" },
                        { "td.connect.port", "6030" },
                        { "client.id", "test_tmq_c" },
                        { "enable.auto.commit", "false" },
                        { "msg.with.table.name", "true" },
                    };

                    var consumer = new ConsumerBuilder(cfg).Build();
                    consumer.Subscribe("test_tmq_commit");
                    var assignment = consumer.Assignment;
                    Assert.Equal(2, assignment.Count);
                    var topics = consumer.Subscription();
                    Assert.Single(topics);
                    Assert.Equal("test_tmq_commit", topics[0]);
                    _output.WriteLine(assignment.ToString());
                    var position1 = consumer.Position(assignment[0]);
                    Assert.Equal(0, position1);
                    var position2 = consumer.Position(assignment[1]);
                    Assert.Equal(0, position2);
                    // poll
                    var messageCount = 0;
                    for (int i = 0; i < 5; i++)
                    {
                        using (var result = consumer.Consume(100))
                        {
                            if (messageCount == 3)
                            {
                                break;
                            }

                            if (result == null)
                            {
                                continue;
                            }

                            foreach (var message in result.Message)
                            {
                                messageCount += 1;
                                _output.WriteLine(message.TableName);
                                _output.WriteLine(message.Datas.ToString());
                            }

                            var committed = consumer.Commit();
                            
                            Assert.Equal(2,committed.Count);
                            foreach (var c in committed)
                            {
                                if (c.Partition == result.Partition)
                                {
                                    Assert.NotEqual(0,c.Offset);
                                }
                            }

                            var allCommitted = consumer.Committed(TimeSpan.Zero);
                            Assert.Equal(committed, allCommitted);
                        }
                    }

                    Assert.Equal(3, messageCount);
                    consumer.Unsubscribe();
                    consumer.Close();
                }
                finally
                {
                    Thread.Sleep(3000);
                    DoRequest(client, "drop topic if exists test_tmq_commit");
                    Thread.Sleep(3000);
                    DoRequest(client, "drop database if exists af_test_tmq_commit");
                }
            }
        }
        
        private void DoRequest(ITDengineClient client, string sql)
        {
            client.Exec(sql);
        }
    }
}