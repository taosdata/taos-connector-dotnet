using System;
using System.Collections.Generic;
using TDengine.Driver;
using TDengine.Driver.Impl.WebSocketMethods;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;

namespace TDengine.TMQ.WebSocket
{
    public class Consumer : IConsumer
    {
        private readonly TMQOptions _options;
        private readonly TMQConnection _connection;
        private ulong _lastMessageId;

        public Consumer(ConsumerBuilder builder)
        {
            _options = new TMQOptions(builder.Config);
            _connection = new TMQConnection(_options.TDConnectIp);
        }

        public ConsumeResult Consume(int millisecondsTimeout)
        {
            var resp = _connection.Poll(millisecondsTimeout);
            if (!resp.HaveMessage)
            {
                return null;
            }

            var consumeResult = new ConsumeResult(resp.MessageId, resp.Topic, resp.VgroupId, resp.Offset,
                (TMQ_RES)resp.MessageType);
            _lastMessageId = resp.MessageId;
            if (!NeedGetData((TMQ_RES)resp.MessageType)) return null;
            while (true)
            {
                var dataMessage = new TmqMessage();
                var fetchResp = _connection.Fetch(resp.MessageId);
                if (fetchResp.Completed)
                {
                    break;
                }

                List<TDengineMeta> metaList = new List<TDengineMeta>(fetchResp.FieldsCount);
                for (int i = 0; i < fetchResp.FieldsCount; i++)
                {
                    var meta = new TDengineMeta
                    {
                        name = fetchResp.FieldsNames[i],
                        type = fetchResp.FieldsTypes[i],
                        size = (int)fetchResp.FieldsLengths[i]
                    };
                    metaList.Add(meta);
                }

                var block = _connection.FetchBlock(resp.MessageId);
                var data = new List<List<object>>(fetchResp.Rows);
                var br = new BlockReader(24, fetchResp.FieldsCount, fetchResp.Precision,
                    fetchResp.FieldsTypes.ToArray(), TimeZoneInfo.Local);
                br.SetBlock(block, fetchResp.Rows);

                for (int rowIndex = 0; rowIndex < fetchResp.Rows; rowIndex++)
                {
                    List<Object> dataRow = new List<object>(fetchResp.FieldsCount);
                    for (int colIndex = 0; colIndex < fetchResp.FieldsCount; colIndex++)
                    {
                        dataRow.Add(br.Read(rowIndex, colIndex));
                    }

                    data.Add(dataRow);
                }

                dataMessage.Datas = data;
                dataMessage.Metas = metaList;
                dataMessage.TableName = fetchResp.TableName;
                consumeResult.Message.Add(dataMessage);
            }

            return consumeResult;
        }

        public List<TopicPartition> Assignment
        {
            get
            {
                var result = new List<TopicPartition>();
                var topics = Subscription();
                foreach (var topic in topics)
                {
                    var resp = _connection.Assignment(topic);
                    foreach (var assignment in resp.Assignment)
                    {
                        result.Add(new TopicPartition(topic, assignment.VGroupId));
                    }
                }

                return result;
            }
        }

        public List<string> Subscription()
        {
            var resp = _connection.Subscription();
            return resp.Topics;
        }

        public void Subscribe(IEnumerable<string> topic)
        {
            _connection.Subscribe((List<string>)topic, _options);
        }

        public void Subscribe(string topic)
        {
            _connection.Subscribe(new List<string> { topic }, _options);
        }

        public void Unsubscribe()
        {
            _connection.Unsubscribe();
        }

        public void Commit(ConsumeResult consumerResult)
        {
            _connection.Commit(consumerResult.MessageId);
        }

        public List<TopicPartitionOffset> Commit()
        {
            _connection.Commit(_lastMessageId);
            return Committed(TimeSpan.Zero);
        }

        public void Commit(IEnumerable<TopicPartitionOffset> tpos)
        {
            foreach (var tpo in tpos)
            {
                _connection.CommitOffset(tpo.Topic, tpo.Partition, tpo.Offset);
            }
        }

        public void Seek(TopicPartitionOffset tpo)
        {
            _connection.Seek(tpo.Topic, tpo.Partition, tpo.Offset);
        }

        public List<TopicPartitionOffset> Committed(TimeSpan timeout)
        {
            var assignment = Assignment;
            var args = new List<WSTopicVgroupId>(assignment.Count);
            var result = new List<TopicPartitionOffset>(assignment.Count);
            foreach (var topicPartition in assignment)
            {
                args.Add(new WSTopicVgroupId
                {
                    Topic = topicPartition.Topic,
                    VGroupId = topicPartition.Partition,
                });
            }

            var resp = _connection.Committed(args);
            for (int i = 0; i < args.Count; i++)
            {
                result.Add(new TopicPartitionOffset(args[i].Topic, args[i].VGroupId, resp.Committed[i]));
            }

            return result;
        }

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
        {
            var args = new List<WSTopicVgroupId>();
            var result = new List<TopicPartitionOffset>();
            foreach (var topicPartition in partitions)
            {
                args.Add(new WSTopicVgroupId
                {
                    Topic = topicPartition.Topic,
                    VGroupId = topicPartition.Partition,
                });
            }

            var resp = _connection.Committed(args);
            for (int i = 0; i < args.Count; i++)
            {
                result.Add(new TopicPartitionOffset(args[i].Topic, args[i].VGroupId, resp.Committed[i]));
            }

            return result;
        }

        public Offset Position(TopicPartition partition)
        {
            var vgid = new List<WSTopicVgroupId>(1)
            {
                new WSTopicVgroupId
                {
                    Topic = partition.Topic,
                    VGroupId = partition.Partition
                }
            };
            var resp = _connection.Position(vgid);
            return resp.Position[0];
        }

        public void Close()
        {
            _connection.Close();
        }

        private bool NeedGetData(TMQ_RES type)
        {
            return type == TMQ_RES.TMQ_RES_DATA || type == TMQ_RES.TMQ_RES_METADATA;
        }
    }
}