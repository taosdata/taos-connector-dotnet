using System;
using System.Collections.Generic;
using TDengineDriver;


namespace TDengineTMQ
{
    /// <summary>
    /// Represent the consumer result's TKey.
    /// </summary>
    public struct TopicPartition
    {
        public string topic { get; set; }
        public int vGroupId { get; set; }
        public string db { get; set; }
        public string table { get; set; }

        public TopicPartition(string topic, int vGroupId, string db, string table)
        {
            this.topic = topic;
            this.vGroupId = vGroupId;
            this.db = db;
            this.table = table;
        }
        public override string ToString()
        {
            return $"topic:{this.topic} \nvGroupId:{this.vGroupId} \ndb:{this.db}  \ntable:{this.table} \n";
        }
    }
    public class TaosResult
    {
        public List<TDengineMeta> Metas { get; set; }
        public List<Object> Datas { get; set; }

        public TaosResult(List<TDengineMeta> meta, List<Object> data)
        {
            this.Datas = data;
            this.Metas = meta;
        }

        public void AppendData(TaosResult result)
        {
            this.Datas.AddRange(result.Datas);
        }

    }
    /// <summary>
    ///  Represent consume result.
    /// </summary>
    public class ConsumeResult
    {
        public Dictionary<TopicPartition, TaosResult> Message { get; set; }

        public IntPtr Offset { get; set; }

        public ConsumeResult()
        {
            this.Message = new Dictionary<TopicPartition, TaosResult>();
            this.Offset = IntPtr.Zero;
        }
        public ConsumeResult(TopicPartition topicPartition, TaosResult taosRes, IntPtr offset)
        {
            if (Message == null)
            {
                Message = new Dictionary<TopicPartition, TaosResult>();
            }

            if (Message.ContainsKey(topicPartition))
            {
                Message[topicPartition].AppendData(taosRes);
            }
            else
            {
                Message.Add(topicPartition, taosRes);
            }
            this.Offset = offset;
        }

        public void Add(TopicPartition topicPartition, TaosResult taosRes)
        {
            if (Message.ContainsKey(topicPartition))
            {
                Message[topicPartition].AppendData(taosRes);
            }
            else
            {
                Message.Add(topicPartition, taosRes);
            }
        }
    
        public void Free()
        {
            TDengineDriver.TDengine.FreeResult(Offset);
        }
    }
}
