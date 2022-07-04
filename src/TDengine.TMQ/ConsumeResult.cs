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
        internal string topic { get; set; }
        internal int vGroupId { get; set; }
        internal string db { get; set; }
        internal string table { get; set; }

        public TopicPartition(string topic, int vGroupId, string db, string table)
        {
            this.topic = topic;
            this.vGroupId = vGroupId;
            this.db = db;
            this.table = table;
        }
        public string ToString()
        {
            return $"topic:{this.topic}+\n + vGroupId:{this.vGroupId.ToString()} + \n + db:{this.db} + \n + table:{this.table}";
        }
    }

    /// <summary>
    ///  Represent consume result.
    /// </summary>
    public class ConsumeResult<TKey, TValue>
    {
        /// <summary>
        /// An instance of the <see cref="TopicPartition"/>
        /// </summary>
        public TKey key { get; }

        /// <summary>
        ///  An instance of KeyValuePair, 
        ///  key is List of <see cref="TDengineMeta"/> represents the meta info of the consumer result.
        ///  and value is <see cref="Object"/> which represents the retrieved data.
        /// </summary>
        public TValue value { get; }

        /// <summary>
        /// store the result pointer.
        /// </summary>


        public ConsumeResult(KeyValuePair<TKey, TValue> record)
        {
            this.key = record.Key;
            this.value = record.Value;
        }

    }
}
