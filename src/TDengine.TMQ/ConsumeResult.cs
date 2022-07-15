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
        internal string? table { get; set; }

        public TopicPartition(string topic, int vGroupId, string db, string ? table)
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

    /// <summary>
    ///  Represent consume result.
    /// </summary>
    public class ConsumeResult
    {
        public List<TopicPartition> TopicPartitions { get; set; }
        /// <summary>
        /// An instance of the <see cref="TopicPartition"/>
        /// </summary>
        public List<List<TDengineMeta>> MetaList { get; set; }

        /// <summary>
        ///  An instance of KeyValuePair, 
        ///  key is List of <see cref="TDengineMeta"/> represents the meta info of the consumer result.
        ///  and value is <see cref="Object"/> which represents the retrieved data.
        /// </summary>
        public List<List<Object>> DataList { get; set; }

        /// <summary>
        /// store the result pointer.
        /// </summary>
        
        public IntPtr msg { get; set; }

        public ConsumeResult() 
        {
            this.TopicPartitions = new List<TopicPartition>();
            this.MetaList = new List<List<TDengineMeta>>();
            this.DataList  = new List<List<object>>();
        }
    }
}
