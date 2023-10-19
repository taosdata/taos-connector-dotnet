using System;
using System.Collections.Generic;
using TDengine.Driver;
using TDengine.Driver.Impl.WebSocketMethods;
using NativeMethods = TDengine.Driver.Impl.NativeMethods.NativeMethods;

namespace TDengine.TMQ
{
    public class TmqMessage
    {
        public List<TDengineMeta> Metas { get; set; }
        public List<List<Object>> Datas { get; set; }

        public string TableName { get; set; }

        public TmqMessage()
        {
        }
    }

    /// <summary>
    ///  Represent consume result.
    /// </summary>
    public class ConsumeResult : IDisposable
    {
        /// <summary>
        ///     The topic associated with the message.
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        ///     The partition associated with the message.
        /// </summary>
        public Partition Partition { get; set; }

        /// <summary>
        ///     The partition offset associated with the message.
        /// </summary>
        public Offset Offset { get; set; }

        public IntPtr ResultPtr { get; set; }

        public ulong MessageId { get; }

        public TMQ_RES Type { get; set; }

        private TMQConnection wsConnection = null;
        /// <summary>
        ///     The TopicPartition associated with the message.
        /// </summary>
        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);

        /// <summary>
        ///     The TopicPartitionOffset associated with the message.
        /// </summary>
        public TopicPartitionOffset TopicPartitionOffset
        {
            get => new TopicPartitionOffset(Topic, Partition, Offset);
            set
            {
                Topic = value.Topic;
                Partition = value.Partition;
                Offset = value.Offset;
            }
        }

        public List<TmqMessage> Message { get; set; }

        public ConsumeResult(IntPtr resultPtr, string topic, Partition partition, Offset offset, TMQ_RES type)
        {
            ResultPtr = resultPtr;
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Type = type;
            Message = new List<TmqMessage>();
        }
        
        public ConsumeResult(ulong messageId,string topic, Partition partition, Offset offset, TMQ_RES type):this(IntPtr.Zero,topic, partition, offset, type)
        {
            MessageId = messageId;
        }

        public void Dispose()
        {
            if (ResultPtr != IntPtr.Zero)
            {
                NativeMethods.FreeResult(ResultPtr);
                ResultPtr = IntPtr.Zero;
            }
        }
    }
}