using System;
using System.Collections.Generic;

namespace TDengine.TMQ
{
    public interface IConsumer
    {
        ConsumeResult Consume(int millisecondsTimeout);
        
        List<TopicPartition> Assignment { get; }

        List<string> Subscription();

        void Subscribe(IEnumerable<string> topic);

        void Subscribe(string topic);

        /// <summary>
        /// Unsubscribe from current subscription
        /// </summary>
        void Unsubscribe();

        //commit
        void Commit(ConsumeResult consumerResult);

        List<TopicPartitionOffset> Commit();
        
        void Commit(IEnumerable<TopicPartitionOffset> offsets);
        
        void Seek(TopicPartitionOffset tpo);
        
        List<TopicPartitionOffset> Committed(TimeSpan timeout);
        
        List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout);

        Offset Position(TopicPartition partition);
        
        void Close();
    }
}