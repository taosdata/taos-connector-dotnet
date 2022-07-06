using System;
using TDengineTMQ.Impl;
using TDengineDriver;
using TDengineDriver.Impl;
using System.Timers;
using System.Collections.Generic;


namespace TDengineTMQ
{
    internal class Consumer<TKey, TValue> : IConsumer<TKey, TValue>
    {
        private TMQSafeHandle tmqHandle;
        private IntPtr comsumer;

        // construct Consumer with incoming configuration

        /// <summary>
        /// 
        /// </summary>
        /// <param name="builder"></param>
        internal Consumer(ConsumerBuilder<TKey, TValue> builder)
        {
            this.tmqHandle = new TMQSafeHandle();
            this.comsumer = tmqHandle.ConsumerNew(builder.Config);
        }

        public List<string> Subscription()
        {
            return tmqHandle.Subscription(this.comsumer);
        }

        public void Close()
        {
            tmqHandle.ConsumerClose(this.comsumer);
        }

        public ConsumeResult Consume(int millisecondsTimeout)
        {
            IntPtr msgPtr = tmqHandle.ConsumerPoll(this.comsumer, millisecondsTimeout);
            try
            {
                List<TDengineMeta> meta = LibTaos.GetMeta(msgPtr);
                List<Object> result = LibTaos.GetData(msgPtr);

                return new ConsumeResult
                {
                    TopicPartition = new TopicPartition(LibTMQ.tmq_get_topic_name(msgPtr), LibTMQ.tmq_get_vgroup_id(msgPtr), LibTMQ.tmq_get_db_name(msgPtr), LibTMQ.tmq_get_table_name(msgPtr), msgPtr)，
                    Key = meta,
                    Value = result
                };
            }
            finally
            {
                TDengine.FreeResult(msgPtr);
            }
 
        }

        public ConsumeResult<TKey, TValue> Consume(TimeSpan timeout)
        => Consume(timeout.TotalMillisecondsAsInt());

        public void Subscribe(IEnumerable<string> topics)
        {
            tmqHandle.Subscribe(this.comsumer, topics);
        }

        public void Subscribe(string topic)
        {
            tmqHandle.Subscribe(this.comsumer, new[] { topic });
        }

        public void Unsubscribe()
        {
            tmqHandle.Unsubscribe(this.comsumer);
        }

        public void Commit(ConsumeResult<TKey, TValue> consumerResult)
        {
            tmqHandle.CommitSync(this.comsumer, consumerResult);
        }

        public void CommitAsync(ConsumeResult<TKey, TValue> consumerResult, Delegate Callback)
        {
            throw new NotImplementedException();
        }
    }
}
