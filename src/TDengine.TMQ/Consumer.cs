using System;
using TDengineTMQ.Impl;
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

        public ConsumeResult<TKey, TValue> Consume(int millisecondsTimeout)
        {
            throw new NotImplementedException();
        }

        public ConsumeResult<TKey, TValue> Consume(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

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
            throw new NotImplementedException();
        }

        public void CommitAsync(ConsumeResult<TKey, TValue> consumerResult, Delegate Callback)
        {
            throw new NotImplementedException();
        }
    }
}
