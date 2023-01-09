using System;
using System.Collections.Generic;
using TDengineTMQ.Impl;


namespace TDengineTMQ
{
    internal class Consumer : IConsumer
    {
        private TMQSafeHandle tmqHandle = new TMQSafeHandle();
        private IntPtr consumer = IntPtr.Zero;

        // construct Consumer with incoming configuration

        /// <summary>
        /// 
        /// </summary>
        /// <param name="builder"></param>
        internal Consumer(ConsumerBuilder builder)
        {
            //this.tmqHandle = new TMQSafeHandle();
            LibTMQ.Initialize(null);
            consumer = tmqHandle.ConsumerNew(builder.Config);
        }

        public List<string> Subscription()
        {
            return tmqHandle.Subscription(this.consumer);
        }

        public void Close()
        {
            tmqHandle.ConsumerClose(this.consumer);
        }

        public ConsumeResult Consume(int millisecondsTimeout)
        {
            return tmqHandle.ConsumerPoll(this.consumer, millisecondsTimeout);
        }

        public void Subscribe(IEnumerable<string> topics)
        {
            tmqHandle.Subscribe(this.consumer, topics);
        }

        public void Subscribe(string topic)
        {
            tmqHandle.Subscribe(this.consumer, new[] { topic });
        }

        public void Unsubscribe()
        {
            tmqHandle.Unsubscribe(this.consumer);
        }

        public void Commit(ConsumeResult consumerResult)
        {
            tmqHandle.CommitSync(this.consumer, consumerResult);
        }

        public void CommitAsync(ConsumeResult consumerResult, Delegate Callback)
        {
            throw new NotImplementedException();
        }
    }
}
