using System;
using System.Collections.Generic;
using TDengineTMQ.Impl;


namespace TDengineTMQ
{
    internal class Consumer : IConsumer
    {
        private TMQSafeHandle tmqHandle = new TMQSafeHandle();
        private IntPtr comsumer = IntPtr.Zero;

        // construct Consumer with incoming configuration

        /// <summary>
        /// 
        /// </summary>
        /// <param name="builder"></param>
        internal Consumer(ConsumerBuilder builder)
        {
            //this.tmqHandle = new TMQSafeHandle();
            LibTMQ.Initialize(null);
            comsumer = tmqHandle.ConsumerNew(builder.Config);
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
            return tmqHandle.ConsumerPoll(this.comsumer, millisecondsTimeout);
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

        public void Commit(ConsumeResult consumerResult)
        {
            tmqHandle.CommitSync(this.comsumer, consumerResult);
        }

        public void CommitAsync(ConsumeResult consumerResult, Delegate Callback)
        {
            throw new NotImplementedException();
        }
    }
}
