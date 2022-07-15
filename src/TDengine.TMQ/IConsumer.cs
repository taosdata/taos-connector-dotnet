using System;
using System.Collections.Generic;

namespace TDengineTMQ
{
    public interface IConsumer
    {
        ConsumeResult Consume(int millisecondsTimeout);

        ConsumeResult Consume(TimeSpan timeout);

        List<string> Subscription();

        void Subscribe(IEnumerable<string> topic);

        void Subscribe(string topic);

        /// <summary>
        /// Unsubscribe from current subscription
        /// </summary>
        void Unsubscribe();

        //commit
        void Commit(ConsumeResult consumerResult);

        void Close();

    }
}

