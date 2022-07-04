using System;
using System.Collections.Generic;

namespace TDengineTMQ
{
    public interface IConsumer<TKey, TValue>
    {
        ConsumeResult<TKey, TValue> Consume(int millisecondsTimeout);

        ConsumeResult<TKey, TValue> Consume(TimeSpan timeout);

        List<string> Subscription();
        void Subscribe(IEnumerable<string> topic);

        void Subscribe(string topic);
        /// <summary>
        /// Unsubscribe from current subscription
        /// </summary>
        void Unsubscribe();

        //commit
        void Commit(ConsumeResult<TKey, TValue> consumerResult);

        void Close();

        //DLL_EXPORT tmq_t *tmq_consumer_new(tmq_conf_t* conf, char* errstr, int32_t errstrLen);
        //DLL_EXPORT int32_t   tmq_commit_sync(tmq_t* tmq, const TAOS_RES* msg);
        //DLL_EXPORT void tmq_commit_async(tmq_t* tmq, const TAOS_RES* msg, tmq_commit_cb *cb, void* param);
    }
}

