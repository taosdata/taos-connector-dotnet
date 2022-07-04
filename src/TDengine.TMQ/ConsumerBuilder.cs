using System;
using System.Collections.Generic;

namespace TDengineTMQ
{
    public class ConsumerBuilder<TKey, TValue>
    {
        internal protected IEnumerable<KeyValuePair<string, string>> Config { get; set; }

        public virtual IConsumer<TKey, TValue> Build()
        {
            return new Consumer<TKey, TValue>(this);
        }

        public ConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config)
        {
            this.Config = config;
        }
    }
}
