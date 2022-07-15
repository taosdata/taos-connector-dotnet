using System;
using System.Collections.Generic;

namespace TDengineTMQ
{
    public class ConsumerBuilder
    {
        internal protected IEnumerable<KeyValuePair<string, string>> Config { get; set; }

        public virtual IConsumer Build()
        {
            return new Consumer(this);
        }

        public ConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config)
        {
            this.Config = config;
        }
    }
}
