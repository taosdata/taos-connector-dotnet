using System;
using System.Collections.Generic;
using TDengine.Data.Client;
using TDengine.Driver;
using TDengine.TMQ.Native;

namespace TDengine.TMQ
{
    public class ConsumerBuilder
    {
        protected internal IEnumerable<KeyValuePair<string, string>> Config { get; set; }

        public IConsumer Build()
        {
            var connectType = TDengineConstant.ProtocolNative;
            foreach (var kv in Config)
            {
                if (kv.Key == "td.connect.type")
                {
                    connectType = kv.Value;
                }
            }

            switch (connectType)
            {
                case TDengineConstant.ProtocolWebSocket:
                    return new WebSocket.Consumer(this);
                case TDengineConstant.ProtocolNative:
                    return new Consumer(this);
                default:
                    throw new ArgumentException($"Unsupported connect type: {connectType}");
            }
        }

        public ConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config)
        {
            Config = config;
        }
    }
}