using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSTMQPollReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("blocking_time")] public long BlockingTime { get; set; }
    }
}