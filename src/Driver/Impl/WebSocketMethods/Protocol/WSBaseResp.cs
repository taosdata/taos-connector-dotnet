using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public interface IWSBaseResp
    {
        [JsonProperty("code")] int Code { get; set; }

        [JsonProperty("message")] string Message { get; set; }

        [JsonProperty("action")] string Action { get; set; }

        [JsonProperty("req_id")] ulong ReqId { get; set; }

        [JsonProperty("timing")] long Timing { get; set; }
    }
}