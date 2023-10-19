using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSConnReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }
        [JsonProperty("user")] public string User { get; set; }
        [JsonProperty("password")] public string Password { get; set; }
        [JsonProperty("db")] public string Db { get; set; }
    }
}