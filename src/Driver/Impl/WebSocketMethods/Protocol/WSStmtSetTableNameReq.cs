using Newtonsoft.Json;

namespace TDengine.Driver.Impl.WebSocketMethods.Protocol
{
    public class WSStmtSetTableNameReq
    {
        [JsonProperty("req_id")] public ulong ReqId { get; set; }

        [JsonProperty("stmt_id")] public ulong StmtId { get; set; }

        [JsonProperty("name")] public string Name { get; set; }
    }
}