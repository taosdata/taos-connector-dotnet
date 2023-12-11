using System;
using System.Net.WebSockets;
using System.Threading;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;

namespace TDengine.Driver.Impl.WebSocketMethods
{
    public partial class Connection : BaseConnection
    {
        private readonly string _user = string.Empty;
        private readonly string _password = string.Empty;
        private readonly string _db = string.Empty;

        public Connection(string addr, string user, string password, string db, TimeSpan connectTimeout = default,
            TimeSpan readTimeout = default, TimeSpan writeTimeout = default) : base(addr, connectTimeout, readTimeout, writeTimeout)
        {
            _user = user;
            _password = password;
            _db = db;
        }

        public void Connect()
        {
            SendJsonBackJson<WSConnReq, WSConnResp>(WSAction.Conn, new WSConnReq
            {
                ReqId = _GetReqId(),
                User = _user,
                Password = _password,
                Db = _db
            });
        }

        public WSQueryResp Query(string sql, ulong reqid = default)
        {
            if (reqid == default)
            {
                reqid = _GetReqId();
            }

            return SendJsonBackJson<WSQueryReq, WSQueryResp>(WSAction.Query, new WSQueryReq
            {
                ReqId = reqid,
                Sql = sql
            });
        }

        public WSFetchResp Fetch(ulong resultId)
        {
            return SendJsonBackJson<WSFetchReq, WSFetchResp>(WSAction.Fetch, new WSFetchReq
            {
                ReqId = _GetReqId(),
                ResultId = resultId
            });
        }

        public byte[] FetchBlock(ulong resultId)
        {
            return SendJsonBackBytes(WSAction.FetchBlock, new WSFetchBlockReq
            {
                ReqId = _GetReqId(),
                ResultId = resultId
            });
        }

        public void FreeResult(ulong resultId)
        {
            SendJson(WSAction.FreeResult, new WSFreeResultReq
            {
                ReqId = _GetReqId(),
                ResultId = resultId
            });
        }
    }
}