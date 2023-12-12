using System;
using System.Diagnostics;
using TDengine.Driver.Impl.WebSocketMethods;

namespace TDengine.Driver.Client.Websocket
{
    public class WSClient : ITDengineClient
    {
        private Connection _connection;
        private TimeZoneInfo _tz;

        public WSClient(ConnectionStringBuilder builder)
        {
            Debug.Assert(builder.Protocol == TDengineConstant.ProtocolWebSocket);
            _tz = builder.Timezone;
            _connection = new Connection(GetUrl(builder), builder.Username, builder.Password,
                builder.Database, builder.ConnTimeout, builder.ReadTimeout, builder.WriteTimeout);

            _connection.Connect();
        }

        private static string GetUrl(ConnectionStringBuilder builder)
        {
            var schema = "ws";
            var port = builder.Port;
            if (builder.UseSSL)
            {
                schema = "wss";
                if (builder.Port == 0)
                {
                    port = 443;
                }
            }
            else
            {
                if (builder.Port == 0)
                {
                    port = 6041;
                }
            }

            if (string.IsNullOrEmpty(builder.Token))
            {
                return $"{schema}://{builder.Host}:{port}/ws";
            }
            else
            {
                return $"{schema}://{builder.Host}:{port}/ws?token={builder.Token}";
            }
        }

        public void Dispose()
        {
            if (_connection != null)
            {
                _connection.Close();
                _connection = null;
            }
        }

        public IStmt StmtInit()
        {
            return StmtInit(ReqId.GetReqId());
        }

        public IStmt StmtInit(long reqId)
        {
            var resp = _connection.StmtInit((ulong)reqId);
            return new WSStmt(resp.StmtId, _tz, _connection);
        }

        public IRows Query(string query)
        {
            return Query(query, ReqId.GetReqId());
        }

        public IRows Query(string query, long reqId)
        {
            var resp = _connection.Query(query, (ulong)reqId);
            if (resp.IsUpdate)
            {
                return new WSRows(resp.AffectedRows);
            }

            return new WSRows(resp, _connection, _tz);
        }

        public long Exec(string query)
        {
            return Exec(query, ReqId.GetReqId());
        }

        public long Exec(string query, long reqId)
        {
            var resp = _connection.Query(query, (ulong)reqId);
            return resp.AffectedRows;
        }

        public void SchemalessInsert(string[] lines, TDengineSchemalessProtocol protocol,
            TDengineSchemalessPrecision precision,
            int ttl, long reqId)
        {
            var line = string.Join("\n", lines);
            _connection.SchemalessInsert(line, protocol, precision, ttl, reqId);
        }
    }
}