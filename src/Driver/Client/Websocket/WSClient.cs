using System;
using System.Diagnostics;
using TDengine.Driver.Impl.WebSocketMethods;

namespace TDengine.Driver.Client.Websocket
{
    public class WSClient : ITDengineClient
    {
        private ConnectionStringBuilder _builder;
        private Connection _connection;
        private TimeZoneInfo _tz;

        public WSClient(ConnectionStringBuilder builder)
        {
            Debug.Assert(builder.Protocol == TDengineConstant.ProtocolWebSocket);
            _tz = builder.Timezone;
            if (string.IsNullOrEmpty(builder.Token))
            {
                _connection = new Connection(builder.Host, builder.Username, builder.Password,
                    builder.Database, builder.ConnTimeout, builder.ReadTimeout, builder.WriteTimeout);
            }
            else
            {
                _connection = new Connection(builder.Host, builder.Database, builder.ConnTimeout, builder.ReadTimeout,
                    builder.WriteTimeout);
            }

            _connection.Connect();
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
            return new WSStmt(resp.StmtId, _tz,_connection);
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