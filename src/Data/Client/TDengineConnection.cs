using System;
using System.Data;
using System.Data.Common;
using TDengine.Data.Protocol;
using TDengine.Data.Protocol.Native;

namespace TDengine.Data.Client
{
    public class TDengineConnection : DbConnection
    {
        private string _connectionString;
        private ConnectionState _state;
        internal ITDengineClient client;
        internal object connection;
        internal TDengineConnectionStringBuilder ConnectionStringBuilder { get; set; }

        public TDengineConnection(string connectionString)
        {
            ConnectionString = connectionString;
            switch (ConnectionStringBuilder.Protocol)
            {
                case TDengineConnectionStringBuilder.ProtocolNative:
                    client = new Native();
                    break;
                
                case TDengineConnectionStringBuilder.ProtocolWebSocket:
                    throw new NotImplementedException();
                default:
                    throw new ArgumentException($"Invalid protocol parameter:{ConnectionStringBuilder.Protocol}");
            }
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotImplementedException();
        }

        public override void ChangeDatabase(string databaseName)
        {
            client.ChangeDatabase(connection, databaseName);
        }

        public override void Close()
        {
            if (State == ConnectionState.Closed) return;
            if (connection != null)
            {
                client.Close(connection);
            }

            SetState(ConnectionState.Closed);
        }

        protected override DbCommand CreateDbCommand()
        {
            return new TDengineCommand(this);
        }

        private void SetState(ConnectionState value)
        {
            var originalState = _state;
            if (originalState == value) return;
            _state = value;
            OnStateChange(new StateChangeEventArgs(originalState, value));
        }

        public override void Open()
        {
            if (State == ConnectionState.Open) return;
            object connection = null;
            client.Open(ConnectionStringBuilder, ref connection);
            this.connection = connection;
            SetState(ConnectionState.Open);
        }

        public sealed override string ConnectionString
        {
            get => _connectionString;
            set
            {
                _connectionString = value;
                ConnectionStringBuilder = new TDengineConnectionStringBuilder(value);
            }
        }

        public override string Database => ConnectionStringBuilder.Database;
        public override ConnectionState State => _state;
        public override string DataSource => ConnectionStringBuilder.Host;
        public override string ServerVersion => client.GetServerVersion(client);
    }
}