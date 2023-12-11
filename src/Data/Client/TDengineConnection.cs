using System;
using System.Data;
using System.Data.Common;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;


namespace TDengine.Data.Client
{
    public class TDengineConnection : DbConnection
    {
        private string _connectionString;
        private ConnectionState _state;
        internal ITDengineClient client;
        // internal object connection;
        public TDengineConnectionStringBuilder ConnectionStringBuilder { get; set; }

        public TDengineConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException();
        }

        public override void ChangeDatabase(string databaseName)
        {
            client.Exec("use `" + databaseName + "`;");
        }

        public override void Close()
        {
            if (State == ConnectionState.Closed) return;
            if (client != null)
            {
                client.Dispose();
                client = null;
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
            client = DbDriver.Open(ConnectionStringBuilder);
            SetState(ConnectionState.Open);
        }

        public sealed override string ConnectionString
        {
            get => _connectionString;
            set
            {
                if (State == ConnectionState.Open)
                {
                    throw new InvalidOperationException("Cannot change connection string on an open connection");
                }

                _connectionString = value;
                ConnectionStringBuilder = new TDengineConnectionStringBuilder(value);
            }
        }

        public override string Database => ConnectionStringBuilder.Database;
        public override ConnectionState State => _state;
        public override string DataSource => ConnectionStringBuilder.Host;

        public override string ServerVersion
        {
            get
            {
                var rows = client.Query("select server_version();");
                return Encoding.UTF8.GetString((byte[])rows.GetValue(0));
            }
        }
    }
}