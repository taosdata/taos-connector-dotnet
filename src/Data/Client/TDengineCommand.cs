using System;
using System.Data;
using System.Data.Common;

namespace TDengine.Data.Client
{
    public class TDengineCommand : DbCommand
    {
        private readonly Lazy<TDengineParameterCollection> _parameters = new Lazy<TDengineParameterCollection>(
            () => new TDengineParameterCollection());

        private TDengineConnection _connection;
        private string _commandText;

        public TDengineCommand()
        {
            
        }

        public TDengineCommand(TDengineConnection connection)
        {
            _connection = connection;
        }
        public override void Cancel()
        {
        }

        public override int ExecuteNonQuery()
        {
            if (_connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"CallRequiresOpenConnection{nameof(ExecuteNonQuery)}");
            }

            if (_commandText == null)
            {
                throw new InvalidOperationException($"CallRequiresSetCommandText{nameof(ExecuteNonQuery)}");
            }

            int result;
            using (var rows = _connection.client.Statement(_connection.connection, _commandText,_parameters))
            {
                result = rows.AffectRows;
            }

            return result;
        }

        public override object ExecuteScalar()
        {
            if (_connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"CallRequiresOpenConnection{nameof(ExecuteScalar)}");
            }

            if (_commandText == null)
            {
                throw new InvalidOperationException($"CallRequiresSetCommandText{nameof(ExecuteScalar)}");
            }

            object result;
            using (var rows = _connection.client.Statement(_connection.connection, _commandText, _parameters))
            {
                result = rows.Read() ? rows.GetValue(0) : null;
            }

            return result;
        }

        public override void Prepare()
        {
            if (_connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"CallRequiresOpenConnection{nameof(Prepare)}");
            }

            if (string.IsNullOrEmpty(_commandText))
            {
                throw new InvalidOperationException($"CallRequiresSetCommandText{nameof(Prepare)}");
            }
        }

        public override string CommandText
        {
            get => _commandText;
            set => _commandText = value;
        }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType
        {
            get => CommandType.Text;
            set
            {
                if (value != CommandType.Text)
                {
                    throw new ArgumentException($"Invalid CommandType{value}");
                }
            }
        }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection DbConnection
        {
            get => _connection;
            set => _connection = (TDengineConnection)value;
        }

        protected override DbParameterCollection DbParameterCollection => _parameters.Value;
        protected override DbTransaction DbTransaction { get; set; }
        public override bool DesignTimeVisible { get; set; }

        protected override DbParameter CreateDbParameter() => new TDengineParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            var rows = _connection.client.Statement(_connection.connection, _commandText,_parameters);
            return new TDengineDataReader(rows);
        }

        public new virtual TDengineParameterCollection Parameters => _parameters.Value;
    }
}