using System.Data;
using TDengine.Data.Client;
using TDengine.Driver;
using Xunit;

namespace Data.Tests
{
    public class TDengineConnectionTests
    {
        private TDengineConnection _connection;

        public TDengineConnectionTests()
        {
            _connection = new TDengineConnection("");
        }

        [Fact]
        public void ConnectionString_Property_Should_Set_ConnectionString_And_ConnectionStringBuilder()
        {
            // Arrange
            string connectionString = "username=root;password=taosdata";

            // Act
            _connection.ConnectionString = connectionString;

            // Assert
            Assert.Equal(connectionString, _connection.ConnectionString);
            Assert.NotNull(_connection.ConnectionStringBuilder);
            Assert.Equal(connectionString, _connection.ConnectionStringBuilder.ConnectionString);
        }

        [Fact]
        public void Database_Property_Should_Return_DatabaseName_From_ConnectionStringBuilder()
        {
            // Arrange
            string databaseName = "test";
            _connection.ConnectionStringBuilder = new TDengineConnectionStringBuilder($"db={databaseName}");

            // Act
            var result = _connection.Database;

            // Assert
            Assert.Equal(databaseName, result);
        }

        [Fact]
        public void State_Property_Should_Return_Initial_State_Closed()
        {
            // Arrange and Act
            var result = _connection.State;

            // Assert
            Assert.Equal(ConnectionState.Closed, result);
        }

        [Fact]
        public void DataSource_Property_Should_Return_Host_From_ConnectionStringBuilder()
        {
            // Arrange
            string host = "localhost";
            _connection.ConnectionStringBuilder = new TDengineConnectionStringBuilder($"Host={host}");

            // Act
            var result = _connection.DataSource;

            // Assert
            Assert.Equal(host, result);
        }

        [Fact]
        public void Close_Method_Should_Close_Connection()
        {
            // Arrange
            _connection.ConnectionString = "username=root;password=taosdata";
            _connection.Open();

            // Act
            _connection.Close();

            // Assert
            Assert.Equal(ConnectionState.Closed, _connection.State);
        }
    }
}
