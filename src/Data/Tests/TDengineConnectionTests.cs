using NUnit.Framework;
using System.Data;
using TDengine.Data.Client;

namespace TDengine.Data.Tests
{
    [TestFixture]
    public class TDengineConnectionTests
    {
        private TDengineConnection _connection;

        [SetUp]
        public void SetUp()
        {
            _connection = new TDengineConnection("");
        }
       


        [Test]
        public void ConnectionString_Property_Should_Set_ConnectionString_And_ConnectionStringBuilder()
        {
            // Arrange
            string connectionString = "username=root;password=taosdata";

            // Act
            _connection.ConnectionString = connectionString;

            // Assert
            Assert.AreEqual(connectionString, _connection.ConnectionString);
            Assert.IsNotNull(_connection.ConnectionStringBuilder);
            Assert.AreEqual(connectionString, _connection.ConnectionStringBuilder.ConnectionString);
        }

        [Test]
        public void Database_Property_Should_Return_DatabaseName_From_ConnectionStringBuilder()
        {
            // Arrange
            string databaseName = "test";
            _connection.ConnectionStringBuilder = new TDengineConnectionStringBuilder($"db={databaseName}");

            // Act
            var result = _connection.Database;

            // Assert
            Assert.AreEqual(databaseName, result);
        }

        [Test]
        public void State_Property_Should_Return_Initial_State_Closed()
        {
            // Arrange and Act
            var result = _connection.State;

            // Assert
            Assert.AreEqual(ConnectionState.Closed, result);
        }

        [Test]
        public void DataSource_Property_Should_Return_Host_From_ConnectionStringBuilder()
        {
            // Arrange
            string host = "localhost";
            _connection.ConnectionStringBuilder = new TDengineConnectionStringBuilder($"Host={host}");

            // Act
            var result = _connection.DataSource;

            // Assert
            Assert.AreEqual(host, result);
        }
        
        [Test]
        public void Close_Method_Should_Close_Connection()
        {
            // Arrange
            _connection.ConnectionString = "username=root;password=taosdata";
            _connection.Open();

            // Act
            _connection.Close();

            // Assert
            Assert.AreEqual(ConnectionState.Closed, _connection.State);
        }
    }
}