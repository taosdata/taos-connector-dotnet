using System;
using NUnit.Framework;
using System.Data;
using TDengine.Data.Client;

namespace TDengine.Data.Tests
{
    [TestFixture]
    public class TDengineCommandTests
    {
        private TDengineConnection _connection;

        [SetUp]
        public void Setup()
        {
            // Initialize and open the connection before each test
            _connection = new TDengineConnection("username=root;password=taosdata");
            _connection.Open();
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "create database if not exists test_command";
                command.ExecuteNonQuery();
                _connection.ChangeDatabase("test_command");
                command.CommandText = "create table if not exists t (ts timestamp,v int)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t VALUES (?,?)";
                command.Parameters.AddWithValue(DateTime.Now);
                command.Parameters.AddWithValue(123);
            }
        }

        [TearDown]
        public void Cleanup()
        {
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "drop database if exists test_command";
                command.ExecuteNonQuery();
            }
            // Close the connection after each test
            _connection.Close();
            _connection.Dispose();
        }

        [Test]
        public void ExecuteNonQuery_WithValidCommand_ReturnsAffectedRows()
        {
            // Arrange
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "create table if not exists t1 (ts timestamp,v int)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t1 VALUES (?,?)";
                command.Parameters.AddWithValue(DateTime.Now);
                command.Parameters.AddWithValue(123);

                // Act
                int affectedRows = command.ExecuteNonQuery();

                // Assert
                Assert.AreEqual(1, affectedRows);
            }
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "create table if not exists t2 (ts timestamp,v binary(16))";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t2 VALUES (?,?)";
                command.Parameters.AddWithValue(DateTime.Now);
                command.Parameters.AddWithValue("5binary");

                // Act
                int affectedRows = command.ExecuteNonQuery();

                // Assert
                Assert.AreEqual(1, affectedRows);
            }
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "create table if not exists t3 (ts timestamp,v nchar(16))";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t3 VALUES (?,?)";
                command.Parameters.AddWithValue(DateTime.Now);
                command.Parameters.AddWithValue("9nchar");

                // Act
                int affectedRows = command.ExecuteNonQuery();

                // Assert
                Assert.AreEqual(1, affectedRows);
            }
        }

        [Test]
        public void ExecuteScalar_WithValidCommand_ReturnsSingleValue()
        {
            // Arrange
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "SELECT COUNT(*) FROM t";

                // Act
                object result = command.ExecuteScalar();

                // Assert
                Assert.IsNotNull(result);
                Assert.IsInstanceOf<long>(result);
            }
        }

        [Test]
        public void Prepare_WithValidCommand_DoesNotThrowException()
        {
            // Arrange
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "SELECT * FROM t";

                // Act & Assert
                Assert.DoesNotThrow(() => command.Prepare());
            }
        }

        [Test]
        public void ExecuteDbDataReader_WithValidCommand_ReturnsDbDataReader()
        {
            // Arrange
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "SELECT * FROM t";

                // Act
                using (var reader = command.ExecuteReader())
                {
                    // Assert
                    Assert.IsNotNull(reader);
                    Assert.IsInstanceOf<TDengineDataReader>(reader);
                }
            }
        }
    }
}