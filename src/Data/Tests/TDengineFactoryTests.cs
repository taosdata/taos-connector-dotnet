using NUnit.Framework;
using System.Data.Common;
using TDengine.Data.Client;

namespace TDengine.Data.Tests
{
    [TestFixture]
    public class TDengineFactoryTests
    {
        [Test]
        public void CreateCommand_ShouldReturnInstanceOfTDengineCommand()
        {
            // Arrange
            var factory = new TDengineFactory();

            // Act
            var command = factory.CreateCommand();

            // Assert
            Assert.IsInstanceOf<TDengineCommand>(command);
        }

        [Test]
        public void CreateConnection_ShouldReturnInstanceOfTDengineConnection()
        {
            // Arrange
            var factory = new TDengineFactory();

            // Act
            var connection = factory.CreateConnection();

            // Assert
            Assert.IsInstanceOf<TDengineConnection>(connection);
        }

        [Test]
        public void CreateConnectionStringBuilder_ShouldReturnInstanceOfTDengineConnectionStringBuilder()
        {
            // Arrange
            var factory = new TDengineFactory();

            // Act
            var connectionStringBuilder = factory.CreateConnectionStringBuilder();

            // Assert
            Assert.IsInstanceOf<TDengineConnectionStringBuilder>(connectionStringBuilder);
        }

        [Test]
        public void CreateParameter_ShouldReturnInstanceOfTDengineParameter()
        {
            // Arrange
            var factory = new TDengineFactory();

            // Act
            var parameter = factory.CreateParameter();

            // Assert
            Assert.IsInstanceOf<TDengineParameter>(parameter);
        }
    }
}