using System;
using NUnit.Framework;
using System.Data;
using TDengine.Data.Client;

namespace TDengine.Data.Tests
{
    [TestFixture]
    public class TDengineParameterTests
    {
        [Test]
        public void ResetDbType_Should_Set_DbType_To_String()
        {
            // Arrange
            var parameter = new TDengineParameter();

            // Act
            parameter.ResetDbType();

            // Assert
            Assert.AreEqual(DbType.String, parameter.DbType);
        }

        [Test]
        public void Direction_Should_Be_Input()
        {
            // Arrange
            var parameter = new TDengineParameter();

            // Assert
            Assert.AreEqual(ParameterDirection.Input, parameter.Direction);
        }

        [Test]
        public void Setting_Non_Input_Direction_Should_Throw_ArgumentException()
        {
            // Arrange
            var parameter = new TDengineParameter();

            // Act and Assert
            Assert.Throws<ArgumentException>(() => parameter.Direction = ParameterDirection.Output);
        }

        [Test]
        public void ParameterName_Should_Be_Settable_And_Gettable()
        {
            // Arrange
            var parameter = new TDengineParameter();
            string parameterName = "testParameter";

            // Act
            parameter.ParameterName = parameterName;

            // Assert
            Assert.AreEqual(parameterName, parameter.ParameterName);
        }

        [Test]
        public void Setting_Null_Or_Empty_ParameterName_Should_Throw_ArgumentNullException()
        {
            // Arrange
            var parameter = new TDengineParameter();

            // Act and Assert
            Assert.Throws<ArgumentNullException>(() => parameter.ParameterName = null);
            Assert.Throws<ArgumentNullException>(() => parameter.ParameterName = string.Empty);
        }

        [Test]
        public void SourceColumn_Should_Be_Settable_And_Gettable()
        {
            // Arrange
            var parameter = new TDengineParameter();
            string sourceColumn = "testColumn";

            // Act
            parameter.SourceColumn = sourceColumn;

            // Assert
            Assert.AreEqual(sourceColumn, parameter.SourceColumn);
        }

        [Test]
        public void Value_Should_Be_Settable_And_Gettable()
        {
            // Arrange
            var parameter = new TDengineParameter();
            int value = 42;

            // Act
            parameter.Value = value;

            // Assert
            Assert.AreEqual(value, parameter.Value);
        }

        [Test]
        public void Size_Should_Return_Correct_Size()
        {
            // Arrange
            var parameter = new TDengineParameter();

            // Case 1: Test string value
            parameter.Value = "Hello, World!";
            Assert.AreEqual(13, parameter.Size);

            // Case 2: Test byte array
            parameter.Value = new byte[] { 0x12, 0x34, 0x56 };
            Assert.AreEqual(3, parameter.Size);

            // Case 3: Test default value
            parameter.Value = null;
            Assert.AreEqual(0, parameter.Size);
        }

        [Test]
        public void Setting_Negative_Size_Should_Throw_ArgumentOutOfRangeException()
        {
            // Arrange
            var parameter = new TDengineParameter();

            // Act and Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => parameter.Size = -1);
        }
    }
}
