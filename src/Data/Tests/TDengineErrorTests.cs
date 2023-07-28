using NUnit.Framework;
using TDengine.Data.Client;

namespace TDengine.Data.Tests
{
    [TestFixture]
    public class TDengineErrorTests
    {
        [Test]
        public void TDengineError_Initialization()
        {
            // Arrange
            int expectedCode = 123;
            string expectedError = "Some error message";

            // Act
            TDengineError error = new TDengineError(expectedCode, expectedError);

            // Assert
            Assert.AreEqual(expectedCode, error.Code);
            Assert.AreEqual(expectedError, error.Error);
            Assert.AreEqual($"code:[0x{expectedCode:x}],error:{expectedError}", error.Message);
        }
    }
}