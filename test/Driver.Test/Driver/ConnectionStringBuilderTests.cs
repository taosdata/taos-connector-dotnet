using System;
using TDengine.Driver;
using Xunit;

namespace Driver.Test.Driver
{
    public class ConnectionStringBuilderTests
    {
        [Theory]
        [InlineData(-1)]
        [InlineData(65536)]
        public void PortSetterShouldRejectInvalidValues(int port)
        {
            var builder = new ConnectionStringBuilder(string.Empty);
            var ex = Assert.Throws<ArgumentException>(() => builder.Port = port);
            Assert.Equal("port", ex.ParamName);
        }
    }
}
