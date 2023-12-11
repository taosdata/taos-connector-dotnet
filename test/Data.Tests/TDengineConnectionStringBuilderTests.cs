using TDengine.Data.Client;
using TDengine.Driver;
using Xunit;

namespace Data.Tests
{
    public class TDengineConnectionStringBuilderTests
    {
        [Fact]
        public void DefaultNative_ShouldSetDefaultValues()
        {
            var builder = new TDengineConnectionStringBuilder("");

            builder.DefaultNative();

            Assert.Equal(6030, builder.Port);
            Assert.Equal(string.Empty, builder.Host);
            Assert.Equal(TDengineConstant.ProtocolNative, builder.Protocol);
        }

        [Fact]
        public void DefaultWebSocket_ShouldSetDefaultValues()
        {
            var builder = new TDengineConnectionStringBuilder("");

            builder.DefaultWebSocket();

            Assert.Equal(6041, builder.Port);
            Assert.Equal("localhost", builder.Host);
            Assert.Equal(TDengineConstant.ProtocolWebSocket, builder.Protocol);
        }

        [Fact]
        public void Parse()
        {
            var builder = new TDengineConnectionStringBuilder("host=127.0.0.1;port=6030;username=root;password=taosdata;protocol=Native;db=test");
            Assert.Equal("127.0.0.1",builder.Host);
            Assert.Equal(6030,builder.Port);
            Assert.Equal("root",builder.Username);
            Assert.Equal("taosdata",builder.Password);
            Assert.Equal("test",builder.Database);
            Assert.Equal(TDengineConstant.ProtocolNative,builder.Protocol);
            builder.Clear();
            Assert.Equal(string.Empty,builder.Host);
            Assert.Equal(6030,builder.Port);
            Assert.Equal(string.Empty,builder.Username);
            Assert.Equal(string.Empty,builder.Password);
            Assert.Equal(string.Empty,builder.Database);
            Assert.Equal(TDengineConstant.ProtocolNative,builder.Protocol);
            builder.Database = "test2";
            Assert.Equal("test2",builder.Database);
            builder.Remove("db");
            Assert.Equal(string.Empty,builder.Database);
        }
    }
}
