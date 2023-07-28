using NUnit.Framework;
using TDengine.Data.Client;

namespace TDengine.Data.Tests
{
    [TestFixture]
    public class TDengineConnectionStringBuilderTests
    {
        [Test]
        public void DefaultNative_ShouldSetDefaultValues()
        {
            var builder = new TDengineConnectionStringBuilder("");
            
            builder.DefaultNative();

            Assert.AreEqual(6030, builder.Port);
            Assert.AreEqual(string.Empty, builder.Host);
            Assert.AreEqual(TDengineConnectionStringBuilder.ProtocolNative, builder.Protocol);
        }
        

        [Test]
        public void DefaultWebSocket_ShouldSetDefaultValues()
        {
            var builder = new TDengineConnectionStringBuilder("");
            
            builder.DefaultWebSocket();
            
            Assert.AreEqual(6041, builder.Port);
            Assert.AreEqual("localhost", builder.Host);
            Assert.AreEqual(TDengineConnectionStringBuilder.ProtocolWebSocket, builder.Protocol);
        }

        [Test]
        public void Parse()
        {
            var builder = new TDengineConnectionStringBuilder("host=127.0.0.1;port=6030;username=root;password=taosdata;protocol=Native;db=test;token=tk");
            Assert.AreEqual("127.0.0.1",builder.Host);
            Assert.AreEqual(6030,builder.Port);
            Assert.AreEqual("root",builder.Username);
            Assert.AreEqual("taosdata",builder.Password);
            Assert.AreEqual("test",builder.Database);
            Assert.AreEqual("tk",builder.Token);
            Assert.AreEqual(TDengineConnectionStringBuilder.ProtocolNative,builder.Protocol);
            builder.Clear();
            Assert.AreEqual(string.Empty,builder.Host);
            Assert.AreEqual(6030,builder.Port);
            Assert.AreEqual(string.Empty,builder.Username);
            Assert.AreEqual(string.Empty,builder.Password);
            Assert.AreEqual(string.Empty,builder.Database);
            Assert.AreEqual(string.Empty,builder.Token);
            Assert.AreEqual(TDengineConnectionStringBuilder.ProtocolNative,builder.Protocol);
            builder.Database = "test2";
            Assert.AreEqual("test2",builder.Database);
            builder.Remove("db");
            Assert.AreEqual(string.Empty,builder.Database);
        }
    }
}