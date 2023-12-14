using System.Text;
using TDengine.Driver;
using Xunit;

namespace Driver.Test.Function.Test
{
    public class Reqid
    {
        [Fact]
        public void MurmurHash32_ReturnsExpectedHash()
        {
            // Arrange
            byte[] data = Encoding.UTF8.GetBytes("driver-go");
            uint seed = 0;

            // Act
            uint hash = ReqId.MurmurHash32(data, seed);

            // Assert
            uint expectedHash = 3037880692;
            Assert.Equal(expectedHash, hash);
        }

        [Fact]
        public void GetReqId()
        {
            var reqId = ReqId.GetReqId();
            var reqId2 = ReqId.GetReqId();
            Assert.NotEqual(0, reqId);
            Assert.Equal(reqId+1,reqId2);
        }
    }
}