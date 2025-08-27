using System.Collections.Generic;
using System.Net.Http.Headers;
using System.Text;
using TDengine.Driver.Impl.StmtBuilder;
using Xunit;
using Xunit.Abstractions;

namespace Driver.Test.Function.Test.StmtBuilder
{
    public class TableNameBuilderTest
    {
        private readonly ITestOutputHelper _testOutputHelper;
        public TableNameBuilderTest(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void TestAdd()
        {
            var builder = new TableNameBuilder();
            TestAddData(builder);
            TestClear(builder);
            TestAddData(builder);
            TestClear(builder);
        }

        private void TestAddData(TableNameBuilder builder)
        {
            builder.Add("table1");
            Assert.Equal(1, builder.Count);
            Assert.Equal(7, builder.TotalBufferLen); // "table1\0" = 6 + 1 for null terminator
            Assert.Equal(new byte[] { 116, 97, 98, 108, 101, 49, 0 }, builder.GetBytes());
            Assert.Equal(new short[] {7}, builder.GetLengths());
            Assert.Equal(new List<string> { "table1" }, builder.TableNames);
            builder.Add("test2");
            Assert.Equal(2, builder.Count);
            Assert.Equal(13, builder.TotalBufferLen); // "table1\0test2\0" = 7 + 6
            Assert.Equal(new byte[] { 116, 97, 98, 108, 101, 49, 0, 116, 101, 115, 116, 50, 0 }, builder.GetBytes());
            Assert.Equal(new short[] { 7, 6 }, builder.GetLengths());
            Assert.Equal(new List<string> { "table1", "test2" }, builder.TableNames);
            builder.Add("中文");
            Assert.Equal(3, builder.Count);
            Assert.Equal(20, builder.TotalBufferLen); // "table1\0test2\0中文\0" = 7 + 6 + 7
            var bs = Encoding.UTF8.GetBytes("中文");
            // _testOutputHelper.WriteLine("Bytes for '中文': " + string.Join(", ", bs));
            Assert.Equal(new byte[] { 116, 97, 98, 108, 101, 49, 0, 116, 101, 115, 116, 50, 0, 228, 184, 173, 230, 150, 135, 0 }, builder.GetBytes());
            Assert.Equal(new short[] { 7, 6, 7 }, builder.GetLengths());
            Assert.Equal(new List<string> { "table1", "test2", "中文" }, builder.TableNames);
        }

        private static void TestClear(TableNameBuilder builder)
        {
            builder.Clear();
            Assert.Equal(0, builder.Count);
            Assert.Equal(0, builder.TotalBufferLen);
            Assert.Empty(builder.GetBytes());
            Assert.Empty(builder.GetLengths());
            Assert.Empty(builder.TableNames);
        }

    }
}