using TDengine.Driver;
using TDengine.Driver.Impl.StmtBuilder;
using Xunit;
using Xunit.Abstractions;

namespace Driver.Test.Function.Test.StmtBuilder
{
    public class VariableLengthBuilderTest
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public VariableLengthBuilderTest(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void TestAdd()
        {
            var builder = new VariableLengthBuilder(TDengineDataType.TSDB_DATA_TYPE_VARBINARY);
            DoTestAdd(builder);
        }

        private void DoTestAdd(VariableLengthBuilder builder)
        {
            builder.AppendString("Hello");
            builder.AppendString("中文");
            builder.AppendNull();
            builder.AppendBytes(new byte[] { 1, 2, 3, 4, 5 });
            builder.AppendString("中文");
            Assert.Equal(5, builder.Length);
            var expectedStmt2BindColInfo = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              5 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              5 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              22, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 5,
                IsNull = new byte[]
                {
                    0, // "Hello" is not null
                    0, // "中文" is not null
                    1, // Null value
                    0, // [1, 2, 3, 4, 5] is not null
                    0, // "中文" is not null
                },
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of "Hello"
                    6, // Length of "中文"
                    0, // Length of null value
                    5, // Length of [1, 2, 3, 4, 5]
                    6, // Length of "中文"
                },
                BufferLength = 22, // 5 + 6 + 0 + 5 + 6
                Buffer = new byte[]
                {
                    72, 101, 108, 108, 111, // "Hello"
                    228, 184, 173, 230, 150, 135, // "中文"
                    1, 2, 3, 4, 5, // [1, 2, 3, 4, 5]
                    228, 184, 173, 230, 150, 135, // "中文"
                }
            };
            var actual = builder.ToStmt2BindColInfo();
            AssertEqual(expectedStmt2BindColInfo, actual);
            // remove the last
            builder.Remove(1);
            expectedStmt2BindColInfo = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              4 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              16, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 4,
                IsNull = new byte[]
                {
                    0, // "Hello" is not null
                    0, // "中文" is not null
                    1, // Null value
                    0, // [1, 2, 3, 4, 5] is not null
                },
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of "Hello"
                    6, // Length of "中文"
                    0, // Length of null value
                    5, // Length of [1, 2, 3, 4, 5]
                },
                BufferLength = 16, // 5 + 6 + 5
                Buffer = new byte[]
                {
                    72, 101, 108, 108, 111, // "Hello"
                    228, 184, 173, 230, 150, 135, // "中文"
                    1, 2, 3, 4, 5, // [1, 2, 3, 4, 5]
                }
            };
            actual = builder.ToStmt2BindColInfo();
            AssertEqual(expectedStmt2BindColInfo, actual);
            // remove contains null
            builder.Remove(2);
            expectedStmt2BindColInfo = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              2 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              2 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              11, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 2,
                IsNull = null,
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of "Hello"
                    6, // Length of "中文"
                },
                BufferLength = 11, // 11
                Buffer = new byte[]
                {
                    72, 101, 108, 108, 111, // "Hello"
                    228, 184, 173, 230, 150, 135, // "中文"
                }
            };
            actual = builder.ToStmt2BindColInfo();
            AssertEqual(expectedStmt2BindColInfo, actual);

            // append null
            builder.AppendNull();
            expectedStmt2BindColInfo = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              3 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              3 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              11, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 3,
                IsNull = new byte[]
                {
                    0, // "Hello" is not null
                    0, // "中文" is not null
                    1, // Null value
                },
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of "Hello"
                    6, // Length of "中文"
                    0, // Length of null value
                },
                BufferLength = 11, // 5 + 6 + 0
                Buffer = new byte[]
                {
                    72, 101, 108, 108, 111, // "Hello"
                    228, 184, 173, 230, 150, 135, // "中文"
                }
            };
            actual = builder.ToStmt2BindColInfo();
            AssertEqual(expectedStmt2BindColInfo, actual);
            // remove null
            builder.Remove(1);
            expectedStmt2BindColInfo = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              2 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              2 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              11, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 2,
                IsNull = null,
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of "Hello"
                    6, // Length of "中文"
                },
                BufferLength = 11, // 5 + 6
                Buffer = new byte[]
                {
                    72, 101, 108, 108, 111, // "Hello"
                    228, 184, 173, 230, 150, 135, // "中文"
                }
            };
            actual = builder.ToStmt2BindColInfo();
            AssertEqual(expectedStmt2BindColInfo, actual);

            builder.AppendString("中文");
            expectedStmt2BindColInfo = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              3 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              3 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              17, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 3,
                IsNull = null,
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of "Hello"
                    6, // Length of "中文"
                    6, // Length of "中文"
                },
                BufferLength = 17, // 5 + 6 + 6
                Buffer = new byte[]
                {
                    72, 101, 108, 108, 111, // "Hello"
                    228, 184, 173, 230, 150, 135, // "中文"
                    228, 184, 173, 230, 150, 135, // "中文"
                }
            };
            actual = builder.ToStmt2BindColInfo();
            AssertEqual(expectedStmt2BindColInfo, actual);
        }

        [Fact]
        public void TestClear()
        {
            var builder = new VariableLengthBuilder(TDengineDataType.TSDB_DATA_TYPE_VARBINARY);
            builder.AppendString("Hello");
            builder.AppendString("中文");
            builder.AppendNull();
            builder.AppendBytes(new byte[] { 1, 2, 3, 4, 5 });
            builder.AppendString("中文");
            Assert.Equal(5, builder.Length);
            var expectedStmt2BindColInfo = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              5 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              5 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              22, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 5,
                IsNull = new byte[]
                {
                    0, // "Hello" is not null
                    0, // "中文" is not null
                    1, // Null value
                    0, // [1, 2, 3, 4, 5] is not null
                    0, // "中文" is not null
                },
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of "Hello"
                    6, // Length of "中文"
                    0, // Length of null value
                    5, // Length of [1, 2, 3, 4, 5]
                    6, // Length of "中文"
                },
                BufferLength = 22, // 5 + 6 + 0 + 5 + 6
                Buffer = new byte[]
                {
                    72, 101, 108, 108, 111, // "Hello"
                    228, 184, 173, 230, 150, 135, // "中文"
                    1, 2, 3, 4, 5, // [1, 2, 3, 4, 5]
                    228, 184, 173, 230, 150, 135, // "中文"
                }
            };
            var actual = builder.ToStmt2BindColInfo();
            AssertEqual(expectedStmt2BindColInfo, actual);
            // clear
            builder.Clear();
            builder.AppendBytes(new byte[] { 1, 2, 3, 4, 5 });
            builder.AppendString("Hello");
            builder.AppendNull();
            builder.AppendString("中文");
            Assert.Equal(4, builder.Length);
            expectedStmt2BindColInfo = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              4 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              16, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 4,
                IsNull = new byte[]
                {
                    0, // [1, 2, 3, 4, 5] is not null
                    0, // "Hello" is not null
                    1, // Null value
                    0, // "中文" is not null
                },
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of [1, 2, 3, 4, 5]
                    5, // Length of "Hello"
                    0, // Length of null value
                    6, // Length of "中文"
                },
                BufferLength = 16,
                Buffer = new byte[]
                {
                    1, 2, 3, 4, 5, // [1, 2, 3, 4, 5]
                    72, 101, 108, 108, 111, // "Hello"
                    228, 184, 173, 230, 150, 135, // "中文"
                }
            };
            actual = builder.ToStmt2BindColInfo();
            AssertEqual(expectedStmt2BindColInfo, actual);
            builder.Clear();
            DoTestAdd(builder);
        }

        [Fact]
        public void TestAddToStmt2BindColInfo()
        {
            // source with null
            var hasNullSource = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              5 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              5 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              22, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 5,
                IsNull = new byte[]
                {
                    0, // "Hello" is not null
                    0, // "中文" is not null
                    1, // Null value
                    0, // [1, 2, 3, 4, 5] is not null
                    0, // "中文" is not null
                },
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of "Hello"
                    6, // Length of "中文"
                    0, // Length of null value
                    5, // Length of [1, 2, 3, 4, 5]
                    6, // Length of "中文"
                },
                BufferLength = 22, // 5 + 6 + 0 + 5 + 6
                Buffer = new byte[]
                {
                    72, 101, 108, 108, 111, // "Hello"
                    228, 184, 173, 230, 150, 135, // "中文"
                    1, 2, 3, 4, 5, // [1, 2, 3, 4, 5]
                    228, 184, 173, 230, 150, 135, // "中文"
                }
            };
            // source without null
            var withoutNullSource = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              4 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              22, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 4,
                IsNull = null,
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of "Hello"
                    6, // Length of "中文"
                    5, // Length of [1, 2, 3, 4, 5]
                    6, // Length of "中文"
                },
                BufferLength = 22, // 5 + 6 + 5 + 6
                Buffer = new byte[]
                {
                    72, 101, 108, 108, 111, // "Hello"
                    228, 184, 173, 230, 150, 135, // "中文"
                    1, 2, 3, 4, 5, // [1, 2, 3, 4, 5]
                    228, 184, 173, 230, 150, 135, // "中文"
                }
            };
            // builder with null
            var builderWithNull = new VariableLengthBuilder(TDengineDataType.TSDB_DATA_TYPE_VARBINARY);
            builderWithNull.AppendString("Hello");
            builderWithNull.AppendBytes(new byte[] { 1, 2, 3, 4, 5 });
            builderWithNull.AppendNull();
            builderWithNull.AppendString("中文");
            // builder without null
            var builderWithoutNull = new VariableLengthBuilder(TDengineDataType.TSDB_DATA_TYPE_VARBINARY);
            builderWithoutNull.AppendString("Hello");
            builderWithoutNull.AppendBytes(new byte[] { 1, 2, 3, 4, 5 });
            builderWithoutNull.AppendString("中文");
            var expectSourceNullBuilderNull = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              9 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              9 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              38, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 9,
                IsNull = new byte[]
                {
                    0, // "Hello" is not null
                    0, // "中文" is not null
                    1, // Null value from source
                    0, // [1, 2, 3, 4, 5] is not null
                    0, // "中文" is not null
                    0, // "Hello" is not null
                    0, // [1, 2, 3, 4, 5] is not null
                    1, // Null value from source
                    0, // "中文" is not null
                },
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of "Hello"
                    6, // Length of "中文"
                    0, // Length of null value from source
                    5, // Length of [1, 2, 3, 4, 5]
                    6, // Length of "中文" from source
                    5, // Length of "Hello" from source
                    5, // Length of [1, 2, 3, 4, 5] from source
                    0, // Length of null value from source
                    6, // Length of "中文" from source
                },
                BufferLength = 38,
                Buffer = new byte[]
                {
                    72, 101, 108, 108, 111, // "Hello"
                    228, 184, 173, 230, 150, 135, // "中文"
                    1, 2, 3, 4, 5, // [1 ,2 ,3 ,4 ,5]
                    228, 184, 173, 230, 150, 135,// "中文"
                    72, 101, 108, 108, 111, // "Hello"
                    1, 2, 3, 4, 5, // [1 ,2 ,3 ,4 ,5]
                    228, 184, 173, 230, 150, 135, // "中文"
                }
            };
            var expectSourceNullBuilderNoNull = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              8 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              8 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              38, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 8,
                IsNull = new byte[]
                {
                    0, // "Hello" is not null
                    0, // "中文" is not null
                    1, // Null value
                    0, // [1, 2, 3, 4, 5] is not null
                    0, // "中文" is not null
                    0, // "Hello" is not null
                    0, // [1, 2, 3, 4, 5] is not null
                    0, // "中文" is not null
                },
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of "Hello"
                    6, // Length of "中文"
                    0, // Length of null value from source
                    5, // Length of [1, 2, 3, 4, 5]
                    6, // Length of "中文" from source
                    5, // Length of "Hello" from source
                    5, // Length of [1, 2, 3, 4, 5] from source
                    6, // Length of "中文" from source
                },
                BufferLength = 38,
                Buffer = new byte[]
                {
                    72, 101, 108, 108, 111, // "Hello"
                    228, 184, 173, 230, 150, 135, // "中文"
                    1, 2, 3, 4, 5, // [1 ,2 ,3 ,4 ,5]
                    228, 184, 173, 230, 150, 135, // "中文"
                    72, 101, 108, 108, 111, // "Hello"
                    1, 2, 3, 4, 5, // [1 ,2 ,3 ,4 ,5]
                    228, 184, 173, 230, 150, 135, // "中文"
                }
            };
            var expectSourceNoNullBuilderNull = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              8 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              8 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              38, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 8,
                IsNull = new byte[]
                {
                    0, // "Hello" is not null
                    0, // "中文" is not null
                    0, // [1, 2, 3, 4, 5] is not null
                    0, // "中文" is not null
                    0, // "Hello" is not null
                    0, // [1, 2, 3, 4, 5] is not null
                    1, // Null value from source
                    0, // "中文" is not null
                },
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of "Hello"
                    6, // Length of "中文"
                    5, // Length of [1, 2, 3, 4, 5]
                    6, // Length of "中文"
                    5, // Length of "Hello" from source
                    5, // Length of [1, 2, 3, 4, 5] from source
                    0, // Length of null value from source
                    6, // Length of "中文" from source
                },
                BufferLength = 38,
                Buffer = new byte[]
                {
                    72, 101, 108, 108, 111, // "Hello"
                    228, 184, 173, 230, 150, 135,// "中文"
                    1,2 ,3 ,4 ,5 ,// [1 ,2 ,3 ,4 ,5]
                    228 ,184 ,173 ,230 ,150 ,135,// "中文"
                    72 ,101 ,108 ,108 ,111,// "Hello"
                    1 ,2 ,3 ,4 ,5,// [1 ,2 ,3 ,4 ,5]
                    228 ,184 ,173 ,230 ,150 ,135,// "中文"
                }
            };
            var expectSourceNoNullBuilderNoNull = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              7 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              7 * 4 + // Length field length, each length is 4 bytes
                              4 + // BufferLength field length
                              38, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                Num = 7,
                IsNull = null,
                HaveLength = 1,
                Length = new int[]
                {
                    5, // Length of "Hello"
                    6, // Length of "中文"
                    5, // Length of [1, 2, 3, 4, 5]
                    6, // Length of "中文"
                    5, // Length of "Hello" from source
                    5, // Length of [1, 2, 3, 4 ,5] from source
                    6,// Length of "中文" from source
                },
                BufferLength = 38,
                Buffer = new byte[]
                {
                    72 ,101 ,108 ,108 ,111,// "Hello"
                    228 ,184 ,173 ,230 ,150 ,135,// "中文"
                    1 ,2 ,3 ,4 ,5,// [1 ,2 ,3 ,4 ,5]
                    228 ,184 ,173 ,230 ,150 ,135,// "中文"
                    72 ,101 ,108 ,108 ,111,// "Hello"
                    1 ,2 ,3 ,4 ,5,// [1 ,2 ,3 ,4 ,5]
                    228 ,184 ,173 ,230 ,150 ,135,// "中文"
                }
            };
            // source with null, builder with null
            var actual = builderWithNull.AddToStmt2BindColInfo(hasNullSource);
            AssertEqual(expectSourceNullBuilderNull, actual);
            // source with null, builder without null
            actual = builderWithoutNull.AddToStmt2BindColInfo(hasNullSource);
            AssertEqual(expectSourceNullBuilderNoNull, actual);
            // source without null, builder with null
            actual = builderWithNull.AddToStmt2BindColInfo(withoutNullSource);
            AssertEqual(expectSourceNoNullBuilderNull, actual);
            // source without null, builder without null
            actual = builderWithoutNull.AddToStmt2BindColInfo(withoutNullSource);
            AssertEqual(expectSourceNoNullBuilderNoNull, actual);
        }

        private void AssertEqual(Stmt2BindColInfo expected, Stmt2BindColInfo actual)
        {
            Assert.Equal(expected.TotalLength, actual.TotalLength);
            Assert.Equal(expected.DataType, actual.DataType);
            Assert.Equal(expected.Num, actual.Num);
            Assert.Equal(expected.IsNull, actual.IsNull);
            Assert.Equal(expected.HaveLength, actual.HaveLength);
            Assert.Equal(expected.Length, actual.Length);
            Assert.Equal(expected.BufferLength, actual.BufferLength);
            Assert.Equal(expected.Buffer, actual.Buffer);
        }
    }
}