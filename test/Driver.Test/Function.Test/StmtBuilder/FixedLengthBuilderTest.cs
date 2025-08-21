using System;
using TDengine.Driver;
using TDengine.Driver.Impl.StmtBuilder;
using Xunit;
using Xunit.Abstractions;

namespace Driver.Test.Function.Test.StmtBuilder
{
    public class FixedLengthBuilderTest
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public FixedLengthBuilderTest(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void TestI8()
        {
            var i8Builder = new I8Builder(TDengineDataType.TSDB_DATA_TYPE_TINYINT);
            i8Builder.Append((sbyte)127);
            var expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              1 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              1 * 1, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT,
                Num = 1,
                IsNull = null,
                HaveLength = 0,
                Length = null,
                BufferLength = 1,
                Buffer = new byte[]
                {
                    127
                }
            };
            var actual = i8Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            i8Builder.AppendNull();
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              2 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              2 * 1, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT,
                Num = 2,
                IsNull = new byte[]
                {
                    0,
                    1
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 2,
                Buffer = new byte[]
                {
                    127,
                    0
                }
            };
            actual = i8Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            i8Builder.Append((sbyte)-128);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              3 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              3 * 1, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT,
                Num = 3,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 3,
                Buffer = new byte[]
                {
                    127,
                    0,
                    128
                }
            };
            actual = i8Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            i8Builder.Append((sbyte)0);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              4 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              4 * 1, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT,
                Num = 4,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                    0
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 4,
                Buffer = new byte[]
                {
                    127,
                    0,
                    128,
                    0,
                }
            };
            actual = i8Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
        }

        [Fact]
        public void TestI16()
        {
            var i16Builder = new I16Builder(TDengineDataType.TSDB_DATA_TYPE_TINYINT);
            i16Builder.Append((short)32767);
            var expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              1 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              1 * 2, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT,
                Num = 1,
                IsNull = null,
                HaveLength = 0,
                Length = null,
                BufferLength = 2,
                Buffer = new byte[]
                {
                    255, 127
                }
            };
            var actual = i16Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            i16Builder.AppendNull();
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              2 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              2 * 2, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT,
                Num = 2,
                IsNull = new byte[]
                {
                    0,
                    1
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 4,
                Buffer = new byte[]
                {
                    255, 127,
                    0, 0
                }
            };
            actual = i16Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            i16Builder.Append((short)-32768);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              3 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              3 * 2, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT,
                Num = 3,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 6,
                Buffer = new byte[]
                {
                    255, 127,
                    0, 0,
                    0, 128
                }
            };
            actual = i16Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            i16Builder.Append((sbyte)0);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              4 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              4 * 2, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT,
                Num = 4,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                    0
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 8,
                Buffer = new byte[]
                {
                    255, 127,
                    0, 0,
                    0, 128,
                    0, 0,
                }
            };
            actual = i16Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
        }

        [Fact]
        public void TestI32()
        {
            var i32Builder = new I32Builder(TDengineDataType.TSDB_DATA_TYPE_INT);
            i32Builder.Append(2147483647);
            var expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              1 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              1 * 4, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_INT,
                Num = 1,
                IsNull = null,
                HaveLength = 0,
                Length = null,
                BufferLength = 4,
                Buffer = new byte[]
                {
                    255, 255, 255, 127
                }
            };
            var actual = i32Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            i32Builder.AppendNull();
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              2 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              2 * 4, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_INT,
                Num = 2,
                IsNull = new byte[]
                {
                    0,
                    1
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 8,
                Buffer = new byte[]
                {
                    255, 255, 255, 127,
                    0, 0, 0, 0
                }
            };
            actual = i32Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            i32Builder.Append(-2147483648);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              3 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              3 * 4, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_INT,
                Num = 3,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 12,
                Buffer = new byte[]
                {
                    255, 255, 255, 127,
                    0, 0, 0, 0,
                    0, 0, 0, 128
                }
            };
            actual = i32Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
        }

        [Fact]
        public void TestI64()
        {
            var i64Builder = new I64Builder(TDengineDataType.TSDB_DATA_TYPE_BIGINT);
            DoTestI64(i64Builder);
        }

        private void DoTestI64(I64Builder i64Builder)
        {
            i64Builder.Append(9223372036854775807L);
            var expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              1 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              1 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                Num = 1,
                IsNull = null,
                HaveLength = 0,
                Length = null,
                BufferLength = 8,
                Buffer = new byte[]
                {
                    255, 255, 255, 255, 255, 255, 255, 127
                }
            };
            var actual = i64Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            i64Builder.AppendNull();
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              2 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              2 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                Num = 2,
                IsNull = new byte[]
                {
                    0,
                    1
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 16,
                Buffer = new byte[]
                {
                    255, 255, 255, 255, 255, 255, 255, 127,
                    0, 0, 0, 0, 0, 0, 0, 0
                }
            };
            actual = i64Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            i64Builder.Append(-9223372036854775808L);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              3 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              3 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                Num = 3,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 24,
                Buffer = new byte[]
                {
                    255, 255, 255, 255, 255, 255, 255, 127,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 128
                }
            };
            actual = i64Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
        }

        [Fact]
        public void TestU8()
        {
            var u8Builder = new U8Builder(TDengineDataType.TSDB_DATA_TYPE_UTINYINT);
            u8Builder.Append((byte)255);
            var expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              1 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              1 * 1, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_UTINYINT,
                Num = 1,
                IsNull = null,
                HaveLength = 0,
                Length = null,
                BufferLength = 1,
                Buffer = new byte[]
                {
                    255
                }
            };
            var actual = u8Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            u8Builder.AppendNull();
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              2 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              2 * 1, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_UTINYINT,
                Num = 2,
                IsNull = new byte[]
                {
                    0,
                    1
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 2,
                Buffer = new byte[]
                {
                    255,
                    0
                }
            };
            actual = u8Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            u8Builder.Append((byte)0);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              3 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              3 * 1, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_UTINYINT,
                Num = 3,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 3,
                Buffer = new byte[]
                {
                    255,
                    0,
                    0
                }
            };
            actual = u8Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
        }

        [Fact]
        public void TestU16()
        {
            var u16Builder = new U16Builder(TDengineDataType.TSDB_DATA_TYPE_USMALLINT);
            u16Builder.Append((ushort)65535);
            var expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              1 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              1 * 2, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_USMALLINT,
                Num = 1,
                IsNull = null,
                HaveLength = 0,
                Length = null,
                BufferLength = 2,
                Buffer = new byte[]
                {
                    255, 255
                }
            };
            var actual = u16Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            u16Builder.AppendNull();
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              2 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              2 * 2, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_USMALLINT,
                Num = 2,
                IsNull = new byte[]
                {
                    0,
                    1
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 4,
                Buffer = new byte[]
                {
                    255, 255,
                    0, 0
                }
            };
            actual = u16Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            u16Builder.Append((ushort)0);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              3 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              3 * 2, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_USMALLINT,
                Num = 3,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 6,
                Buffer = new byte[]
                {
                    255, 255,
                    0, 0,
                    0, 0
                }
            };
            actual = u16Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
        }

        [Fact]
        public void TestU32()
        {
            var u32Builder = new U32Builder(TDengineDataType.TSDB_DATA_TYPE_UINT);
            u32Builder.Append(4294967295U);
            var expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              1 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              1 * 4, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_UINT,
                Num = 1,
                IsNull = null,
                HaveLength = 0,
                Length = null,
                BufferLength = 4,
                Buffer = new byte[]
                {
                    255, 255, 255, 255
                }
            };
            var actual = u32Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            u32Builder.AppendNull();
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              2 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              2 * 4, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_UINT,
                Num = 2,
                IsNull = new byte[]
                {
                    0,
                    1
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 8,
                Buffer = new byte[]
                {
                    255, 255, 255, 255,
                    0, 0, 0, 0
                }
            };
            actual = u32Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            u32Builder.Append(0U);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              3 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              3 * 4, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_UINT,
                Num = 3,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 12,
                Buffer = new byte[]
                {
                    255, 255, 255, 255,
                    0, 0, 0, 0,
                    0, 0, 0, 0
                }
            };
            actual = u32Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
        }

        [Fact]
        public void TestU64()
        {
            var u64Builder = new U64Builder(TDengineDataType.TSDB_DATA_TYPE_UBIGINT);
            u64Builder.Append(18446744073709551615UL);
            var expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              1 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              1 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_UBIGINT,
                Num = 1,
                IsNull = null,
                HaveLength = 0,
                Length = null,
                BufferLength = 8,
                Buffer = new byte[]
                {
                    255, 255, 255, 255, 255, 255, 255, 255
                }
            };
            var actual = u64Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            u64Builder.AppendNull();
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              2 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              2 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_UBIGINT,
                Num = 2,
                IsNull = new byte[]
                {
                    0,
                    1
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 16,
                Buffer = new byte[]
                {
                    255, 255, 255, 255, 255, 255, 255, 255,
                    0, 0, 0, 0, 0, 0, 0, 0
                }
            };
            actual = u64Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            u64Builder.Append(0UL);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              3 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              3 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_UBIGINT,
                Num = 3,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 24,
                Buffer = new byte[]
                {
                    255, 255, 255, 255, 255, 255, 255, 255,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0
                }
            };
            actual = u64Builder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
        }

        [Fact]
        public void TestFloat()
        {
            var floatBuilder = new F32Builder(TDengineDataType.TSDB_DATA_TYPE_FLOAT);
            floatBuilder.Append(3.1415927f);
            var expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              1 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              1 * 4, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT,
                Num = 1,
                IsNull = null,
                HaveLength = 0,
                Length = null,
                BufferLength = 4,
                Buffer = BitConverter.GetBytes(3.1415927f)
            };
            var actual = floatBuilder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            floatBuilder.AppendNull();
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              2 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              2 * 4, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT,
                Num = 2,
                IsNull = new byte[]
                {
                    0,
                    1
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 8,
                Buffer = new byte[]
                {
                    BitConverter.GetBytes(3.1415927f)[0],
                    BitConverter.GetBytes(3.1415927f)[1],
                    BitConverter.GetBytes(3.1415927f)[2],
                    BitConverter.GetBytes(3.1415927f)[3],
                    0, 0, 0, 0
                }
            };
            actual = floatBuilder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            floatBuilder.Append(float.MaxValue);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              3 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              3 * 4, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT,
                Num = 3,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 12,
                Buffer = new byte[]
                {
                    BitConverter.GetBytes(3.1415927f)[0],
                    BitConverter.GetBytes(3.1415927f)[1],
                    BitConverter.GetBytes(3.1415927f)[2],
                    BitConverter.GetBytes(3.1415927f)[3],
                    0, 0, 0, 0,
                    BitConverter.GetBytes(float.MaxValue)[0],
                    BitConverter.GetBytes(float.MaxValue)[1],
                    BitConverter.GetBytes(float.MaxValue)[2],
                    BitConverter.GetBytes(float.MaxValue)[3]
                }
            };
            actual = floatBuilder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            floatBuilder.Append(0.0f);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              4 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              4 * 4, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT,
                Num = 4,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                    0
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 16,
                Buffer = new byte[]
                {
                    BitConverter.GetBytes(3.1415927f)[0],
                    BitConverter.GetBytes(3.1415927f)[1],
                    BitConverter.GetBytes(3.1415927f)[2],
                    BitConverter.GetBytes(3.1415927f)[3],
                    0, 0, 0, 0,
                    BitConverter.GetBytes(float.MaxValue)[0],
                    BitConverter.GetBytes(float.MaxValue)[1],
                    BitConverter.GetBytes(float.MaxValue)[2],
                    BitConverter.GetBytes(float.MaxValue)[3],
                    BitConverter.GetBytes(0.0f)[0],
                    BitConverter.GetBytes(0.0f)[1],
                    BitConverter.GetBytes(0.0f)[2],
                    BitConverter.GetBytes(0.0f)[3]
                }
            };
            actual = floatBuilder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
        }

        [Fact]
        public void TestDouble()
        {
            var doubleBuilder = new F64Builder(TDengineDataType.TSDB_DATA_TYPE_DOUBLE);
            doubleBuilder.Append(3.141592653589793);
            var expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              1 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              1 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE,
                Num = 1,
                IsNull = null,
                HaveLength = 0,
                Length = null,
                BufferLength = 8,
                Buffer = BitConverter.GetBytes(3.141592653589793)
            };
            var actual = doubleBuilder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            doubleBuilder.AppendNull();
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              2 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              2 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE,
                Num = 2,
                IsNull = new byte[]
                {
                    0,
                    1
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 16,
                Buffer = new byte[]
                {
                    BitConverter.GetBytes(3.141592653589793)[0],
                    BitConverter.GetBytes(3.141592653589793)[1],
                    BitConverter.GetBytes(3.141592653589793)[2],
                    BitConverter.GetBytes(3.141592653589793)[3],
                    BitConverter.GetBytes(3.141592653589793)[4],
                    BitConverter.GetBytes(3.141592653589793)[5],
                    BitConverter.GetBytes(3.141592653589793)[6],
                    BitConverter.GetBytes(3.141592653589793)[7],
                    0, 0, 0, 0, 0, 0, 0, 0
                }
            };
            actual = doubleBuilder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            doubleBuilder.Append(double.MaxValue);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              3 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              3 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE,
                Num = 3,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 24,
                Buffer = new byte[]
                {
                    BitConverter.GetBytes(3.141592653589793)[0],
                    BitConverter.GetBytes(3.141592653589793)[1],
                    BitConverter.GetBytes(3.141592653589793)[2],
                    BitConverter.GetBytes(3.141592653589793)[3],
                    BitConverter.GetBytes(3.141592653589793)[4],
                    BitConverter.GetBytes(3.141592653589793)[5],
                    BitConverter.GetBytes(3.141592653589793)[6],
                    BitConverter.GetBytes(3.141592653589793)[7],
                    0, 0, 0, 0, 0, 0, 0, 0,
                    BitConverter.GetBytes(double.MaxValue)[0],
                    BitConverter.GetBytes(double.MaxValue)[1],
                    BitConverter.GetBytes(double.MaxValue)[2],
                    BitConverter.GetBytes(double.MaxValue)[3],
                    BitConverter.GetBytes(double.MaxValue)[4],
                    BitConverter.GetBytes(double.MaxValue)[5],
                    BitConverter.GetBytes(double.MaxValue)[6],
                    BitConverter.GetBytes(double.MaxValue)[7]
                }
            };
            actual = doubleBuilder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
            doubleBuilder.Append(0.0);
            expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              4 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              4 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE,
                Num = 4,
                IsNull = new byte[]
                {
                    0,
                    1,
                    0,
                    0
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 32,
                Buffer = new byte[]
                {
                    BitConverter.GetBytes(3.141592653589793)[0],
                    BitConverter.GetBytes(3.141592653589793)[1],
                    BitConverter.GetBytes(3.141592653589793)[2],
                    BitConverter.GetBytes(3.141592653589793)[3],
                    BitConverter.GetBytes(3.141592653589793)[4],
                    BitConverter.GetBytes(3.141592653589793)[5],
                    BitConverter.GetBytes(3.141592653589793)[6],
                    BitConverter.GetBytes(3.141592653589793)[7],
                    0, 0, 0, 0, 0, 0, 0, 0,
                    BitConverter.GetBytes(double.MaxValue)[0],
                    BitConverter.GetBytes(double.MaxValue)[1],
                    BitConverter.GetBytes(double.MaxValue)[2],
                    BitConverter.GetBytes(double.MaxValue)[3],
                    BitConverter.GetBytes(double.MaxValue)[4],
                    BitConverter.GetBytes(double.MaxValue)[5],
                    BitConverter.GetBytes(double.MaxValue)[6],
                    BitConverter.GetBytes(double.MaxValue)[7],
                    BitConverter.GetBytes(0.0)[0],
                    BitConverter.GetBytes(0.0)[1],
                    BitConverter.GetBytes(0.0)[2],
                    BitConverter.GetBytes(0.0)[3],
                    BitConverter.GetBytes(0.0)[4],
                    BitConverter.GetBytes(0.0)[5],
                    BitConverter.GetBytes(0.0)[6],
                    BitConverter.GetBytes(0.0)[7]
                }
            };
            actual = doubleBuilder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
        }

        [Fact]
        public void TestFloatWithSpecialValues()
        {
            var floatBuilder = new F32Builder(TDengineDataType.TSDB_DATA_TYPE_FLOAT);
            floatBuilder.Append(float.NaN);
            floatBuilder.Append(float.PositiveInfinity);
            floatBuilder.Append(float.NegativeInfinity);

            var expected = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              3 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              3 * 4, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT,
                Num = 3,
                IsNull = null,
                HaveLength = 0,
                Length = null,
                BufferLength = 12,
                Buffer = new byte[]
                {
                    BitConverter.GetBytes(float.NaN)[0],
                    BitConverter.GetBytes(float.NaN)[1],
                    BitConverter.GetBytes(float.NaN)[2],
                    BitConverter.GetBytes(float.NaN)[3],
                    BitConverter.GetBytes(float.PositiveInfinity)[0],
                    BitConverter.GetBytes(float.PositiveInfinity)[1],
                    BitConverter.GetBytes(float.PositiveInfinity)[2],
                    BitConverter.GetBytes(float.PositiveInfinity)[3],
                    BitConverter.GetBytes(float.NegativeInfinity)[0],
                    BitConverter.GetBytes(float.NegativeInfinity)[1],
                    BitConverter.GetBytes(float.NegativeInfinity)[2],
                    BitConverter.GetBytes(float.NegativeInfinity)[3]
                }
            };
            var actual = floatBuilder.ToStmt2BindColInfo();
            AssertEqual(expected, actual);
        }

        [Fact]
        public void TestClear()
        {
            var i64Builder = new I64Builder(TDengineDataType.TSDB_DATA_TYPE_BIGINT);
            i64Builder.Append(1);
            i64Builder.Append(2);
            i64Builder.AppendNull();
            i64Builder.Append(3);
            i64Builder.Append(4);
            var expectedStmt2BindColInfo = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              5 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              5 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                Num = 5,
                IsNull = new byte[]
                {
                    0,
                    0,
                    1,
                    0,
                    0,
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 40,
                Buffer = new byte[]
                {
                    1, 0, 0, 0, 0, 0, 0, 0,
                    2, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    3, 0, 0, 0, 0, 0, 0, 0,
                    4, 0, 0, 0, 0, 0, 0, 0,
                }
            };
            var actual = i64Builder.ToStmt2BindColInfo();
            AssertEqual(expectedStmt2BindColInfo, actual);
            i64Builder.Clear();
            DoTestI64(i64Builder);
        }

        [Fact]
        public void TestAddToStmt2BindColInfo()
        {
            var hasNullSource = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              5 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              5 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                Num = 5,
                IsNull = new byte[]
                {
                    0,
                    0,
                    1,
                    0,
                    0,
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 40,
                Buffer = new byte[]
                {
                    1, 0, 0, 0, 0, 0, 0, 0,
                    2, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    3, 0, 0, 0, 0, 0, 0, 0,
                    4, 0, 0, 0, 0, 0, 0, 0,
                }
            };
            var withoutNullSource = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              4 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              4 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                Num = 4,
                IsNull = null,
                HaveLength = 0,
                Length = null,
                BufferLength = 32,
                Buffer = new byte[]
                {
                    1, 0, 0, 0, 0, 0, 0, 0,
                    2, 0, 0, 0, 0, 0, 0, 0,
                    3, 0, 0, 0, 0, 0, 0, 0,
                    4, 0, 0, 0, 0, 0, 0, 0,
                }
            };
            var builderWithNull = new I64Builder(TDengineDataType.TSDB_DATA_TYPE_BIGINT);
            builderWithNull.Append(5);
            builderWithNull.Append(6);
            builderWithNull.AppendNull();
            builderWithNull.Append(7);
            var builderWithoutNull = new I64Builder(TDengineDataType.TSDB_DATA_TYPE_BIGINT);
            builderWithoutNull.Append(8);
            builderWithoutNull.Append(9);
            builderWithoutNull.Append(10);
            builderWithoutNull.Append(11);
            var expectSourceNullBuilderNull = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              9 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              9 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                Num = 9,
                IsNull = new byte[]
                {
                    0,
                    0,
                    1,
                    0,
                    0,
                    0,
                    0,
                    1,
                    0
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 72,
                Buffer = new byte[]
                {
                    1, 0, 0, 0, 0, 0, 0, 0,
                    2, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    3, 0, 0, 0, 0, 0, 0, 0,
                    4, 0, 0, 0, 0, 0, 0, 0,
                    5, 0, 0, 0, 0, 0, 0, 0,
                    6, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    7, 0, 0, 0, 0, 0, 0, 0,
                }
            };
            var expectSourceNullBuilderNoNull = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              9 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              9 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                Num = 9,
                IsNull = new byte[]
                {
                    0,
                    0,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 72,
                Buffer = new byte[]
                {
                    1, 0, 0, 0, 0, 0, 0, 0,
                    2, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    3, 0, 0, 0, 0, 0, 0, 0,
                    4, 0, 0, 0, 0, 0, 0, 0,
                    8, 0, 0, 0, 0, 0, 0, 0,
                    9, 0, 0, 0, 0, 0, 0, 0,
                    10, 0, 0, 0, 0, 0, 0, 0,
                    11, 0, 0, 0, 0, 0, 0, 0,
                }
            };
            var expectSourceNoNullBuilderNull = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              8 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              8 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                Num = 8,
                IsNull = new byte[]
                {
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    1,
                    0
                },
                HaveLength = 0,
                Length = null,
                BufferLength = 64,
                Buffer = new byte[]
                {
                    1, 0, 0, 0, 0, 0, 0, 0,
                    2, 0, 0, 0, 0, 0, 0, 0,
                    3, 0, 0, 0, 0, 0, 0, 0,
                    4, 0, 0, 0, 0, 0, 0, 0,
                    5, 0, 0, 0, 0, 0, 0, 0,
                    6, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    7, 0, 0, 0, 0, 0, 0, 0,
                }
            };
            var expectSourceNoNullBuilderNoNull = new Stmt2BindColInfo
            {
                TotalLength = 4 + // TotalLength field length
                              4 + // DataType field length
                              4 + // Num field length
                              8 * 1 + // IsNull field length
                              1 + // HaveLength field length
                              4 + // BufferLength field length
                              8 * 8, // Buffer field length
                DataType = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                Num = 8,
                IsNull = null,
                HaveLength = 0,
                Length = null,
                BufferLength = 64,
                Buffer = new byte[]
                {
                    1, 0, 0, 0, 0, 0, 0, 0,
                    2, 0, 0, 0, 0, 0, 0, 0,
                    3, 0, 0, 0, 0, 0, 0, 0,
                    4, 0, 0, 0, 0, 0, 0, 0,
                    8, 0, 0, 0, 0, 0, 0, 0,
                    9, 0, 0, 0, 0, 0, 0, 0,
                    10, 0, 0, 0, 0, 0, 0, 0,
                    11, 0, 0, 0, 0, 0, 0, 0,
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