using System;
using System.Text;
using TDengine.Driver;
using Xunit;
using Xunit.Abstractions;

namespace Driver.Test.Function.Test
{
    public class BlockReaderTest
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public BlockReaderTest(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void TestAllTypeRead()
        {
            var data = new byte[]
            {
                0x01, 0x00, 0x00, 0x00,
                0xbc, 0x02, 0x00, 0x00,
                0x04, 0x00, 0x00, 0x00,
                0x12, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x80,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                0x09, 0x08, 0x00, 0x00, 0x00,
                0x01, 0x01, 0x00, 0x00, 0x00,
                0x02, 0x01, 0x00, 0x00, 0x00,
                0x03, 0x02, 0x00, 0x00, 0x00,
                0x04, 0x04, 0x00, 0x00, 0x00,
                0x05, 0x08, 0x00, 0x00, 0x00,
                0x0b, 0x01, 0x00, 0x00, 0x00,
                0x0c, 0x02, 0x00, 0x00, 0x00,
                0x0d, 0x04, 0x00, 0x00, 0x00,
                0x0e, 0x08, 0x00, 0x00, 0x00,
                0x06, 0x04, 0x00, 0x00, 0x00,
                0x07, 0x08, 0x00, 0x00, 0x00,
                0x08, 0x16, 0x00, 0x00, 0x00,
                0x0a, 0x52, 0x00, 0x00, 0x00,
                0x10, 0x16, 0x00, 0x00, 0x00,
                0x14, 0x66, 0x00, 0x00, 0x00,
                0x11, 0x04, 0x14, 0x00, 0x10,
                0x15, 0x04, 0x08, 0x00, 0x08,

                0x20, 0x00, 0x00, 0x00,
                0x04, 0x00, 0x00, 0x00,
                0x04, 0x00, 0x00, 0x00,
                0x08, 0x00, 0x00, 0x00,
                0x10, 0x00, 0x00, 0x00,
                0x20, 0x00, 0x00, 0x00,
                0x04, 0x00, 0x00, 0x00,
                0x08, 0x00, 0x00, 0x00,
                0x10, 0x00, 0x00, 0x00,
                0x20, 0x00, 0x00, 0x00,
                0x10, 0x00, 0x00, 0x00,
                0x20, 0x00, 0x00, 0x00,
                0x14, 0x00, 0x00, 0x00,
                0x2a, 0x00, 0x00, 0x00,
                0x17, 0x00, 0x00, 0x00,
                0x2e, 0x00, 0x00, 0x00,
                0x40, 0x00, 0x00, 0x00,
                0x20, 0x00, 0x00, 0x00,

                0x00,
                0xca, 0x61, 0x78, 0x87, 0x97, 0x01, 0x00, 0x00,
                0xb2, 0x65, 0x78, 0x87, 0x97, 0x01, 0x00, 0x00,
                0x9a, 0x69, 0x78, 0x87, 0x97, 0x01, 0x00, 0x00,
                0x82, 0x6d, 0x78, 0x87, 0x97, 0x01, 0x00, 0x00,

                0x40,
                0x01,
                0x00,
                0x00,
                0x01,

                0x40,
                0x7f,
                0x00,
                0x80,
                0x01,

                0x40,
                0xff, 0x7f,
                0x00, 0x00,
                0x00, 0x80,
                0x01, 0x00,

                0x40,
                0xff, 0xff, 0xff, 0x7f,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x80,
                0x01, 0x00, 0x00, 0x00,

                0x40,
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                0x40,
                0xff,
                0x00,
                0x00,
                0x01,

                0x40,
                0xff, 0xff,
                0x00, 0x00,
                0x00, 0x00,
                0x01, 0x00,

                0x40,
                0xff, 0xff, 0xff, 0xff,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0x00, 0x00,

                0x40,
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                0x40,
                0x00, 0x00, 0x00, 0x4f,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x80, 0x3f,

                0x40,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x43,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,

                0x00, 0x00, 0x00, 0x00,
                0xff, 0xff, 0xff, 0xff,
                0x08, 0x00, 0x00, 0x00,
                0x11, 0x00, 0x00, 0x00,
                0x06, 0x00,
                0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,
                0x07, 0x00,
                0xe4, 0xb8, 0xad, 0x61, 0xe6, 0x96, 0x87,
                0x01, 0x00,
                0x31,

                0x00, 0x00, 0x00, 0x00,
                0xff, 0xff, 0xff, 0xff,
                0x16, 0x00, 0x00, 0x00,
                0x24, 0x00, 0x00, 0x00,
                0x14, 0x00,
                0x6e, 0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x68, 0x00, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x72,
                0x00, 0x00, 0x00,
                0x0c, 0x00,
                0x2d, 0x4e, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x87, 0x65, 0x00, 0x00,
                0x04, 0x00,
                0x31, 0x00, 0x00, 0x00,

                0x00, 0x00, 0x00, 0x00,
                0xff, 0xff, 0xff, 0xff,
                0x0b, 0x00, 0x00, 0x00,
                0x14, 0x00, 0x00, 0x00,
                0x09, 0x00,
                0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,
                0x07, 0x00,
                0xe4, 0xb8, 0xad, 0x61, 0xe6, 0x96, 0x87,
                0x01, 0x00,
                0x31,

                0x00, 0x00, 0x00, 0x00,
                0xff, 0xff, 0xff, 0xff,
                0x17, 0x00, 0x00, 0x00,
                0xff, 0xff, 0xff, 0xff,
                0x15, 0x00,
                0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x59, 0x40,
                0x15, 0x00,
                0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x59, 0x40,

                0x40,
                0xff, 0xff, 0x0f, 0x63, 0x2d, 0x5e, 0xc7, 0x6b, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0xf0, 0x9c, 0xd2, 0xa1, 0x38, 0x94, 0xfa, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                0x40,
                0xff, 0xe0, 0xf5, 0x05, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x01, 0x1f, 0x0a, 0xfa, 0xff, 0xff, 0xff, 0xff,
                0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                0x00,
            };
            // create table t (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100),v16 decimal(20,4),v17 decimal(8,4));

            // insert into t values(1750324502986,true,127,32767,2147483647,9223372036854775807,255,65535,4294967295,18446744073709551615,2147483647,18446744073709551615,'binary','nchar','varbinary','point(100 100)',9999999999999999.9999,9999.9999)
            // (1750324503986,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)
            // (1750324504986,false,-128,-32768,-2147483648,-9223372036854775808,0,0,0,0,0,0,'中a文','中a文','中a文','point(100 100)',-9999999999999999.9999,-9999.9999)
            // (1750324505986,true,1,1,1,1,1,1,1,1,1,1,'1','1','1',null,1,1);
            var scales = new byte[18]
            {
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4
            };
            var colTypes = new byte[18]
            {
                0x09,
                0x01,
                0x02,
                0x03,
                0x04,
                0x05,
                0x0b,
                0x0c,
                0x0d,
                0x0e,
                0x06,
                0x07,
                0x08,
                0x0a,
                0x10,
                0x14,
                0x11,
                0x15,
            };
            var parser = new BlockReader(0, 18, (int)TDenginePrecision.TSDB_TIME_PRECISION_MILLI, colTypes, scales);
            parser.SetBlock(data);
            var values = new object[18];
            var cols = parser.GetValues(0, values);
            var expected = new object[]
            {
                TDengineConstant.ConvertTimeToDatetime(1750324502986, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                true, // bool
                (sbyte)127, // tinyint
                (short)32767, // smallint
                2147483647, // int
                9223372036854775807L, // bigint
                (byte)255, // tinyint unsigned
                (ushort)65535, // smallint unsigned
                4294967295U, // int unsigned
                18446744073709551615UL, // bigint unsigned
                2147483647F, // float
                18446744073709551615D, // double
                Encoding.UTF8.GetBytes("binary"), // binary(20)
                "nchar", // nchar(20)
                Encoding.UTF8.GetBytes("varbinary"), // varbinary(20)
                new byte[]
                {
                    0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59, 0x40
                }, // geometry(100)
                "9999999999999999.9999", // decimal(20,4)
                "9999.9999", // decimal(8,4)
            };
            var dateTimeIndex = 0;
            var boolIndex = 1;
            var tinyIntIndex = 2;
            var smallIntIndex = 3;
            var intIndex = 4;
            var bigIntIndex = 5;
            var tinyIntUnsignedIndex = 6;
            var smallIntUnsignedIndex = 7;
            var intUnsignedIndex = 8;
            var bigIntUnsignedIndex = 9;
            var floatIndex = 10;
            var doubleIndex = 11;
            var binaryIndex = 12;
            var ncharIndex = 13;
            var varbinaryIndex = 14;
            var geometryIndex = 15;
            var decimal128Index = 16;
            var decimal64Index = 17;
            Assert.Equal(expected.Length, cols);
            Assert.Equal(expected, values);
            // get date time
            var dt = parser.GetDateTime(0, dateTimeIndex);
            Assert.Equal(expected[0], dt);
            Assert.Throws<InvalidCastException>(() => parser.GetDateTime(0, boolIndex));
            // get boolean
            Assert.Equal(expected[1], parser.GetBoolean(0, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetBoolean(0, dateTimeIndex));
            // test convert to byte
            Assert.Equal(expected[tinyIntUnsignedIndex], parser.GetByte(0, tinyIntUnsignedIndex));
            Assert.Equal((byte)127, parser.GetByte(0, tinyIntIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(0, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(0, dateTimeIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(0, smallIntIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(0, intIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(0, bigIntIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(0, smallIntUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(0, intUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(0, bigIntUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(0, floatIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(0, doubleIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(0, decimal128Index));
            Assert.Throws<OverflowException>(() => parser.GetByte(0, decimal64Index));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(0, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(0, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(0, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(0, geometryIndex));

            // test convert to int16
            Assert.Equal((short)255, parser.GetInt16(0, tinyIntUnsignedIndex));
            Assert.Equal((short)127, parser.GetInt16(0, tinyIntIndex));
            Assert.Equal((short)32767, parser.GetInt16(0, smallIntIndex));
            Assert.Equal((short)9999, parser.GetInt16(0, decimal64Index));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(0, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(0, dateTimeIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(0, intIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(0, bigIntIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(0, smallIntUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(0, intUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(0, bigIntUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(0, floatIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(0, doubleIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(0, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(0, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(0, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(0, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(0, geometryIndex));
            // test convert to int32
            Assert.Equal(255, parser.GetInt32(0, tinyIntUnsignedIndex));
            Assert.Equal(127, parser.GetInt32(0, tinyIntIndex));
            Assert.Equal(32767, parser.GetInt32(0, smallIntIndex));
            Assert.Equal(9999, parser.GetInt32(0, decimal64Index));
            Assert.Equal(65535, parser.GetInt32(0, smallIntUnsignedIndex));
            Assert.Equal(2147483647, parser.GetInt32(0, intIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(0, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(0, dateTimeIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(0, bigIntIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(0, intUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(0, bigIntUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(0, floatIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(0, doubleIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(0, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(0, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(0, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(0, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(0, geometryIndex));
            // test convert to int64
            Assert.Equal((long)255, parser.GetInt64(0, tinyIntUnsignedIndex));
            Assert.Equal((long)127, parser.GetInt64(0, tinyIntIndex));
            Assert.Equal((long)32767, parser.GetInt64(0, smallIntIndex));
            Assert.Equal((long)9999, parser.GetInt64(0, decimal64Index));
            Assert.Equal((long)65535, parser.GetInt64(0, smallIntUnsignedIndex));
            Assert.Equal((long)2147483647, parser.GetInt64(0, intIndex));
            Assert.Equal((long)4294967295, parser.GetInt64(0, intUnsignedIndex));
            Assert.Equal((long)9223372036854775807, parser.GetInt64(0, bigIntIndex));
            Assert.Equal((long)((float)2147483647), parser.GetInt64(0, floatIndex));
            Assert.Equal((long)9999999999999999, parser.GetInt64(0, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(0, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(0, dateTimeIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt64(0, bigIntUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt64(0, doubleIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(0, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(0, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(0, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(0, geometryIndex));


            cols = parser.GetValues(1, values);
            expected = new object[]
            {
                TDengineConstant.ConvertTimeToDatetime(1750324503986, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                null, // bool
                null, // tinyint
                null, // smallint
                null, // int
                null, // bigint
                null, // tinyint unsigned
                null, // smallint unsigned
                null, // int unsigned
                null, // bigint unsigned
                null, // float
                null, // double
                null, // binary(20)
                null, // nchar(20)
                null, // varbinary(20)
                null, // geometry(100)
                null, // decimal(20,4)
                null, // decimal(8,4)
            };
            Assert.Equal(expected.Length, cols);
            Assert.Equal(expected, values);
            cols = parser.GetValues(2, values);
            expected = new object[]
            {
                TDengineConstant.ConvertTimeToDatetime(1750324504986, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                false, // bool
                (sbyte)(-128), // tinyint
                (short)(-32768), // smallint
                -2147483648, // int
                -9223372036854775808L, // bigint
                (byte)0, // tinyint unsigned
                (ushort)0, // smallint unsigned
                0U, // int unsigned
                0UL, // bigint unsigned
                0F, // float
                0D, // double
                Encoding.UTF8.GetBytes("中a文"), // binary(20)
                "中a文", // nchar(20)
                Encoding.UTF8.GetBytes("中a文"), // varbinary(20)
                new byte[]
                {
                    0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59, 0x40
                }, // geometry(100)
                "-9999999999999999.9999", // decimal(20,4)
                "-9999.9999", // decimal(8,4)
            };
            Assert.Equal(expected.Length, cols);
            Assert.Equal(expected, values);

            cols = parser.GetValues(3, values);
            expected = new object[]
            {
                TDengineConstant.ConvertTimeToDatetime(1750324505986, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                true, // bool
                (sbyte)(1), // tinyint
                (short)(1), // smallint
                1, // int
                1L, // bigint
                (byte)1, // tinyint unsigned
                (ushort)1, // smallint unsigned
                1U, // int unsigned
                1UL, // bigint unsigned
                1F, // float
                1D, // double
                Encoding.UTF8.GetBytes("1"), // binary(20)
                "1", // nchar(20)
                Encoding.UTF8.GetBytes("1"), // varbinary(20)
                null, // geometry(100)
                "1.0000", // decimal(20,4)
                "1.0000", // decimal(8,4)
            };
            Assert.Equal(expected.Length, cols);
            Assert.Equal(expected, values);
        }
    }
}