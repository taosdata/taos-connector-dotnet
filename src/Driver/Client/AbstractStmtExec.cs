using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using TDengine.Driver.Impl.StmtBuilder;

namespace TDengine.Driver.Client
{
    public abstract partial class AbstractStmt
    {
        public void Exec()
        {
            if (!_addBatched)
            {
                throw new InvalidOperationException("No batch added. Call AddBatch() before Exec().");
            }

            try
            {
                var buffer = GenerateBindBinary();

                // print buffer
                // StringBuilder sb = new StringBuilder();
                // for (int i = 0; i < buffer.Length; i++)
                // {
                //     sb.Append($"0x{buffer[i]:X2}");
                //     if (i < buffer.Length - 1)
                //         sb.Append(", ");
                //     if (i % 16 == 15)
                //         sb.AppendLine();
                // }
                // Console.WriteLine(sb.ToString());
                var affectedRows = 0;
                try
                {
                    BindBinaryInternal(buffer, out affectedRows);
                }
                catch (Exception e)
                {
                    // if the connection is available, throw the exception directly
                    if (!AutoReconnectInternal() || IsConnectionAvailable(e)) throw;
                    // try reconnect
                    ReconnectInternal();
                    // prepare again
                    RePrepare();
                    // bind and execute again
                    BindBinaryInternal(buffer, out affectedRows);
                }

                if (_isInsert)
                {
                    _affectedRows = affectedRows;
                }
            }
            finally
            {
                CleanExec();
            }
        }


        private const int TotalLengthOffset = 0;
        private const int DataTypeOffset = 4;
        private const int NumOffset = 8;
        private const int IsNullOffset = 12;
        private const int HaveLengthOffset = 13;
        private const int FixedBufferLengthOffset = 14;
        private const int FixedBufferOffset = 18;

        private int WriteBindTag(TaosFieldE[] tagFields, object[] tags, byte[] buffer, int offset)
        {
            var startOffset = offset;
            for (var i = 0; i < tags.Length; i++)
            {
                var totalLength = 0;
                // write DataType
                WriteU32(buffer, startOffset + DataTypeOffset, (uint)tagFields[i].type);
                // write Num
                WriteU32(buffer, startOffset + NumOffset, 1);
                // hasLength
                bool isVarData = TDengineConstant.IsVarDataType((byte)tagFields[i].type);

                if (isVarData)
                {
                    buffer[startOffset + HaveLengthOffset] = 1;
                }
                else
                {
                    buffer[startOffset + HaveLengthOffset] = 0;
                }

                // isNull
                if (tags[i] == null)
                {
                    buffer[startOffset + IsNullOffset] = 1;
                    if (isVarData)
                    {
                        buffer[startOffset + HaveLengthOffset] = 0;
                        // write TotalLength
                        totalLength = 4 + // TotalLength field length
                                      4 + // DataType field length
                                      4 + // Num field length
                                      1 + // IsNull field length
                                      1 + // HaveLength field length
                                      4 + // Length field length, each length is 4 bytes
                                      4; // BufferLength field length
                    }
                    else
                    {
                        // write TotalLength
                        totalLength = 4 + // TotalLength field length
                                      4 + // DataType field length
                                      4 + // Num field length
                                      1 + // IsNull field length
                                      1 + // HaveLength field length
                                      4 + // BufferLength field length
                                      tagFields[i].bytes;
                    }

                    WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                }
                else
                {
                    buffer[startOffset + IsNullOffset] = 0;
                    switch (tags[i])
                    {
                        case bool boolVal:

                            // write TotalLength
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          1; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            // write BufferLength
                            WriteU32(buffer, startOffset + IsNullOffset + FixedBufferLengthOffset, 1);
                            buffer[startOffset + IsNullOffset + FixedBufferOffset] = boolVal ? (byte)1 : (byte)0;
                            break;
                        case sbyte sbyteVal:
                            // write TotalLength
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          1; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            // write BufferLength
                            WriteU32(buffer, startOffset + FixedBufferLengthOffset, 1);
                            buffer[startOffset + FixedBufferOffset] = (byte)sbyteVal;
                            break;
                        case byte byteVal:
                            // write TotalLength
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          1; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            // write BufferLength
                            WriteU32(buffer, startOffset + FixedBufferLengthOffset, 1);
                            buffer[startOffset + FixedBufferOffset] = byteVal;
                            break;
                        case short shortVal:
                            // write TotalLength
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          2; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            // write BufferLength
                            WriteU32(buffer, startOffset + FixedBufferLengthOffset, 2);
                            WriteU16(buffer, startOffset + FixedBufferOffset, (ushort)shortVal);
                            break;
                        case ushort ushortVal:
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          2; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            // write BufferLength
                            WriteU32(buffer, startOffset + FixedBufferLengthOffset, 2);
                            WriteU16(buffer, startOffset + FixedBufferOffset, ushortVal);
                            break;
                        case int intVal:
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          4; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            // write BufferLength
                            WriteU32(buffer, startOffset + FixedBufferLengthOffset, 4);
                            WriteU32(buffer, startOffset + FixedBufferOffset, (uint)intVal);
                            break;
                        case uint uintVal:
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          4; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            // write BufferLength
                            WriteU32(buffer, startOffset + FixedBufferLengthOffset, 4);
                            WriteU32(buffer, startOffset + FixedBufferOffset, uintVal);
                            break;
                        case long longVal:
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          8; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            // write BufferLength
                            WriteU32(buffer, startOffset + FixedBufferLengthOffset, 8);
                            WriteU64(buffer, startOffset + FixedBufferOffset, (ulong)longVal);
                            break;
                        case ulong ulongVal:
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          8; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            // write BufferLength
                            WriteU32(buffer, startOffset + FixedBufferLengthOffset, 8);
                            WriteU64(buffer, startOffset + FixedBufferOffset, ulongVal);
                            break;
                        case float floatVal:
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          4; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            // write BufferLength
                            WriteU32(buffer, startOffset + FixedBufferLengthOffset, 4);
#if NETSTANDARD2_1_OR_GREATER ||NET5_0_OR_GREATER||NETCOREAPP2_0_OR_GREATER
                            var floatInt = BitConverter.SingleToInt32Bits(floatVal);
                            WriteU32(buffer, startOffset + FixedBufferOffset, (uint)floatInt);
#else
                            var floatBytes = BitConverter.GetBytes(floatVal);
                            Buffer.BlockCopy(floatBytes, 0, buffer, startOffset + FixedBufferOffset, 4);
#endif
                            break;
                        case double doubleVal:
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          8; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            // write BufferLength
                            WriteU32(buffer, startOffset + FixedBufferLengthOffset, 8);
                            BitConverter.DoubleToInt64Bits(doubleVal);
                            WriteU64(buffer, startOffset + FixedBufferOffset, (ulong)doubleVal);
                            break;
                        case string strVal:
                            var len = Encoding.UTF8.GetByteCount(strVal);
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // Length field length, each length is 4 bytes
                                          4 + // BufferLength field length
                                          len; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            buffer[startOffset + HaveLengthOffset] = 1;
                            // write LengthField
                            WriteU32(buffer, startOffset + HaveLengthOffset + 1, (uint)len);
                            // write BufferLength
                            WriteU32(buffer, startOffset + HaveLengthOffset + 1 + 4, (uint)len);
                            // write Buffer
                            Encoding.UTF8.GetBytes(strVal, 0, strVal.Length, buffer,
                                startOffset + HaveLengthOffset + 1 + 4 + 4);
                            break;
                        case byte[] binVal:
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // Length field length, each length is 4 bytes
                                          4 + // BufferLength field length
                                          binVal.Length; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            buffer[startOffset + HaveLengthOffset] = 1;
                            // write LengthField
                            WriteU32(buffer, startOffset + HaveLengthOffset + 1, (uint)binVal.Length);
                            // write BufferLength
                            WriteU32(buffer, startOffset + HaveLengthOffset + 1 + 4, (uint)binVal.Length);
                            // write Buffer
                            Buffer.BlockCopy(binVal, 0, buffer, startOffset + HaveLengthOffset + 1 + 4 + 4,
                                binVal.Length);
                            break;
                        case DateTime dt:
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          8; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            // write BufferLength
                            WriteU32(buffer, startOffset + FixedBufferLengthOffset, 8);
                            var ts = TDengineConstant.ConvertDateTimeToTimestamp(dt,
                                (TDenginePrecision)tagFields[i].precision);
                            WriteU64(buffer, startOffset + FixedBufferOffset, (ulong)ts);
                            break;
                        case DateTimeOffset dto:
                            totalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          1 + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          8; // Buffer field length
                            WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                            // write BufferLength
                            WriteU32(buffer, startOffset + FixedBufferLengthOffset, 8);
                            var timestamp =
                                TDengineConstant.ConvertDateTimeOffsetToTimestamp(dto,
                                    (TDenginePrecision)tagFields[i].precision);
                            WriteU64(buffer, startOffset + FixedBufferOffset, (ulong)timestamp);
                            break;
                        default:
                            throw new ArgumentException(
                                $"tag fields type not support: {(TDengineDataType)tagFields[i].type}");
                    }
                }

                startOffset += totalLength;
            }

            return startOffset;
        }

        // public class Stmt2BindColInfo
        // {
        //     public uint TotalLength; // current Info total length, includes TotalLength field length
        //     public int DataType; // data type, see TDengineDataType
        //     public int Num; // how many rows of data, 1 for single row, >1 for multi rows
        //     public byte[] IsNull; // Num * 1, each row data is null or not, Num elements
        //
        //     public byte
        //         HaveLength; // 1, whether it has length, 0 for no, 1 for yes, when data type is variable length (binary, nchar, json, varbinary, varchar) must have length
        //
        //     public int[] Length; // each row data length, Num elements, when HaveLength is 0, this field is not used
        //     public uint BufferLength; // Buffer length, the length of the data in Buffer
        //     public byte[] Buffer; // bound data, the actual data buffer, the length is BufferLength
        // }
        private int WriteBindCol(TaosFieldE[] colFields, List<object>[] cols, int rows, byte[] buffer, int offset)
        {
            var startOffset = offset;
            var haveLengthOffset = IsNullOffset + rows;
            var fixedBufferLengthOffset = haveLengthOffset + 1;
            var fixedBufferOffset = fixedBufferLengthOffset + 4;
            var variableLengthOffset = haveLengthOffset + 1;
            var variableBufferLengthOffset = variableLengthOffset + (4 * rows);
            var variableBufferOffset = variableBufferLengthOffset + 4;
            for (int colIndex = 0; colIndex < cols.Length; colIndex++)
            {
                var totalLength = 0;
                // write DataType
                WriteU32(buffer, startOffset + DataTypeOffset, (uint)colFields[colIndex].type);
                // write Num
                WriteU32(buffer, startOffset + NumOffset, (uint)rows);
                // hasLength
                bool isVarData = TDengineConstant.IsVarDataType((byte)colFields[colIndex].type);
                if (isVarData)
                {
                    buffer[startOffset + haveLengthOffset] = 1;
                }

                if (isVarData)
                {
                    var variableOffset = startOffset + variableBufferOffset;
                    // variable length data
                    var totalVarBufferLength = 0;
                    for (int rowIndex = 0; rowIndex < rows; rowIndex++)
                    {
                        if (cols[colIndex][rowIndex] == null || Convert.IsDBNull(cols[colIndex][rowIndex]))
                        {
                            // is null
                            buffer[startOffset + IsNullOffset + rowIndex] = 1;
                            // length
                            WriteU32(buffer, startOffset + variableLengthOffset + rowIndex * 4,
                                (uint)0);
                        }
                        else
                        {
                            switch (cols[colIndex][rowIndex])
                            {
                                case string strVal:
                                {
                                    var length = Encoding.UTF8.GetByteCount(strVal);
                                    WriteU32(buffer, startOffset + variableLengthOffset + rowIndex * 4, (uint)length);
                                    Encoding.UTF8.GetBytes(strVal, 0, strVal.Length, buffer, variableOffset);
                                    totalVarBufferLength += length;
                                    variableOffset += length;
                                    break;
                                }
                                case byte[] binVal:
                                {
                                    WriteU32(buffer, startOffset + variableLengthOffset + rowIndex * 4,
                                        (uint)binVal.Length);
                                    Buffer.BlockCopy(binVal, 0, buffer, variableOffset, binVal.Length);
                                    totalVarBufferLength += binVal.Length;
                                    variableOffset += binVal.Length;
                                    break;
                                }
                                default:
                                    throw new NotSupportedException(
                                        $"col field type not support: {(TDengineDataType)colFields[colIndex].type}");
                            }
                        }
                    }

                    totalLength = 4 + // TotalLength field length
                                  4 + // DataType field length
                                  4 + // Num field length
                                  (1 * rows) + // IsNull field length
                                  1 + // HaveLength field length
                                  (4 * rows) + // Length field length, each length is 4 bytes
                                  4 + // BufferLength field length
                                  totalVarBufferLength; // Buffer field length
                    // write TotalLength
                    WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                    // write BufferLength
                    WriteU32(buffer, startOffset + variableBufferLengthOffset, (uint)totalVarBufferLength);
                }
                else
                {
                    var totalFixedBufferLength = 0;
                    var typeLength = TDengineConstant.TypeLengthMap[(TDengineDataType)colFields[colIndex].type];
                    var fixedOffset = startOffset + fixedBufferOffset;
                    for (int rowIndex = 0; rowIndex < rows; rowIndex++)
                    {
                        if (cols[colIndex][rowIndex] == null || Convert.IsDBNull(cols[colIndex][rowIndex]))
                        {
                            buffer[startOffset + IsNullOffset + rowIndex] = 1;
                        }
                        else
                        {
                            switch (cols[colIndex][rowIndex])
                            {
                                case DateTimeOffset dto:
                                    var timestamp = TDengineConstant.ConvertDateTimeOffsetToTimestamp(dto,
                                        (TDenginePrecision)colFields[colIndex].precision);
                                    WriteU64(buffer, fixedOffset, (ulong)timestamp);
                                    break;
                                case DateTime dt:
                                    var ts = TDengineConstant.ConvertDateTimeToTimestamp(dt,
                                        (TDenginePrecision)colFields[colIndex].precision);
                                    WriteU64(buffer, fixedOffset, (ulong)ts);
                                    break;
                                case bool boolVal:
                                    buffer[fixedOffset] = boolVal ? (byte)1 : (byte)0;
                                    break;
                                case sbyte sbyteVal:
                                    buffer[fixedOffset] = (byte)sbyteVal;
                                    break;
                                case byte byteVal:
                                    buffer[fixedOffset] = byteVal;
                                    break;
                                case short shortVal:
                                    WriteU16(buffer, fixedOffset, (ushort)shortVal);
                                    break;
                                case ushort ushortVal:
                                    WriteU16(buffer, fixedOffset, ushortVal);
                                    break;
                                case int intVal:
                                    WriteU32(buffer, fixedOffset, (uint)intVal);
                                    break;
                                case uint uintVal:
                                    WriteU32(buffer, fixedOffset, uintVal);
                                    break;
                                case long longVal:
                                    WriteU64(buffer, fixedOffset, (ulong)longVal);
                                    break;
                                case ulong ulongVal:
                                    WriteU64(buffer, fixedOffset, ulongVal);
                                    break;
                                case float floatVal:
#if NETSTANDARD2_1_OR_GREATER ||NET5_0_OR_GREATER||NETCOREAPP2_0_OR_GREATER
                                    var floatInt = BitConverter.SingleToInt32Bits(floatVal);
                                    WriteU32(buffer, fixedOffset, (uint)floatInt);
#else
                                    var floatBytes = BitConverter.GetBytes(floatVal);
                                    Buffer.BlockCopy(floatBytes, 0, buffer, fixedOffset, 4);
#endif
                                    break;
                                case double doubleVal:
                                    var doubleInt = BitConverter.DoubleToInt64Bits(doubleVal);
                                    WriteU64(buffer, fixedOffset, (ulong)doubleInt);
                                    break;
                                default:
                                    throw new NotSupportedException(
                                        $"col field type not support: {(TDengineDataType)colFields[colIndex].type}");
                            }
                        }

                        totalFixedBufferLength += typeLength;
                        fixedOffset += typeLength;
                    }

                    totalLength = 4 + // TotalLength field length
                                  4 + // DataType field length
                                  4 + // Num field length
                                  (1 * rows) + // IsNull field length
                                  1 + // HaveLength field length
                                  4 + // BufferLength field length
                                  totalFixedBufferLength; // Buffer field length
                    // write TotalLength
                    WriteU32(buffer, startOffset + TotalLengthOffset, (uint)totalLength);
                    // write BufferLength
                    WriteU32(buffer, startOffset + FixedBufferLengthOffset, (uint)totalFixedBufferLength);
                }

                startOffset += totalLength;
            }

            return startOffset;
        }


        private byte[] GenerateBindBinary()
        {
            var tableCount = _tableInfos.Count;
            var colCount = _isInsert ? _colFields.Length : _fieldsCount;
            var colFields = _isInsert ? _colFields : _queryFields;
            const uint fixedHeaderLen = 28;
            var tableNameLengthLen = (uint)0;
            var tableNameBufferLen = (uint)0;

            var tagsDataLengthLen = (uint)0;
            var tagsBufferLen = (uint)0;
            var colsDataLengthLen = (uint)(tableCount * 4);
            var colsBufferLen = (uint)0;

            var utf8TableNameLen = new short[_needTableName ? tableCount : 0];
            var tableTagLengthList = new uint[NeedTags ? tableCount : 0];
            var tableColLengthList = new uint[tableCount];
            var tableNames = new string[tableCount];
            var tmpTableIndex = 0;
            foreach (var tableInfo in _tableInfos)
            {
                // calculate table name
                if (_needTableName)
                {
                    var bsCount = Encoding.UTF8.GetByteCount(tableInfo.Key);
                    utf8TableNameLen[tmpTableIndex] = (short)(bsCount + 1);
                    tableNameBufferLen += (uint)(bsCount + 1);
                    tableNames[tmpTableIndex] = tableInfo.Key;
                }
                else
                {
                    tableNames[0] = string.Empty;
                }

                // calculate tags
                if (NeedTags)
                {
                    var tableTagLength = (uint)0;
                    for (int i = 0; i < _tagFields.Length; i++)
                    {
                        if (TDengineConstant.IsVarDataType((byte)_tagFields[i].type))
                        {
                            // variant type
                            var bsCount = 0;
                            switch (tableInfo.Value.Tags[i])
                            {
                                case null:
                                    break;
                                case string strVal:
                                {
                                    bsCount = Encoding.UTF8.GetByteCount(strVal);
                                    break;
                                }
                                case byte[] binVal:
                                {
                                    bsCount = binVal.Length;
                                    break;
                                }
                                default:
                                    throw new NotSupportedException(
                                        $"tag field type not support: {(TDengineDataType)_tagFields[i].type}");
                            }

                            uint totalLength = 4 + // TotalLength field length
                                               4 + // DataType field length
                                               4 + // Num field length
                                               (uint)1 + // IsNull field length
                                               1 + // HaveLength field length
                                               4 + // Length field length, each length is 4 bytes
                                               4 + // BufferLength field length
                                               (uint)bsCount; // Buffer field length
                            tableTagLength += totalLength;
                            tagsBufferLen += totalLength;
                        }
                        else
                        {
                            var typeLength = TDengineConstant.TypeLengthMap[(TDengineDataType)_tagFields[i].type];
                            uint totalLength = 4 + // TotalLength field length
                                               4 + // DataType field length
                                               4 + // Num field length
                                               (uint)1 + // IsNull field length
                                               1 + // HaveLength field length
                                               4 + // BufferLength field length
                                               (uint)typeLength; // Buffer field length
                            tableTagLength += totalLength;
                            tagsBufferLen += totalLength;
                        }
                    }

                    tableTagLengthList[tmpTableIndex] = tableTagLength;
                }

                // calculate cols
                var tableColLength = (uint)0;
                var rows = tableInfo.Value.Rows;
                for (int i = 0; i < colCount; i++)
                {
                    if (TDengineConstant.IsVarDataType((byte)colFields[i].type))
                    {
                        // variant type
                        var bsCount = 0;
                        for (int j = 0; j < rows; j++)
                        {
                            switch (tableInfo.Value.Cols[i][j])
                            {
                                case null:
                                    break;
                                case string strVal:
                                {
                                    bsCount += Encoding.UTF8.GetByteCount(strVal);
                                    break;
                                }
                                case byte[] binVal:
                                {
                                    bsCount += binVal.Length;
                                    break;
                                }
                                default:
                                    throw new NotSupportedException(
                                        $"col field type not support: {(TDengineDataType)colFields[i].type}");
                            }
                        }

                        uint totalLength = 4 + // TotalLength field length
                                           4 + // DataType field length
                                           4 + // Num field length
                                           (uint)(1 * rows) + // IsNull field length
                                           1 + // HaveLength field length
                                           (uint)(4 * rows) + // Length field length, each length is 4 bytes
                                           4 + // BufferLength field length
                                           (uint)bsCount; // Buffer field length
                        tableColLength += totalLength;
                        colsBufferLen += totalLength;
                    }
                    else
                    {
                        var typeLength = TDengineConstant.TypeLengthMap[(TDengineDataType)colFields[i].type];
                        uint totalLength = 4 + // TotalLength field length
                                           4 + // DataType field length
                                           4 + // Num field length
                                           (uint)(1 * rows) + // IsNull field length
                                           1 + // HaveLength field length
                                           4 + // BufferLength field length
                                           (uint)(typeLength * rows); // Buffer field length
                        tableColLength += totalLength;
                        colsBufferLen += totalLength;
                    }
                }

                tableColLengthList[tmpTableIndex] = tableColLength;
                tmpTableIndex++;
            }

            // table name
            if (_needTableName)
            {
                tableNameLengthLen = (uint)(tableCount * 2);
            }

            if (NeedTags)
            {
                tagsDataLengthLen = (uint)(tableCount * 4);
            }

            var tableNameLength = tableNameLengthLen + tableNameBufferLen;
            var tagsDataLength = tagsDataLengthLen + tagsBufferLen;
            var colsDataLength = colsDataLengthLen + colsBufferLen;
            var totalBufferLen = fixedHeaderLen + tableNameLength + tagsDataLength + colsDataLength;
            var tableNameOffset = fixedHeaderLen;
            var tagsOffset = tableNameOffset + tableNameLength;
            var colsOffset = tagsOffset + tagsDataLength;
            var buffer = new byte[totalBufferLen + _binaryHeaderLength];
            WriteU32(buffer, _binaryHeaderLength + 0, totalBufferLen); // TotalLength
            WriteU32(buffer, _binaryHeaderLength + 4, (uint)tableCount); // Count
            WriteU32(buffer, _binaryHeaderLength + 8, NeedTags ? (uint)_tagFields.Length : 0); // TagCount
            WriteU32(buffer, _binaryHeaderLength + 12, (uint)colCount); // ColCount
            WriteU32(buffer, _binaryHeaderLength + 16, _needTableName ? fixedHeaderLen : 0); // TableNamesOffset
            WriteU32(buffer, _binaryHeaderLength + 20, NeedTags ? tagsOffset : 0); // TagsOffset
            WriteU32(buffer, _binaryHeaderLength + 24, colsOffset); // ColsOffset
            var tableNameLengthOffset = _binaryHeaderLength + (int)tableNameOffset;
            var tableNameBufferOffset = tableNameLengthOffset + (int)tableNameLengthLen;
            var tagsLengthOffset = _binaryHeaderLength + (int)tagsOffset;
            var tagsBufferOffset = tagsLengthOffset + (int)tagsDataLengthLen;
            var colsLengthOffset = _binaryHeaderLength + (int)colsOffset;
            var colsBufferOffset = colsLengthOffset + (int)colsDataLengthLen;
            if (NeedTags)
            {
                // tags length
                Buffer.BlockCopy(tableTagLengthList, 0, buffer, tagsLengthOffset, (int)tagsDataLengthLen);
            }

            // cols length
            Buffer.BlockCopy(tableColLengthList, 0, buffer, colsLengthOffset, (int)colsDataLengthLen);

            if (_needTableName)
            {
                Buffer.BlockCopy(utf8TableNameLen, 0, buffer, tableNameLengthOffset,
                    (int)tableNameLengthLen);
            }

            var tmpTableNameOffset = tableNameBufferOffset;

            var tagOffset = tagsBufferOffset;
            var colOffset = colsBufferOffset;
            for (int tableIndex = 0; tableIndex < tableCount; tableIndex++)
            {
                var tableName = tableNames[tableIndex];
                if (_needTableName)
                {
                    // write table name
                    Encoding.UTF8.GetBytes(tableName, 0, tableName.Length, buffer, tmpTableNameOffset);
                    tmpTableNameOffset += utf8TableNameLen[tableIndex];
                }

                var bindData = _tableInfos[tableName];
                // write tags
                if (NeedTags)
                {
                    // tags data
                    tagOffset = WriteBindTag(_tagFields, bindData.Tags, buffer, tagOffset);
                }

                // write cols
                colOffset = WriteBindCol(colFields, bindData.Cols, bindData.Rows, buffer, colOffset);
            }

            return buffer;
        }

        // little-endian write int32 to buffer
        private static void WriteU32(byte[] buffer, int offset, uint value)
        {
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
            buffer[offset + 2] = (byte)(value >> 16);
            buffer[offset + 3] = (byte)(value >> 24);
        }

        private static void WriteU64(byte[] buffer, int offset, ulong value)
        {
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
            buffer[offset + 2] = (byte)(value >> 16);
            buffer[offset + 3] = (byte)(value >> 24);
            buffer[offset + 4] = (byte)(value >> 32);
            buffer[offset + 5] = (byte)(value >> 40);
            buffer[offset + 6] = (byte)(value >> 48);
            buffer[offset + 7] = (byte)(value >> 56);
        }


        private static void WriteU16(byte[] buffer, int offset, ushort value)
        {
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
        }

        protected abstract void BindBinaryInternal(byte[] data, out int affectedRows);
    }
}