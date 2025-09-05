using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
#if NETSTANDARD2_1_OR_GREATER ||NET5_0_OR_GREATER||NETCOREAPP2_0_OR_GREATER
using System.Runtime.InteropServices;
#endif

namespace TDengine.Driver.Client
{
    class ColInfo
    {
        public TaosFieldType FieldType; // 类型 
        public int FieldIndex; // tag 或列数据索引
        public bool IsVariable; // 是否变长
        public int FixedLength; // 定长类型长度
        public uint BufferLength; // Buffer长度
        public uint TotalLength; // Bind 结构总长度
        public int NextBufferOffset; // 下一个写入的 buffer offset，变长需要随时移动
        public int StartOffset; // start offset 
        public int IsNullOffset; // is_null offset
        public int LengthsOffset; // lengths offset
        public int NextWriteIndex; // 下一个写入的行索引，用来计算 is_null 和 lengths 的写入位置
        public TaosFieldAll Field;

        public ColInfo(TaosFieldAll field)
        {
            Field = field;
        }
    }

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
                // var buffer = GenerateBindBinary();
                var buffer = GenerateAsColumnsBindBinary();
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
                int affectedRows;
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
                uint totalLength;
                // write DataType
                WriteU32(buffer, startOffset + DataTypeOffset, (uint)tagFields[i].type);
                // write Num
                WriteU32(buffer, startOffset + NumOffset, 1);
                // hasLength
                bool isVarData = TDengineConstant.IsVarDataType((byte)tagFields[i].type);

                // isNull
                if (tags[i] == null || Convert.IsDBNull(tags[i]))
                {
                    buffer[startOffset + IsNullOffset] = 1;
                    if (isVarData)
                    {
                        // have length
                        buffer[startOffset + HaveLengthOffset] = 1;
                        // length
                        WriteU32(buffer, startOffset + HaveLengthOffset + 4, 0);
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
                        var dataLength = (uint)TDengineConstant.TypeLengthMap[(TDengineDataType)tagFields[i].type];
                        totalLength = 4 + // TotalLength field length
                                      4 + // DataType field length
                                      4 + // Num field length
                                      1 + // IsNull field length
                                      1 + // HaveLength field length
                                      4 + // BufferLength field length
                                      dataLength;
                        WriteU32(buffer, startOffset + FixedBufferLengthOffset, dataLength);
                    }

                    WriteU32(buffer, startOffset + TotalLengthOffset, totalLength);
                }
                else
                {
                    if (!isVarData)
                    {
                        var dataLength = (uint)TDengineConstant.TypeLengthMap[(TDengineDataType)tagFields[i].type];

                        switch (tags[i])
                        {
                            case bool boolVal:
                                buffer[startOffset + FixedBufferOffset] = boolVal ? (byte)1 : (byte)0;
                                break;
                            case sbyte sbyteVal:
                                buffer[startOffset + FixedBufferOffset] = (byte)sbyteVal;
                                break;
                            case byte byteVal:
                                buffer[startOffset + FixedBufferOffset] = byteVal;
                                break;
                            case short shortVal:
                                WriteU16(buffer, startOffset + FixedBufferOffset, (ushort)shortVal);
                                break;
                            case ushort ushortVal:
                                WriteU16(buffer, startOffset + FixedBufferOffset, ushortVal);
                                break;
                            case int intVal:
                                WriteU32(buffer, startOffset + FixedBufferOffset, (uint)intVal);
                                break;
                            case uint uintVal:
                                WriteU32(buffer, startOffset + FixedBufferOffset, uintVal);
                                break;
                            case long longVal:
                                WriteU64(buffer, startOffset + FixedBufferOffset, (ulong)longVal);
                                break;
                            case ulong ulongVal:
                                WriteU64(buffer, startOffset + FixedBufferOffset, ulongVal);
                                break;
                            case float floatVal:
#if NETSTANDARD2_1_OR_GREATER ||NET5_0_OR_GREATER||NETCOREAPP2_0_OR_GREATER
                                var floatInt = BitConverter.SingleToInt32Bits(floatVal);
                                WriteU32(buffer, startOffset + FixedBufferOffset, (uint)floatInt);
#else
                                var floatBytes = BitConverter.GetBytes(floatVal);
                                Buffer.BlockCopy(floatBytes, 0, buffer, startOffset + FixedBufferOffset, 4);
#endif
                                break;
                            case double doubleVal:
                                // write BufferLength
                                var doubleInt = BitConverter.DoubleToInt64Bits(doubleVal);
                                WriteU64(buffer, startOffset + FixedBufferOffset, (ulong)doubleInt);
                                break;
                            case DateTime dt:
                                var ts = TDengineConstant.ConvertDateTimeToTimestamp(dt,
                                    (TDenginePrecision)tagFields[i].precision);
                                WriteU64(buffer, startOffset + FixedBufferOffset, (ulong)ts);
                                break;
                            case DateTimeOffset dto:
                                var timestamp =
                                    TDengineConstant.ConvertDateTimeOffsetToTimestamp(dto,
                                        (TDenginePrecision)tagFields[i].precision);
                                WriteU64(buffer, startOffset + FixedBufferOffset, (ulong)timestamp);
                                break;
                            default:
                                throw new ArgumentException(
                                    $"tag fields type not support: {(TDengineDataType)tagFields[i].type}, value: {tags[i]}");
                        }

                        totalLength = 4 + // TotalLength field length
                                      4 + // DataType field length
                                      4 + // Num field length
                                      1 + // IsNull field length
                                      1 + // HaveLength field length
                                      4 + // BufferLength field length
                                      dataLength; // Buffer field length
                        WriteU32(buffer, startOffset + TotalLengthOffset, totalLength);
                        // write BufferLength
                        WriteU32(buffer, startOffset + FixedBufferLengthOffset, dataLength);
                    }
                    else
                    {
                        uint dataLength;
                        switch (tags[i])
                        {
                            case string strVal:
                                dataLength = (uint)Encoding.UTF8.GetByteCount(strVal);
                                // write Buffer
                                Encoding.UTF8.GetBytes(strVal, 0, strVal.Length, buffer,
                                    startOffset + HaveLengthOffset + 1 + 4 + 4);
                                break;
                            case byte[] binVal:
                                dataLength = (uint)binVal.Length;
                                // write Buffer
                                Buffer.BlockCopy(binVal, 0, buffer, startOffset + HaveLengthOffset + 1 + 4 + 4,
                                    binVal.Length);
                                break;
                            default:
                                throw new ArgumentException(
                                    $"tag fields type not support: {(TDengineDataType)tagFields[i].type}, value: {tags[i]}");
                        }

                        totalLength = 4 + // TotalLength field length
                                      4 + // DataType field length
                                      4 + // Num field length
                                      1 + // IsNull field length
                                      1 + // HaveLength field length
                                      4 + // Length field length, each length is 4 bytes
                                      4 + // BufferLength field length
                                      dataLength; // Buffer field length
                        WriteU32(buffer, startOffset + TotalLengthOffset, totalLength);
                        buffer[startOffset + HaveLengthOffset] = 1;
                        // write LengthField
                        WriteU32(buffer, startOffset + HaveLengthOffset + 1, dataLength);
                        // write BufferLength
                        WriteU32(buffer, startOffset + HaveLengthOffset + 1 + 4, dataLength);
                    }
                }

                startOffset += (int)totalLength;
            }

            return startOffset;
        }

        private int WriteBindCol(TaosFieldE[] colFields, List<object>[] cols, int rows, byte[] buffer, int offset)
        {
            var startOffset = offset;
            var haveLengthOffset = IsNullOffset + rows;
            var fixedBufferLengthOffset = haveLengthOffset + 1;
            var fixedBufferOffset = fixedBufferLengthOffset + 4;
            var variableLengthOffset = haveLengthOffset + 1;
            var variableBufferLengthOffset = variableLengthOffset + (4 * rows);
            var variableBufferOffset = variableBufferLengthOffset + 4;
            for (var colIndex = 0; colIndex < cols.Length; colIndex++)
            {
                var colData = cols[colIndex];
                int totalLength;
                // write DataType
                WriteU32(buffer, startOffset + DataTypeOffset, (uint)colFields[colIndex].type);
                // write Num
                WriteU32(buffer, startOffset + NumOffset, (uint)rows);
                // hasLength
                var isVarData = TDengineConstant.IsVarDataType((byte)colFields[colIndex].type);
                if (isVarData)
                {
                    buffer[startOffset + haveLengthOffset] = 1;
                    var variableOffset = startOffset + variableBufferOffset;
                    // variable length data
                    var totalVarBufferLength = 0;
                    for (var rowIndex = 0; rowIndex < rows; rowIndex++)
                    {
                        var value = colData[rowIndex];
                        if (value == null || Convert.IsDBNull(value))
                        {
                            // is null
                            buffer[startOffset + IsNullOffset + rowIndex] = 1;
                            // length
                            // WriteU32(buffer, startOffset + variableLengthOffset + rowIndex * 4, 0);
                        }
                        else
                        {
                            switch (value)
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
                                        $"col field type not support: {(TDengineDataType)colFields[colIndex].type}, value: {value}");
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
                    for (var rowIndex = 0; rowIndex < rows; rowIndex++)
                    {
                        var value = colData[rowIndex];
                        if (value == null || Convert.IsDBNull(value))
                        {
                            buffer[startOffset + IsNullOffset + rowIndex] = 1;
                        }
                        else
                        {
                            switch (value)
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
                    WriteU32(buffer, startOffset + fixedBufferLengthOffset, (uint)totalFixedBufferLength);
                }

                startOffset += totalLength;
            }

            return startOffset;
        }

        // type Protocol struct {
        //     TotalLength      uint32 // 4, 当前 TagData 的全部长度,包括 TotalLength 字段长度
        //     Count            int32  // 4 固定值 1
        //     TagCount         int32  // 4 固定值 0
        //     ColCount         int32  // 4 列数
        //     TableNamesOffset uint32 // 4 固定值 0
        //     TagsOffset       uint32 // 4 固定值 0
        //     ColsOffset       uint32 // 4 固定值 28
        //
        //
        //     ColDataLength []uint32 // cols 的长度, 1 个元素
        //     ColBuffer     []byte   // col 的 buffer 列数个 BindData
        // }
        //
        // type BindData struct {
        //     TotalLength  uint32  // 4, 当前 TagData 的全部长度,包括 TotalLength 字段长度
        //     Type         int32   // 4, 数据类型
        //     Num          int32   // 4, 多少行数据
        //     IsNull       []byte  // Num * 1 每个 tag 是否为 null, Num 个元素
        //     haveLength   byte    // 1, 是否有长度，0 为没有，1 为有，当数据类型为变长时必须有长度（binary, nchar, json, varbinary, varchar）
        //     Length       []int32 // Num * 4 每个 tag 的长度, Num 个元素，当 hasLength 为 0 时，无该字段
        //     BufferLength uint32  // 4, Buffer 的长度
        //     Buffer       []byte  // 绑定数据
        // }


        private byte[] GenerateAsColumnsBindBinary()
        {
            var colFields = _isInsert ? _fields : _queryFields;
            var colCount = colFields.Length;
            const uint fixedHeaderLen = 28;

            // var colsDataLengthLen = (uint)4;
            var colsBufferLen = (uint)0;
            // var fieldIndex = new int[colCount];
            var variableColIndexes = new List<int>();
            var colIndex = 0;
            var tagIndex = 0;
            var colInfos = new ColInfo[colCount];
            for (var index = 0; index < colCount; index++)
            {
                var colField = colFields[index];
                var colInfo = new ColInfo(colField);
                switch ((TaosFieldType)colField.field_type)
                {
                    case TaosFieldType.TAOS_FIELD_COL:
                        colInfo.FieldType = TaosFieldType.TAOS_FIELD_COL;
                        colInfo.FieldIndex = colIndex;
                        colInfo.IsVariable = TDengineConstant.IsVarDataType((byte)colField.type);
                        if (colInfo.IsVariable)
                        {
                            // variant type
                            variableColIndexes.Add(index);
                        }
                        else
                        {
                            var typeLength = TDengineConstant.TypeLengthMap[(TDengineDataType)colField.type];
                            colInfo.FixedLength = typeLength;
                        }

                        colIndex++;
                        break;
                    case TaosFieldType.TAOS_FIELD_TAG:
                        colInfo.FieldType = TaosFieldType.TAOS_FIELD_TAG;
                        colInfo.FieldIndex = tagIndex;
                        colInfo.IsVariable = TDengineConstant.IsVarDataType((byte)colField.type);
                        if (colInfo.IsVariable)
                        {
                            // variant type
                            variableColIndexes.Add(index);
                        }
                        else
                        {
                            var typeLength = TDengineConstant.TypeLengthMap[(TDengineDataType)colField.type];
                            colInfo.FixedLength = typeLength;
                        }

                        tagIndex++;
                        break;
                    case TaosFieldType.TAOS_FIELD_TBNAME:
                        colInfo.FieldType = TaosFieldType.TAOS_FIELD_TBNAME;
                        colInfo.IsVariable = true;
                        variableColIndexes.Add(index);
                        break;
                    default:
                        throw new NotSupportedException(
                            $"stmt field type not support: {(TaosFieldType)colFields[index].field_type}");
                }

                colInfos[index] = colInfo;
            }

            // 计算总长度
            var totalRows = 0;
            foreach (var tableInfo in _tableInfos)
            {
                var rows = tableInfo.Value.Rows;
                // 计算变长类型长度
                foreach (var variableColIndex in variableColIndexes)
                {
                    var bufferLength = (uint)0;
                    var colInfo = colInfos[variableColIndex];
                    int bsCount;
                    switch (colInfo.FieldType)
                    {
                        case TaosFieldType.TAOS_FIELD_COL:
                            var colValues = tableInfo.Value.Cols[colInfo.FieldIndex];
                            foreach (var colValue in colValues)
                            {
                                if (colValue == null || Convert.IsDBNull(colValue))
                                {
                                    continue;
                                }

                                switch (colValue)
                                {
                                    case string str:
                                        bsCount = Encoding.UTF8.GetByteCount(str);
                                        bufferLength += (uint)bsCount;
                                        break;
                                    case byte[] byteArray:
                                        bufferLength += (uint)byteArray.Length;
                                        break;
                                    default:
                                        throw new NotSupportedException(
                                            $"col field type not support: {(TDengineDataType)_colFields[colInfo.FieldIndex].type}, value: {colValue}");
                                }
                            }

                            break;
                        case TaosFieldType.TAOS_FIELD_TBNAME:
                            bsCount = Encoding.UTF8.GetByteCount(tableInfo.Key);
                            bufferLength += (uint)bsCount * (uint)rows;
                            break;
                        case TaosFieldType.TAOS_FIELD_TAG:
                            var tagValue = tableInfo.Value.Tags[colInfo.FieldIndex];
                            if (tagValue == null || Convert.IsDBNull(tagValue))
                            {
                                continue;
                            }

                            switch (tagValue)
                            {
                                case string str:
                                    bsCount = Encoding.UTF8.GetByteCount(str);
                                    bufferLength += (uint)bsCount * (uint)rows;
                                    break;
                                case byte[] byteArray:
                                    bufferLength += (uint)byteArray.Length * (uint)rows;
                                    break;
                                default:
                                    throw new NotSupportedException(
                                        $"tag field type not support: {(TDengineDataType)_tagFields[colInfo.FieldIndex].type}, value: {tagValue}");
                            }

                            break;
                        default:
                            throw new NotSupportedException(
                                $"stmt field type not support: {colInfo.FieldType}");
                    }

                    colInfo.BufferLength += bufferLength;
                }

                totalRows += rows;
            }

            var colsOffset = fixedHeaderLen;
            var colsLengthOffset = _binaryHeaderLength + (int)colsOffset;
            var colsBufferOffset = colsLengthOffset + 4;

            for (var index = 0; index < colInfos.Length; index++)
            {
                var colInfo = colInfos[index];
                if (colInfo.IsVariable)
                {
                    colInfo.TotalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          (uint)(totalRows * 1) + // IsNull field length
                                          1 + // HaveLength field length
                                          (uint)(totalRows * 4) + // Length field length, each length is 4 bytes
                                          4 + // BufferLength field length
                                          colInfo.BufferLength; // Buffer field length
                    colInfo.StartOffset = index == 0
                        ? colsBufferOffset
                        : (int)colInfos[index - 1].TotalLength + colInfos[index - 1].StartOffset;
                    colInfo.IsNullOffset = colInfo.StartOffset + 12;
                    colInfo.LengthsOffset = colInfo.IsNullOffset + totalRows + 1;
                    colInfo.NextBufferOffset = colInfo.LengthsOffset + totalRows * 4 + 4;
                }
                else
                {
                    colInfo.BufferLength = (uint)(colInfo.FixedLength * totalRows);
                    colInfo.TotalLength = 4 + // TotalLength field length
                                          4 + // DataType field length
                                          4 + // Num field length
                                          (uint)(totalRows * 1) + // IsNull field length
                                          1 + // HaveLength field length
                                          4 + // BufferLength field length
                                          colInfo.BufferLength; // Buffer field length
                    colInfo.StartOffset = index == 0
                        ? colsBufferOffset
                        : (int)colInfos[index - 1].TotalLength + colInfos[index - 1].StartOffset;
                    colInfo.IsNullOffset = colInfo.StartOffset + 12;
                    colInfo.NextBufferOffset = colInfo.IsNullOffset + totalRows + 1 + 4;
                }

                colsBufferLen += colInfo.TotalLength;
            }



            var totalBufferLen = fixedHeaderLen + 4 + colsBufferLen;

            var buffer = new byte[totalBufferLen + _binaryHeaderLength];
            WriteU32(buffer, _binaryHeaderLength + 0, totalBufferLen); // TotalLength
            WriteU32(buffer, _binaryHeaderLength + 4, 1); // Count
            WriteU32(buffer, _binaryHeaderLength + 8, 0); // TagCount
            WriteU32(buffer, _binaryHeaderLength + 12, (uint)colCount); // ColCount
            WriteU32(buffer, _binaryHeaderLength + 16, 0); // TableNamesOffset
            WriteU32(buffer, _binaryHeaderLength + 20, 0); // TagsOffset
            WriteU32(buffer, _binaryHeaderLength + 24, colsOffset); // ColsOffset
            WriteU32(buffer, colsLengthOffset, colsBufferLen);
            // type BindData struct {
            //     TotalLength  uint32  // 4, 当前 TagData 的全部长度,包括 TotalLength 字段长度
            //     Type         int32   // 4, 数据类型
            //     Num          int32   // 4, 多少行数据
            //     IsNull       []byte  // Num * 1 每个 tag 是否为 null, Num 个元素
            //     haveLength   byte    // 1, 是否有长度，0 为没有，1 为有，当数据类型为变长时必须有长度（binary, nchar, json, varbinary, varchar）
            //     Length       []int32 // Num * 4 每个 tag 的长度, Num 个元素，当 hasLength 为 0 时，无该字段
            //     BufferLength uint32  // 4, Buffer 的长度
            //     Buffer       []byte  // 绑定数据
            // }
            foreach (var colInfo in colInfos)
            {
                WriteU32(buffer,colInfo.StartOffset,colInfo.TotalLength);
                WriteU32(buffer,colInfo.StartOffset+4,(uint)colInfo.Field.type);
                WriteU32(buffer,colInfo.StartOffset+8,(uint)totalRows);
                if (colInfo.IsVariable)
                {
                    buffer[colInfo.IsNullOffset + totalRows] = 1;
                }
                WriteU32(buffer,colInfo.NextBufferOffset - 4, colInfo.BufferLength);
            }
            foreach (var tableData in _tableInfos.Values)
            {
                foreach (var colInfo in colInfos)
                {
                    switch (colInfo.FieldType)
                    {
                        case TaosFieldType.TAOS_FIELD_COL:
                            var colData = tableData.Cols[colInfo.FieldIndex];
                            if (colInfo.IsVariable)
                            {
                                for (int i = 0; i < colData.Count; i++)
                                {
                                    var value = colData[i];
                                    WriteVariableValue(buffer, colInfo, value);
                                }
                            }
                            else
                            {
                                for (int i = 0; i < colData.Count; i++)
                                {
                                    var value = colData[i];
                                    WriteFixedValue(buffer, colInfo, value);
                                }
                            }

                            break;
                        case TaosFieldType.TAOS_FIELD_TAG:
                            for (int i = 0; i < tableData.Rows; i++)
                            {
                                var tagValue = tableData.Tags[colInfo.FieldIndex];
                                if (colInfo.IsVariable)
                                {
                                    WriteVariableValue(buffer, colInfo, tagValue);
                                }
                                else
                                {
                                    WriteFixedValue(buffer, colInfo, tagValue);
                                }
                            }

                            break;
                        case TaosFieldType.TAOS_FIELD_TBNAME:
                            for (int i = 0; i < tableData.Rows; i++)
                            {
                                WriteVariableValue(buffer, colInfo, tableData.TableName);
                            }
                            break;
                        default:
                            throw new NotSupportedException(
                                $"stmt field type not support: {colInfo.FieldType}");
                    }
                }
            }

            return buffer;
        }

        private void WriteVariableValue(byte[] buffer, ColInfo colInfo, object value)
        {
            if (value == null || Convert.IsDBNull(value))
            {
                // is null
                buffer[colInfo.IsNullOffset + colInfo.NextWriteIndex] = 1;
                // length
                // WriteU32(buffer, colInfo.CurrentBufferOffset + VariableLengthOffset + i * 4, 0);
            }
            else
            {
                switch (value)
                {
                    case string strVal:
                    {
                        var length = Encoding.UTF8.GetByteCount(strVal);
                        WriteU32(buffer, colInfo.LengthsOffset + colInfo.NextWriteIndex * 4,
                            (uint)length);
                        Encoding.UTF8.GetBytes(strVal, 0, strVal.Length, buffer,
                            colInfo.NextBufferOffset);
                        colInfo.NextBufferOffset += length;
                        break;
                    }
                    case byte[] binVal:
                    {
                        WriteU32(buffer, colInfo.LengthsOffset + colInfo.NextWriteIndex * 4,
                            (uint)binVal.Length);
                        Buffer.BlockCopy(binVal, 0, buffer, colInfo.NextBufferOffset,
                            binVal.Length);
                        colInfo.NextBufferOffset += binVal.Length;
                        break;
                    }
                    default:
                        throw new NotSupportedException(
                            $"col field type not support: {(TDengineDataType)_colFields[colInfo.FieldIndex].type}, value: {value}");
                }
            }

            colInfo.NextWriteIndex += 1;
        }

        private void WriteFixedValue(byte[] buffer, ColInfo colInfo, object value)
        {
            if (value == null || Convert.IsDBNull(value))
            {
                buffer[colInfo.IsNullOffset + colInfo.NextWriteIndex] = 1;
            }
            else
            {
                switch (value)
                {
                    case DateTimeOffset dto:
                        var timestamp = TDengineConstant.ConvertDateTimeOffsetToTimestamp(dto,
                            (TDenginePrecision)colInfo.Field.precision);
                        WriteU64(buffer, colInfo.NextBufferOffset, (ulong)timestamp);
                        break;
                    case DateTime dt:
                        var ts = TDengineConstant.ConvertDateTimeToTimestamp(dt,
                            (TDenginePrecision)colInfo.Field.precision);
                        WriteU64(buffer, colInfo.NextBufferOffset, (ulong)ts);
                        break;
                    case bool boolVal:
                        buffer[colInfo.NextBufferOffset] = boolVal ? (byte)1 : (byte)0;
                        break;
                    case sbyte sbyteVal:
                        buffer[colInfo.NextBufferOffset] = (byte)sbyteVal;
                        break;
                    case byte byteVal:
                        buffer[colInfo.NextBufferOffset] = byteVal;
                        break;
                    case short shortVal:
                        WriteU16(buffer, colInfo.NextBufferOffset, (ushort)shortVal);
                        break;
                    case ushort ushortVal:
                        WriteU16(buffer, colInfo.NextBufferOffset, ushortVal);
                        break;
                    case int intVal:
                        WriteU32(buffer, colInfo.NextBufferOffset, (uint)intVal);
                        break;
                    case uint uintVal:
                        WriteU32(buffer, colInfo.NextBufferOffset, uintVal);
                        break;
                    case long longVal:
                        WriteU64(buffer, colInfo.NextBufferOffset, (ulong)longVal);
                        break;
                    case ulong ulongVal:
                        WriteU64(buffer, colInfo.NextBufferOffset, ulongVal);
                        break;
                    case float floatVal:
#if NETSTANDARD2_1_OR_GREATER ||NET5_0_OR_GREATER||NETCOREAPP2_0_OR_GREATER
                        var floatInt = BitConverter.SingleToInt32Bits(floatVal);
                        WriteU32(buffer, colInfo.NextBufferOffset, (uint)floatInt);
#else
                        var floatBytes = BitConverter.GetBytes(floatVal);
                        Buffer.BlockCopy(floatBytes, 0, buffer, colInfo.NextBufferOffset, 4);
#endif
                        break;
                    case double doubleVal:
                        var doubleInt = BitConverter.DoubleToInt64Bits(doubleVal);
                        WriteU64(buffer, colInfo.NextBufferOffset, (ulong)doubleInt);
                        break;
                    default:
                        throw new NotSupportedException(
                            $"col field type not support: {(TDengineDataType)colInfo.Field.type}");
                }
            }

            colInfo.NextWriteIndex += 1;
            colInfo.NextBufferOffset += colInfo.FixedLength;
        }

        private byte[] GenerateBindBinary()
        {
            var tableCount = _tableInfos.Count;
            var colCount = _isInsert ? _colFields.Length : _fieldsCount;
            var colFields = _isInsert ? _colFields : _queryFieldEs;
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
                            var tagVal = tableInfo.Value.Tags[i];
                            if (tagVal != null && !Convert.IsDBNull(tagVal))
                            {
                                switch (tableInfo.Value.Tags[i])
                                {
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
                                            $"tag field type not support: {(TDengineDataType)_tagFields[i].type}, value: {tagVal}");
                                }
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
                            var colVal = tableInfo.Value.Cols[i][j];
                            if (colVal == null || Convert.IsDBNull(colVal))
                            {
                                continue;
                            }

                            switch (tableInfo.Value.Cols[i][j])
                            {
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
                                        $"col field type not support: {(TDengineDataType)colFields[i].type}, value: {colVal}");
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WriteU32(byte[] buffer, int offset, uint value)
        {
#if NETSTANDARD2_1_OR_GREATER ||NET5_0_OR_GREATER||NETCOREAPP2_1_OR_GREATER
            Span<byte> span = buffer.AsSpan(offset);
#if NET8_0_OR_GREATER
            MemoryMarshal.Write(span, in value);
#else
            MemoryMarshal.Write(span, ref value);
#endif
#else
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
            buffer[offset + 2] = (byte)(value >> 16);
            buffer[offset + 3] = (byte)(value >> 24);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WriteU64(byte[] buffer, int offset, ulong value)
        {
#if NETSTANDARD2_1_OR_GREATER ||NET5_0_OR_GREATER||NETCOREAPP2_1_OR_GREATER
            Span<byte> span = buffer.AsSpan(offset);
#if NET8_0_OR_GREATER
            MemoryMarshal.Write(span, in value);
#else
            MemoryMarshal.Write(span, ref value);
#endif
#else
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
            buffer[offset + 2] = (byte)(value >> 16);
            buffer[offset + 3] = (byte)(value >> 24);
            buffer[offset + 4] = (byte)(value >> 32);
            buffer[offset + 5] = (byte)(value >> 40);
            buffer[offset + 6] = (byte)(value >> 48);
            buffer[offset + 7] = (byte)(value >> 56);
#endif
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WriteU16(byte[] buffer, int offset, ushort value)
        {
#if NETSTANDARD2_1_OR_GREATER ||NET5_0_OR_GREATER||NETCOREAPP2_1_OR_GREATER
            Span<byte> span = buffer.AsSpan(offset);
#if NET8_0_OR_GREATER
            MemoryMarshal.Write(span, in value);
#else
            MemoryMarshal.Write(span, ref value);
#endif
#else
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
#endif
        }

        protected abstract void BindBinaryInternal(byte[] data, out int affectedRows);
    }
}