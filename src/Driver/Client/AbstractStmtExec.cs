using System;
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

        private byte[] GenerateBindBinary()
        {
            var tableCount = _tableInfos.Count;
            const uint fixedHeaderLen = 28;
            var tableNameLengthLen = (uint)0;
            var tableNameBufferLen = (uint)0;
            var tagsDataLengthLen = (uint)0;
            var tagsBufferLen = (uint)0;
            var colsDataLengthLen = (uint)(tableCount * 4);
            var colsBufferLen = (uint)0;
            // table name
            if (_needTableName)
            {
                tableNameLengthLen = (uint)(_tableNameBuilder.Count * 2);
                tableNameBufferLen = (uint)_tableNameBuilder.TotalBufferLen;
            }

            if (NeedTags)
            {
                tagsDataLengthLen = (uint)(tableCount * 4);
            }

            foreach (var tableInfo in _tableInfos.Values)
            {
                // tags
                if (NeedTags)
                {
                    for (int i = 0; i < tableInfo.TagsLength; i++)
                    {
                        tagsBufferLen += tableInfo.Tags[i].TotalLength;
                    }
                }

                // cols
                for (int i = 0; i < tableInfo.ColsLength; i++)
                {
                    colsBufferLen += tableInfo.Cols[i].TotalLength;
                }
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
            WriteU32(buffer, _binaryHeaderLength + 8, NeedTags ? (uint)_tagBuilders.Length : 0); // TagCount
            WriteU32(buffer, _binaryHeaderLength + 12, (uint)_colBuilders.Length); // ColCount
            WriteU32(buffer, _binaryHeaderLength + 16, _needTableName ? fixedHeaderLen : 0); // TableNamesOffset
            WriteU32(buffer, _binaryHeaderLength + 20, NeedTags ? tagsOffset : 0); // TagsOffset
            WriteU32(buffer, _binaryHeaderLength + 24, colsOffset); // ColsOffset
            var tableNameLengthOffset = _binaryHeaderLength + (int)tableNameOffset;
            var tableNameBufferOffset = tableNameLengthOffset + (int)tableNameLengthLen;
            var tagsLengthOffset = _binaryHeaderLength + (int)tagsOffset;
            var tagsBufferOffset = tagsLengthOffset + (int)tagsDataLengthLen;
            var colsLengthOffset = _binaryHeaderLength + (int)colsOffset;
            var colsBufferOffset = colsLengthOffset + (int)colsDataLengthLen;
            if (_needTableName)
            {
                Buffer.BlockCopy(_tableNameBuilder.GetLengths(), 0, buffer, tableNameLengthOffset,
                    (int)tableNameLengthLen);
                _tableNameBuilder.CopyValueTo(buffer,tableNameBufferOffset, (int)tableNameBufferLen);
                // Buffer.BlockCopy(_tableNameBuilder.GetBytes(), 0, buffer, tableNameBufferOffset,
                //     (int)tableNameBufferLen);
                for (var i = 0; i < _tableNameBuilder.Count; i++)
                {
                    var tableName = _tableNameBuilder.TableNames[i];
                    var tableInfo = _tableInfos[tableName];
                    if (NeedTags)
                    {
                        tagsBufferOffset = SetBindData(buffer, tagsBufferOffset, tableInfo.Tags,tableInfo.TagsLength,
                            out var tableTagLength);
                        WriteU32(buffer, tagsLengthOffset, (uint)tableTagLength); // TagsDataLength
                        tagsLengthOffset += 4;
                    }

                    colsBufferOffset = SetBindData(buffer, colsBufferOffset, tableInfo.Cols,tableInfo.ColsLength,
                        out var tableColLength);
                    WriteU32(buffer, colsLengthOffset, (uint)tableColLength); // ColData
                    colsLengthOffset += 4;
                }

                _tableNameBuilder.Clear();
            }
            else
            {
                var tableInfo = _tableInfos[string.Empty];
                if (NeedTags)
                {
                    SetBindData(buffer, tagsBufferOffset, tableInfo.Tags,tableInfo.TagsLength, out var tableTagLength);
                    WriteU32(buffer, tagsLengthOffset, (uint)tableTagLength); // TagsDataLength
                }

                SetBindData(buffer, colsBufferOffset, tableInfo.Cols,tableInfo.ColsLength, out var tableColLength);
                WriteU32(buffer, colsLengthOffset, (uint)tableColLength); // ColData
            }

            return buffer;
        }

        private int SetBindData(byte[] buffer, int offset, Stmt2BindColInfo[] fields,int fieldsCount, out int totalLen)
        {
            totalLen = 0;
            for (var i = 0; i < fieldsCount; i++)
            {
                totalLen += (int)fields[i].TotalLength;
                WriteU32(buffer, offset, fields[i].TotalLength); // TotalLength
                offset += 4;
                WriteU32(buffer, offset, (uint)fields[i].DataType); // DataType
                offset += 4;
                WriteU32(buffer, offset, (uint)fields[i].Num); // Num
                offset += 4;
                if (fields[i].IsNull != null)
                {
                    Buffer.BlockCopy(fields[i].IsNull, 0, buffer, offset, fields[i].Num); // IsNull
                }

                // If IsNull is null, it means no null values, fill with 0, the buffer is already initialized with 0
                offset += fields[i].Num;
                buffer[offset] = fields[i].HaveLength; // HaveLength
                offset += 1;
                if (fields[i].HaveLength == 1 && fields[i].Length != null)
                {
                    Buffer.BlockCopy(fields[i].Length, 0, buffer, offset, fields[i].Num * 4); // Length
                    offset += fields[i].Num * 4;
                }

                WriteU32(buffer, offset, fields[i].BufferLength); // BufferLength
                offset += 4;
                if (fields[i].Buffer != null)
                {
                    Buffer.BlockCopy(fields[i].Buffer, 0, buffer, offset, (int)fields[i].BufferLength); // Buffer
                    offset += (int)fields[i].BufferLength;
                    _bufferPool.ReturnBytes(fields[i].Buffer);
                }
            }

            return offset;
        }

        // little-endian write int32 to buffer
        private void WriteU32(byte[] buffer, int offset, uint value)
        {
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
            buffer[offset + 2] = (byte)(value >> 16);
            buffer[offset + 3] = (byte)(value >> 24);
        }

        protected abstract void BindBinaryInternal(byte[] data, out int affectedRows);
    }
}