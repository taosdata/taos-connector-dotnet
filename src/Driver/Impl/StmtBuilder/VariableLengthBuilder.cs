using System;
using System.Collections.Generic;
using System.Text;

namespace TDengine.Driver.Impl.StmtBuilder
{
    public class VariableLengthBuilder : IFieldBuilder
    {
        public TDengineDataType DataType { get; }
        public int Length { get; private set; }
        private List<int> LengthList { get; } = new List<int>(1);
        private int _totalValueLength = 0;
        private List<byte> Values { get; set; } = new List<byte>(0);
        private List<byte> NullMem { get; set; } = new List<byte>();
        private int NullCount { get; set; }

        public VariableLengthBuilder(TDengineDataType dataType)
        {
            DataType = dataType;
        }

        public void AppendString(string value)
        {
            var bs = Encoding.UTF8.GetBytes(value);
            AppendBytes(bs);
        }

        public void AppendBytes(byte[] value)
        {
            Values.AddRange(value);
            LengthList.Add(value.Length);
            _totalValueLength += value.Length;
            if (NullCount != 0)
            {
                NullMem.Add(0);
            }

            Length += 1;
        }

        public void AppendNull()
        {
            if (NullCount == 0)
            {
                NullMem = new List<byte>(new byte[Length]);
            }

            LengthList.Add(0);
            NullMem.Add(1);
            NullCount += 1;
            Length += 1;
        }

        public void Clear()
        {
            Values.Clear();
            LengthList.Clear();
            NullMem.Clear();
            NullCount = 0;
            _totalValueLength = 0;
            Length = 0;
        }

        private byte[] ValueBuffer()
        {
            return Values.ToArray();
        }

        private int[] DataLength()
        {
            return LengthList.ToArray();
        }

        public Stmt2BindColInfo ToStmt2BindColInfo()
        {
            byte[] isNull = null;
            if (NullCount > 0)
            {
                isNull = NullMem.ToArray();
            }

            var valueBuffer = ValueBuffer();
            uint totalLength = 4 + // TotalLength field length
                               4 + // DataType field length
                               4 + // Num field length
                               (uint)(Length) + // IsNull field length
                               1 + // HaveLength field length
                               (uint)(Length * 4) + // Length field length, each length is 4 bytes
                               4 + // BufferLength field length
                               (uint)valueBuffer.Length; // Buffer field length
            return new Stmt2BindColInfo
            {
                TotalLength = totalLength,
                DataType = (int)DataType,
                Num = Length,
                IsNull = isNull,
                HaveLength = 1,
                Length = DataLength(),
                BufferLength = (uint)valueBuffer.Length,
                Buffer = valueBuffer
            };
        }

        public Stmt2BindColInfo AddToStmt2BindColInfo(Stmt2BindColInfo bindColInfo)
        {
            if (bindColInfo.DataType != (int)DataType)
            {
                throw new ArgumentException($"Data type mismatch: expected {DataType}, got {bindColInfo.DataType}");
            }

            // IsNull
            if (bindColInfo.IsNull != null || NullMem.Count > 0)
            {
                Array.Resize(ref bindColInfo.IsNull, bindColInfo.Num + Length);
                if (NullMem.Count > 0)
                {
                    Array.Copy(NullMem.ToArray(), 0, bindColInfo.IsNull, bindColInfo.Num, Length);
                }
            }

            bindColInfo.Num += Length;
            // Buffer
            var valueBuffer = ValueBuffer();
            var newBufferLength = bindColInfo.BufferLength + (uint)valueBuffer.Length;
            Array.Resize(ref bindColInfo.Buffer, (int)newBufferLength);
            Buffer.BlockCopy(valueBuffer, 0, bindColInfo.Buffer, bindColInfo.Buffer.Length, valueBuffer.Length);
            bindColInfo.BufferLength += newBufferLength;

            // Length
            var dataLength = DataLength();
            Array.Resize(ref bindColInfo.Length, bindColInfo.Num);
            Buffer.BlockCopy(dataLength, 0, bindColInfo.Length, bindColInfo.Num * 4, Length * 4);

            // TotalLength
            bindColInfo.TotalLength += (uint)Length // length of IsNull
                                       + (uint)valueBuffer.Length + // length of Buffer
                                       +(uint)(Length * 4); // length of Length
            return bindColInfo;
        }

        public void Remove(int count)
        {
            if (count < 0 || count > Length)
            {
                throw new ArgumentOutOfRangeException(nameof(count),
                    "Count must be non-negative and less than or equal to the number of elements in Mem.");
            }

            var removeByteCount = 0;
            var removeIndex = Length - count;
            for (int i = removeIndex; i < Length; i++)
            {
                removeByteCount += LengthList[i];
            }

            if (removeByteCount > 0)
            {
                Values.RemoveRange(Length - removeByteCount, removeByteCount);
                _totalValueLength -= removeByteCount;
            }
            if (NullCount > 0)
            {
                for (int i = removeIndex; i < Length; i++)
                {
                    if (NullMem[i] == 1)
                    {
                        NullCount--;
                    }
                }

                if (NullCount == 0)
                {
                    NullMem = null;
                }
                else
                {
                    NullMem.RemoveRange(removeIndex, count);
                }
            }

            NullCount -= count;
            Length -= count;
            LengthList.RemoveRange(removeIndex,count);
        }
    }
}