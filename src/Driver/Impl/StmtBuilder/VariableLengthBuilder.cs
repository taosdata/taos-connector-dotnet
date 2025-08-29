using System;
using System.Collections.Generic;
using System.Text;

namespace TDengine.Driver.Impl.StmtBuilder
{
    public class VariableLengthBuilder : IFieldBuilder
    {
        private BufferPool _pool;
        public TDengineDataType DataType { get; }
        public int Count { get; private set; }
        private List<int> LengthList { get; } = new List<int>(1);
        private List<byte> Values { get; set; } = new List<byte>(0);
        private List<byte> NullMem { get; set; } = new List<byte>();
        private int NullCount { get; set; }

        public VariableLengthBuilder(TDengineDataType dataType)
        {
            DataType = dataType;
            _pool = NoPool.Instance;
        }

        public void SetBufferPool(BufferPool pool)
        {
            _pool = pool;
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
            if (NullCount > 0)
            {
                NullMem.Add(0);
            }

            Count += 1;
        }

        public void AppendNull()
        {
            if (NullCount == 0)
            {
                NullMem = new List<byte>(new byte[Count]);
            }

            LengthList.Add(0);
            NullMem.Add(1);
            NullCount += 1;
            Count += 1;
        }

        public void Clear()
        {
            Values.Clear();
            LengthList.Clear();
            NullMem.Clear();
            NullCount = 0;
            Count = 0;
        }

        private byte[] ValueBuffer(out int length)
        {
            length = Values.Count;
            var bytes = _pool.GetBytes(length);
            Values.CopyTo(0, bytes, 0, length);
            return bytes;
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

            var valueBuffer = ValueBuffer(out var bufferLength);
            uint totalLength = 4 + // TotalLength field length
                               4 + // DataType field length
                               4 + // Num field length
                               (uint)(Count) + // IsNull field length
                               1 + // HaveLength field length
                               (uint)(Count * 4) + // Length field length, each length is 4 bytes
                               4 + // BufferLength field length
                               (uint)bufferLength; // Buffer field length
            return new Stmt2BindColInfo
            {
                TotalLength = totalLength,
                DataType = (int)DataType,
                Num = Count,
                IsNull = isNull,
                HaveLength = 1,
                Length = DataLength(),
                BufferLength = (uint)bufferLength,
                Buffer = valueBuffer
            };
        }

        public void ToStmt2BindColInfo2(ref Stmt2BindColInfo info)
        {
            byte[] isNull = null;
            if (NullCount > 0)
            {
                isNull = NullMem.ToArray();
            }

            var valueBuffer = ValueBuffer(out var bufferLength);
            uint totalLength = 4 + // TotalLength field length
                               4 + // DataType field length
                               4 + // Num field length
                               (uint)(Count) + // IsNull field length
                               1 + // HaveLength field length
                               (uint)(Count * 4) + // Length field length, each length is 4 bytes
                               4 + // BufferLength field length
                               (uint)bufferLength; // Buffer field length

            info.TotalLength = totalLength;
            info.DataType = (int)DataType;
            info.Num = Count;
            info.IsNull = isNull;
            info.HaveLength = 1;
            info.Length = DataLength();
            info.BufferLength = (uint)bufferLength;
            info.Buffer = valueBuffer;
        }

        public Stmt2BindColInfo AddToStmt2BindColInfo(Stmt2BindColInfo source)
        {
            if (source.DataType != (int)DataType)
            {
                throw new ArgumentException($"Data type mismatch: expected {DataType}, got {source.DataType}");
            }

            var target = new Stmt2BindColInfo()
            {
                DataType = source.DataType,
                HaveLength = 1,
            };

            // IsNull
            if (source.IsNull != null || NullMem.Count > 0)
            {
                target.IsNull = new byte[source.Num + Count];
                if (source.IsNull != null)
                {
                    Array.Copy(source.IsNull, 0, target.IsNull, 0, source.Num);
                }

                if (NullMem.Count > 0)
                {
                    Array.Copy(NullMem.ToArray(), 0, target.IsNull, source.Num, Count);
                }
            }

            target.Num = source.Num + Count;
            var valueBuffer = ValueBuffer(out var bufferLength);
            try
            {
                target.BufferLength = source.BufferLength + (uint)bufferLength;
                target.Buffer = new byte[target.BufferLength];
                Buffer.BlockCopy(source.Buffer, 0, target.Buffer, 0, (int)source.BufferLength);
                Buffer.BlockCopy(valueBuffer, 0, target.Buffer, (int)source.BufferLength, bufferLength);

                // Length
                var dataLength = DataLength();
                target.Length = new int[source.Length.Length + dataLength.Length];
                Buffer.BlockCopy(source.Length, 0, target.Length, 0, source.Length.Length * 4);
                Buffer.BlockCopy(dataLength, 0, target.Length, source.Length.Length * 4, Count * 4);
                // TotalLength
                target.TotalLength = source.TotalLength + (uint)Count // length of IsNull
                                                        + (uint)bufferLength + // length of Buffer
                                                        +(uint)(Count * 4); // length of Length
                return target;
            }
            finally
            {
                _pool.ReturnBytes(valueBuffer);
            }
        }

        public void Remove(int count)
        {
            if (count < 0 || count > Count)
            {
                throw new ArgumentOutOfRangeException(nameof(count),
                    "Count must be non-negative and less than or equal to the number of elements in Mem.");
            }

            var removeByteCount = 0;
            var removeIndex = Count - count;
            for (var i = removeIndex; i < Count; i++)
            {
                removeByteCount += LengthList[i];
            }

            if (removeByteCount > 0)
            {
                Values.RemoveRange(Values.Count - removeByteCount, removeByteCount);
            }

            if (NullCount > 0)
            {
                for (var i = removeIndex; i < Count; i++)
                {
                    if (NullMem[i] == 1)
                    {
                        NullCount--;
                    }
                }

                if (NullCount == 0)
                {
                    NullMem.Clear();
                }
                else
                {
                    NullMem.RemoveRange(removeIndex, count);
                }
            }

            Count -= count;
            LengthList.RemoveRange(removeIndex, count);
        }
    }
}