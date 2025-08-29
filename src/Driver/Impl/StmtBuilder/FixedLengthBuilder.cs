using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace TDengine.Driver.Impl.StmtBuilder
{
    public class FixedLengthBuilder<T> : IFieldBuilder where T : struct
    {
        public TDengineDataType DataType { get; }
        private readonly int _size = Marshal.SizeOf(typeof(T));
        public int Count => Mem.Count;
        private List<T> Mem { get; } = new List<T>(1);
        private List<byte> NullMem { get; set; } = new List<byte>();

        private int _nullCount;
        private BufferPool _pool = null;

        public FixedLengthBuilder(TDengineDataType dataType)
        {
            DataType = dataType;
            _pool = NoPool.Instance;
        }

        public void SetBufferPool(BufferPool pool)
        {
            _pool = pool;
        }

        public void Append(T value)
        {
            Mem.Add(value);
            if (_nullCount != 0)
            {
                NullMem.Add(0);
            }
        }

        public void AppendNull()
        {
            if (_nullCount == 0)
            {
                NullMem = new List<byte>(new byte[Count]);
            }

            Mem.Add(default);
            NullMem.Add(1);
            _nullCount += 1;
        }

        private int ValueLength()
        {
            return Count * _size;
        }

        public void Clear()
        {
            Mem.Clear();
            NullMem.Clear();
            _nullCount = 0;
        }

        public void Remove(int count)
        {
            if (count < 0 || count > Mem.Count)
            {
                throw new ArgumentOutOfRangeException(nameof(count),
                    "Count must be non-negative and less than or equal to the number of elements in Mem.");
            }

            var totalCount = Mem.Count;
            var removeIndex = totalCount - count;
            Mem.RemoveRange(removeIndex, count);
            if (_nullCount > 0)
            {
                for (var i = removeIndex; i < totalCount; i++)
                {
                    if (NullMem[i] == 1)
                    {
                        _nullCount--;
                    }
                }

                if (_nullCount == 0)
                {
                    NullMem.Clear();
                }
                else
                {
                    NullMem.RemoveRange(removeIndex, count);
                }
            }
        }

        public Stmt2BindColInfo ToStmt2BindColInfo()
        {
            byte[] isNull = null;
            if (_nullCount > 0)
            {
                isNull = NullMem.ToArray();
            }

            var valueBuffer = ValueBuffer(out var bufferLength);
            uint totalLength = 4 + // TotalLength field length
                               4 + // DataType field length
                               4 + // Num field length
                               (uint)(Count) + // IsNull field length
                               1 + // HaveLength field length
                               4 + // BufferLength field length
                               (uint)bufferLength; // Buffer field length
            return new Stmt2BindColInfo
            {
                TotalLength = totalLength,
                DataType = (int)DataType,
                Num = Count,
                IsNull = isNull,
                HaveLength = 0, // Fixed length does not have length
                Length = null, // Fixed length does not have length
                BufferLength = (uint)bufferLength,
                Buffer = valueBuffer
            };
        }

        public void ToStmt2BindColInfo2(ref Stmt2BindColInfo info)
        {
            byte[] isNull = null;
            if (_nullCount > 0)
            {
                isNull = NullMem.ToArray();
            }

            var valueBuffer = ValueBuffer(out var bufferLength);
            uint totalLength = 4 + // TotalLength field length
                               4 + // DataType field length
                               4 + // Num field length
                               (uint)(Count) + // IsNull field length
                               1 + // HaveLength field length
                               4 + // BufferLength field length
                               (uint)bufferLength; // Buffer field length


            info.TotalLength = totalLength;
            info.DataType = (int)DataType;
            info.Num = Count;
            info.IsNull = isNull;
            info.HaveLength = 0; // Fixed length does not have length
            info.Length = null; // Fixed length does not have length
            info.BufferLength = (uint)bufferLength;
            info.Buffer = valueBuffer;
        }

        private byte[] ValueBuffer(out int length)
        {
            length = ValueLength();
            var bytes = _pool.GetBytes(length);
#if NET5_0_OR_GREATER
            Span<T> listSpan = CollectionsMarshal.AsSpan(Mem);
            MemoryMarshal.AsBytes(listSpan).CopyTo(bytes);
            return bytes;
#else
            if (typeof(T) == typeof(byte))
            {
                var byteList = Mem as List<byte>;
                byteList?.CopyTo(bytes);
                return bytes;
            }
            var array = Mem.ToArray();
            Buffer.BlockCopy(array, 0, bytes, 0, length);
            return bytes;
#endif
        }

        public Stmt2BindColInfo AddToStmt2BindColInfo(Stmt2BindColInfo source)
        {
            if (source.DataType != (int)DataType)
            {
                throw new ArgumentException($"Data type mismatch: expected {DataType}, got {source.DataType}");
            }

            var target = new Stmt2BindColInfo
            {
                DataType = source.DataType,
                HaveLength = 0, // Fixed length does not have length
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
                // var valueBuffer = ValueBuffer(out var bufferLength);
                target.BufferLength = source.BufferLength + (uint)bufferLength;
                target.Buffer = _pool.GetBytes((int)target.BufferLength);
                Buffer.BlockCopy(source.Buffer, 0, target.Buffer, 0, (int)source.BufferLength);
                Buffer.BlockCopy(valueBuffer, 0, target.Buffer, (int)source.BufferLength, bufferLength);
                // TotalLength
                target.TotalLength = source.TotalLength + (uint)Count // length of IsNull
                                                        + (uint)bufferLength; // length of Buffer
                return target;
            }
            finally
            {
                _pool.ReturnBytes(valueBuffer);
            }
        }
    }
}