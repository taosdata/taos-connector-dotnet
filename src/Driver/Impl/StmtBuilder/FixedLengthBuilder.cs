using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace TDengine.Driver.Impl.StmtBuilder
{
    public class FixedLengthBuilder<T> : IFieldBuilder where T : struct
    {
        public TDengineDataType DataType { get; }
        private readonly int _size = Marshal.SizeOf(typeof(T));
        public int Length => Mem.Count;
        private List<T> Mem { get; } = new List<T>(1);
        private List<byte> NullMem { get; set; } = new List<byte>();

        public List<int> LengthList => null;
        public int NullCount { get; private set; }

        public bool IsVariable => false;

        public FixedLengthBuilder(TDengineDataType dataType)
        {
            DataType = dataType;
        }

        public void Append(T value)
        {
            Mem.Add(value);
            if (NullCount != 0)
            {
                NullMem.Add(0);
            }
        }

        public void AppendNull()
        {
            if (NullCount == 0)
            {
                NullMem = new List<byte>(new byte[Length]);
            }

            Mem.Add(default);
            NullMem.Add(1);
            NullCount += 1;
        }

        private int ValueLength()
        {
            return Length * _size;
        }

        public void Clear()
        {
            Mem.Clear();
            NullMem.Clear();
            NullCount = 0;
        }

        public void Remove(int count)
        {
            if (count < 0 || count > Mem.Count)
            {
                throw new ArgumentOutOfRangeException(nameof(count),
                    "Count must be non-negative and less than or equal to the number of elements in Mem.");
            }

            Mem.RemoveRange(Mem.Count - count, count);
            if (NullCount > 0)
            {
                for (int i = Mem.Count - count; i < Mem.Count; i++)
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
                    NullMem.RemoveRange(NullMem.Count - count, count);
                }
            }

            NullCount -= count;
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
                               4 + // BufferLength field length
                               (uint)valueBuffer.Length; // Buffer field length
            return new Stmt2BindColInfo
            {
                TotalLength = totalLength,
                DataType = (int)DataType,
                Num = Length,
                IsNull = isNull,
                HaveLength = 0, // Fixed length does not have length
                Length = null, // Fixed length does not have length
                BufferLength = (uint)valueBuffer.Length,
                Buffer = valueBuffer
            };
        }

        private byte[] ValueBuffer()
        {
            // todo reduce memory copy
            var array = Mem.ToArray();
            var byteArray = new byte[ValueLength()];
            Buffer.BlockCopy(array, 0, byteArray, 0, byteArray.Length);
            return byteArray;
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
            // TotalLength
            bindColInfo.TotalLength += (uint)Length // length of IsNull
                                       + (uint)valueBuffer.Length; // length of Buffer
            return bindColInfo;
        }
    }
}