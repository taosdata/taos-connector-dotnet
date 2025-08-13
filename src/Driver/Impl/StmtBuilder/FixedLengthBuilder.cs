using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.ComTypes;

namespace TDengine.Driver.Impl.StmtBuilder
{
    public class FixedLengthBuilder<T> : IFieldBuilder where T : struct
    {
        public TDengineDataType DataType { get; }
        private readonly int _size = Marshal.SizeOf(typeof(T));
        public int Length => Mem.Count;
        public List<T> Mem { get; } = new List<T>(1);
        public List<byte> NullMem { get; private set; } = new List<byte>();

        public List<int> LengthList => null;
        public int NullCount { get; private set; }

        public bool IsVariable => false;

        public FixedLengthBuilder(TDengineDataType dataType)
        {
            DataType = dataType;
        }

        public void AppendObject(object value)
        {
            if (value is T tValue)
            {
                Append(tValue);
            }
            else
            {
                throw new ArgumentException($"Unsupported type: {value.GetType()}, expected {typeof(T)}");
            }
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

        public int ValueLength()
        {
            return Length * _size;
        }

        public int NullLength()
        {
            return Length;
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

        public void CopyValueToPtr(IntPtr ptr)
        {
            var array = Mem.ToArray();
            var byteArray = new byte[ValueLength()];
            Buffer.BlockCopy(array, 0, byteArray, 0, byteArray.Length);
            Marshal.Copy(byteArray, 0, ptr, byteArray.Length);
        }
    }
}