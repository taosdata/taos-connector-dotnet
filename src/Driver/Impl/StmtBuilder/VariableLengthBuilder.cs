using System;
using System.Collections.Generic;
using System.Text;

namespace TDengine.Driver.Impl.StmtBuilder
{
    public class VariableLengthBuilder : IFieldBuilder
    {
        public TDengineDataType DataType { get; }
        public int Length => values.Count;
        public bool IsVariable => true;
        public List<int> LengthList { get; } = new List<int>(1);
        private int _totalValueLength = 0;

        public VariableLengthBuilder(TDengineDataType dataType)
        {
            DataType = dataType;
        }

        private List<byte[]> values { get; set; } = new List<byte[]>(1);
        private List<byte> NullMem { get; set; } = new List<byte>();
        public int NullCount { get; private set; }

        public void AppendObject(object value)
        {
            switch (value)
            {
                case string s:
                    AppendString(s);
                    break;
                case byte[] b:
                    AppendBytes(b);
                    break;
                default:
                    throw new ArgumentException($"Unsupported type: {value.GetType()}");
            }
        }

        public void AppendString(string value)
        {
            var bs = Encoding.UTF8.GetBytes(value);
            AppendBytes(bs);
        }

        public void AppendBytes(byte[] value)
        {
            values.Add(value);
            LengthList.Add(value.Length);
            _totalValueLength += value.Length;
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

            values.Add(null);
            LengthList.Add(0);
            NullMem.Add(1);
            NullCount += 1;
        }

        public int ValueLength()
        {
            return _totalValueLength;
        }

        public int NullLength()
        {
            return Length;
        }

        public void Clear()
        {
            values.Clear();
            LengthList.Clear();
            NullMem.Clear();
            NullCount = 0;
            _totalValueLength = 0;
        }
    }
}