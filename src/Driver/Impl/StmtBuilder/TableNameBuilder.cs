using System.Collections.Generic;
using System.Text;

namespace TDengine.Driver.Impl.StmtBuilder
{
    public class TableNameBuilder
    {
        public int Length { get; private set; }
        public int TotalBufferLen => Values.Count;
        private List<short> LengthList { get; } = new List<short>();
        private List<byte> Values { get; } = new List<byte>();
        public List<string> TableNames { get; } = new List<string>();

        public byte[] GetBytes()
        {
            return Values.ToArray();
        }

        public short[] GetLengths()
        {
            return LengthList.ToArray();
        }
        public void Add(string tableName)
        {
            var bs = Encoding.UTF8.GetBytes(tableName);
            var len = bs.Length + 1;
            LengthList.Add(checked((short)len));
            Values.AddRange(bs);
            Values.Add(0); // Null-terminator for the string
            Length += 1;
            TableNames.Add(tableName);
        }
        public void Clear()
        {
            LengthList.Clear();
            Values.Clear();
            TableNames.Clear();
            Length = 0;
        }
    }
}