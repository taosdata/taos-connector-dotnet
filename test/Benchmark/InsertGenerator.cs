using System;
using System.Text;
using TDengineDriver;
namespace Benchmark
{
    internal class InsertGenerator
    {
        string Table { get; set; }
        public long BeginTime { get; set; }
        bool BoolVal { get; set; }
        sbyte TinyIntVal { get; set; }
        short SmallIntVal { get; set; }
        int IntVal { get; set; }
        long BigIntVal { get; set; }
        byte UTinyIntVal { get; set; }
        ushort USmallIntVal { get; set; }
        uint UIntVal { get; set; }
        ulong UBigIntVal { get; set; }
        float FloatVal { get; set; }
        double DoubleVal { get; set; }
        int StringColLength { get; set; } = 20;
        int MaxSqlLength { get; set; } = 5000;
        public InsertGenerator(long begineTime, int maxSqlLength)
        {
            BeginTime = begineTime;
            MaxSqlLength = maxSqlLength;
        }

        public string RandomSQL(string table, int numOfRows)
        {
            StringBuilder sqlBuilder = new StringBuilder();
            Random random = new Random();
            try
            {
                sqlBuilder.Append($"insert into {table} values ");

                for (int i = 0; i < numOfRows; i++)
                {
                    BeginTime += 1;
                    BoolVal = random.Next(0, 1) == 0;
                    TinyIntVal = (sbyte)random.Next(sbyte.MinValue, sbyte.MaxValue);
                    SmallIntVal = (short)random.Next(short.MinValue, short.MaxValue);
                    IntVal = random.Next(int.MinValue, int.MaxValue);
                    BigIntVal = random.NextInt64(long.MinValue, long.MaxValue);
                    UTinyIntVal = (byte)random.Next(byte.MaxValue);
                    USmallIntVal = (ushort)random.Next(ushort.MaxValue);
                    UIntVal = (uint)random.NextInt64(uint.MaxValue);
                    UBigIntVal = (ulong)random.NextInt64(long.MaxValue);
                    FloatVal = random.NextSingle();
                    DoubleVal = random.NextDouble();
                    sqlBuilder.Append('(');
                    sqlBuilder.Append(BeginTime);
                    sqlBuilder.Append(',');
                    sqlBuilder.Append(BoolVal);
                    sqlBuilder.Append(',');
                    sqlBuilder.Append(TinyIntVal);
                    sqlBuilder.Append(',');
                    sqlBuilder.Append(SmallIntVal);
                    sqlBuilder.Append(',');
                    sqlBuilder.Append(IntVal);
                    sqlBuilder.Append(',');
                    sqlBuilder.Append(BigIntVal);
                    sqlBuilder.Append(',');
                    sqlBuilder.Append(UTinyIntVal);
                    sqlBuilder.Append(',');
                    sqlBuilder.Append(USmallIntVal);
                    sqlBuilder.Append(',');
                    sqlBuilder.Append(UIntVal);
                    sqlBuilder.Append(',');
                    sqlBuilder.Append(UBigIntVal);
                    sqlBuilder.Append(',');
                    sqlBuilder.Append(FloatVal);
                    sqlBuilder.Append(',');
                    sqlBuilder.Append(DoubleVal);
                    sqlBuilder.Append(',');
                    sqlBuilder.Append('\'');
                    sqlBuilder.Append(RandomString(StringColLength));
                    sqlBuilder.Append('\'');
                    sqlBuilder.Append(',');
                    sqlBuilder.Append('\'');
                    sqlBuilder.Append(RandomString(StringColLength));
                    sqlBuilder.Append('\'');
                    sqlBuilder.Append(')');
                }

                sqlBuilder.Append(';');
                return sqlBuilder.ToString();
            }
            finally
            {
                sqlBuilder.Clear();
            }
        }

        public string RandomString(int length)
        {
            StringBuilder randomStr = new StringBuilder();
            Random random = new Random();

            for (int i = 0; i < length; i++)
            {
                randomStr.Append(Convert.ToChar(random.Next(97, 122)));
            }
            return randomStr.ToString();
        }
 

        public bool IfTaosQuerySucc(IntPtr res, string sql)
        {
            if (TDengine.ErrorNo(res) == 0)
            {
                return true;
            }
            else
            {
                throw new Exception($"execute {sql} failed,reason {TDengine.Error(res)}, code{TDengine.ErrorNo(res)}");
            }
        }

    }

    public struct RunContext
    {
        public string tableName { get; set; }
        public int numOfRows { get; set; }
        public int numOfTables { get; set; }
        public IntPtr conn { get; set; }
        public RunContext(string name, int rows, int tables, IntPtr connection)
        {
            tableName = name;
            numOfRows = rows;
            numOfTables = tables;
            conn = connection;

        }
        public string ToString()
        {
            return $"tablename:{tableName} numOfRow:{numOfRows} numOfTable:{numOfTables}";
        }

    }
}