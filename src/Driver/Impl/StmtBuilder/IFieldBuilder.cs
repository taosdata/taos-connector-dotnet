namespace TDengine.Driver.Impl.StmtBuilder
{
    public interface IFieldBuilder
    {
        TDengineDataType DataType { get; }
        int Count { get; }
        void AppendNull();
        void Clear();
        Stmt2BindColInfo ToStmt2BindColInfo();
        Stmt2BindColInfo AddToStmt2BindColInfo(Stmt2BindColInfo source);
        void Remove(int count);
    }

    public sealed class I8Builder : FixedLengthBuilder<sbyte>
    {
        public I8Builder(TDengineDataType dataType) : base(dataType)
        {
        }
    }

    public sealed class I16Builder : FixedLengthBuilder<short>
    {
        public I16Builder(TDengineDataType dataType) : base(dataType)
        {
        }
    }

    public sealed class I32Builder : FixedLengthBuilder<int>
    {
        public I32Builder(TDengineDataType dataType) : base(dataType)
        {
        }
    }

    public sealed class I64Builder : FixedLengthBuilder<long>
    {
        public I64Builder(TDengineDataType dataType) : base(dataType)
        {
        }
    }

    public sealed class U8Builder : FixedLengthBuilder<byte>
    {
        public U8Builder(TDengineDataType dataType) : base(dataType)
        {
        }
    }

    public sealed class U16Builder : FixedLengthBuilder<ushort>
    {
        public U16Builder(TDengineDataType dataType) : base(dataType)
        {
        }
    }

    public sealed class U32Builder : FixedLengthBuilder<uint>
    {
        public U32Builder(TDengineDataType dataType) : base(dataType)
        {
        }
    }

    public sealed class U64Builder : FixedLengthBuilder<ulong>
    {
        public U64Builder(TDengineDataType dataType) : base(dataType)
        {
        }
    }

    public sealed class F32Builder : FixedLengthBuilder<float>
    {
        public F32Builder(TDengineDataType dataType) : base(dataType)
        {
        }
    }

    public sealed class F64Builder : FixedLengthBuilder<double>
    {
        public F64Builder(TDengineDataType dataType) : base(dataType)
        {
        }
    }

    public struct Stmt2BindTableInfo
    {
        public string TableName; // table name
        public Stmt2BindColInfo[] Cols; // col info
        public Stmt2BindColInfo[] Tags; // tag info
    }

    public struct Stmt2BindColInfo
    {
        public uint TotalLength; // current Info total length, includes TotalLength field length
        public int DataType; // data type, see TDengineDataType
        public int Num; // how many rows of data, 1 for single row, >1 for multi rows
        public byte[] IsNull; // Num * 1, each row data is null or not, Num elements

        public byte
            HaveLength; // 1, whether it has length, 0 for no, 1 for yes, when data type is variable length (binary, nchar, json, varbinary, varchar) must have length

        public int[] Length; // each row data length, Num elements, when HaveLength is 0, this field is not used
        public uint BufferLength; // Buffer length, the length of the data in Buffer
        public byte[] Buffer; // bound data, the actual data buffer, the length is BufferLength
    }
}