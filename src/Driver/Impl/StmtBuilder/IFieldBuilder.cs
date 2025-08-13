using System.Collections.Generic;

namespace TDengine.Driver.Impl.StmtBuilder
{
    public interface IFieldBuilder
    {
        TDengineDataType DataType { get; }
        int Length { get; }
        int NullCount { get; }
        bool IsVariable { get; }
        List<int> LengthList { get; }
        void AppendObject(object value);
        void AppendNull();
        int ValueLength();
        int NullLength();
        void Clear();
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
    
    
}