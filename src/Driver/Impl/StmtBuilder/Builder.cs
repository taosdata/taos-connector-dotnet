using System;

namespace TDengine.Driver.Impl.StmtBuilder
{
    public static class Builder
    {
        private static readonly Func<TDengineDataType, IFieldBuilder>[] _builderCreators =
            {
                null, //TSDB_DATA_TYPE_NULL
                dataType => new I8Builder(dataType), //TSDB_DATA_TYPE_BOOL
                dataType => new I8Builder(dataType), //TSDB_DATA_TYPE_TINYINT
                dataType => new I16Builder(dataType), //TSDB_DATA_TYPE_SMALLINT 
                dataType => new I32Builder(dataType), //TSDB_DATA_TYPE_INT
                dataType => new I64Builder(dataType), //TSDB_DATA_TYPE_BIGINT
                dataType => new F32Builder(dataType), //TSDB_DATA_TYPE_FLOAT,
                dataType => new F64Builder(dataType), //TSDB_DATA_TYPE_DOUBLE,
                dataType => new VariableLengthBuilder(dataType), //TSDB_DATA_TYPE_BINARY,
                dataType => new I64Builder(dataType), //TSDB_DATA_TYPE_TIMESTAMP,
                dataType => new VariableLengthBuilder(dataType), //TSDB_DATA_TYPE_NCHAR,
                dataType => new U8Builder(dataType), //TSDB_DATA_TYPE_UTINYINT,
                dataType => new U16Builder(dataType), //TSDB_DATA_TYPE_USMALLINT,
                dataType => new U32Builder(dataType), //TSDB_DATA_TYPE_UINT,
                dataType => new U64Builder(dataType), //TSDB_DATA_TYPE_UBIGINT,
                dataType => new VariableLengthBuilder(dataType), //TSDB_DATA_TYPE_JSONTAG,
                dataType => new VariableLengthBuilder(dataType), //TSDB_DATA_TYPE_VARBINARY,
                null, //TSDB_DATA_TYPE_DECIMAL
                null, //TSDB_DATA_TYPE_BLOB
                null, //TSDB_DATA_TYPE_MEDIUMBLOB
                dataType => new VariableLengthBuilder(dataType), //TSDB_DATA_TYPE_GEOMETRY
                null, //TSDB_DATA_TYPE_DECIMAL64
            };

        public static IFieldBuilder CreateBuilder(TDengineDataType dataType)
        {
            if (dataType < TDengineDataType.TSDB_DATA_TYPE_NULL || dataType >= TDengineDataType.TSDB_DATA_TYPE_MAX)
            {
                throw new ArgumentOutOfRangeException(nameof(dataType), "Invalid TDengine data type.");
            }

            var creator = _builderCreators[(int)dataType];
            if (creator != null)
            {
                return creator(dataType);
            }

            throw new ArgumentException($"Unsupported data type: {TDengineConstant.GetFieldTypeName((sbyte)dataType)}");
        }
    }
}