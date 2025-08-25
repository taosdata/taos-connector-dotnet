using System;
using TDengine.Driver.Impl.StmtBuilder;

namespace TDengine.Driver.Client
{
    public abstract partial class AbstractStmt
    {
        private static void CacheRowValue(object[] obj, IFieldBuilder[] builders, TaosFieldE[] fields)
        {
            for (var i = 0; i < builders.Length; i++)
            {
                try
                {
                    if (obj[i] == null || Convert.IsDBNull(obj[i]))
                    {
                        builders[i].AppendNull();
                    }
                    else
                    {
                        switch (obj[i])
                        {
                            case bool val:
                                if (builders[i] is I8Builder boolBuilder &&
                                    builders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_BOOL)
                                {
                                    boolBuilder.Append(val ? (sbyte)1 : (sbyte)0);
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type bool to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            case sbyte val:
                                if (builders[i] is I8Builder i8Builder &&
                                    builders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_TINYINT)
                                {
                                    i8Builder.Append(val);
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type sbyte to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            case short val:
                                if (builders[i] is I16Builder i16Builder &&
                                    builders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_SMALLINT)
                                {
                                    i16Builder.Append(val);
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type short to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            case int val:
                                if (builders[i] is I32Builder i32Builder &&
                                    builders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_INT)
                                {
                                    i32Builder.Append(val);
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type short to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            case long val:
                                if (builders[i] is I64Builder i64Builder &&
                                    (builders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_BIGINT ||
                                     builders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP))
                                {
                                    i64Builder.Append(val);
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type long to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            case byte val:
                                if (builders[i] is U8Builder u8Builder &&
                                    builders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_UTINYINT)
                                {
                                    u8Builder.Append(val);
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type byte to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            case ushort val:
                                if (builders[i] is U16Builder u16Builder &&
                                    builders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_USMALLINT)
                                {
                                    u16Builder.Append(val);
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type ushort to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            case uint val:
                                if (builders[i] is U32Builder u32Builder &&
                                    builders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_UINT)
                                {
                                    u32Builder.Append(val);
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type uint to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            case ulong val:
                                if (builders[i] is U64Builder u64Builder &&
                                    builders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_UBIGINT)
                                {
                                    u64Builder.Append(val);
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type ulong to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            case float val:
                                if (builders[i] is F32Builder floatBuilder &&
                                    builders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_FLOAT)
                                {
                                    floatBuilder.Append(val);
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type float to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            case double val:
                                if (builders[i] is F64Builder doubleBuilder &&
                                    builders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_DOUBLE)
                                {
                                    doubleBuilder.Append(val);
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type double to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            case DateTime val:
                                if (builders[i] is I64Builder dateTimeBuilder && builders[i].DataType ==
                                    TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
                                {
                                    dateTimeBuilder.Append(TDengineConstant.ConvertDateTimeToTimestamp(val,
                                        (TDenginePrecision)fields[i].precision));
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type DateTime to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            case DateTimeOffset val:
                                if (builders[i] is I64Builder dateTimeOffsetBuilder && builders[i].DataType ==
                                    TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
                                {
                                    dateTimeOffsetBuilder.Append(TDengineConstant.ConvertDateTimeOffsetToTimestamp(val,
                                        (TDenginePrecision)fields[i].precision));
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"Bind type DateTimeOffset not supported for field {fields[i].name}");
                                }

                                break;
                            case byte[] val:
                                if (builders[i] is VariableLengthBuilder bytesBuilder && (
                                        fields[i].type == (int)TDengineDataType.TSDB_DATA_TYPE_BINARY ||
                                        fields[i].type == (int)TDengineDataType.TSDB_DATA_TYPE_JSONTAG ||
                                        fields[i].type == (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY ||
                                        fields[i].type == (int)TDengineDataType.TSDB_DATA_TYPE_GEOMETRY
                                    ))
                                {
                                    bytesBuilder.AppendBytes(val);
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type byte[] to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            case string val:
                                if (builders[i] is VariableLengthBuilder stringBuilder && (
                                        fields[i].type == (int)TDengineDataType.TSDB_DATA_TYPE_BINARY ||
                                        fields[i].type == (int)TDengineDataType.TSDB_DATA_TYPE_JSONTAG ||
                                        fields[i].type == (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY ||
                                        fields[i].type == (int)TDengineDataType.TSDB_DATA_TYPE_NCHAR
                                    ))
                                {
                                    stringBuilder.AppendString(val);
                                }
                                else
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type string to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                break;
                            default:
                                throw new ArgumentException(
                                    $"BindIndex: {i}, field name: {fields[i].name}, stmt bind param type not supported: {obj[i].GetType()}");
                        }
                    }
                }
                catch
                {
                    for (var j = 0; j < i; j++)
                    {
                        builders[j].Remove(1);
                    }

                    throw;
                }
            }
        }

        private void CacheQueryRow(object[] row)
        {
            if (_colBuilders != null)
            {
                throw new InvalidOperationException(
                    "Query parameters have already been set.");
            }

            _colBuilders = new IFieldBuilder[row.Length];
            try
            {
                for (var i = 0; i < row.Length; i++)
                {
                    if (row[i] == null || Convert.IsDBNull(row[i]))
                    {
                        throw new ArgumentException("query parameter cannot be null or DBNull");
                    }

                    switch (row[i])
                    {
                        case bool val:
                            var i8Builder = new I8Builder(TDengineDataType.TSDB_DATA_TYPE_BOOL);
                            i8Builder.Append(val ? (sbyte)1 : (sbyte)0);
                            _colBuilders[i] = i8Builder;
                            break;
                        case sbyte val:
                            var i8 = new I8Builder(TDengineDataType.TSDB_DATA_TYPE_TINYINT);
                            i8.Append(val);
                            _colBuilders[i] = i8;
                            break;
                        case short val:
                            var i16 = new I16Builder(TDengineDataType.TSDB_DATA_TYPE_SMALLINT);
                            i16.Append(val);
                            _colBuilders[i] = i16;
                            break;
                        case int val:
                            var i32 = new I32Builder(TDengineDataType.TSDB_DATA_TYPE_INT);
                            i32.Append(val);
                            _colBuilders[i] = i32;
                            break;
                        case long val:
                            var i64 = new I64Builder(TDengineDataType.TSDB_DATA_TYPE_BIGINT);
                            i64.Append(val);
                            _colBuilders[i] = i64;
                            break;
                        case byte val:
                            var u8 = new U8Builder(TDengineDataType.TSDB_DATA_TYPE_UTINYINT);
                            u8.Append(val);
                            _colBuilders[i] = u8;
                            break;
                        case ushort val:
                            var u16 = new U16Builder(TDengineDataType.TSDB_DATA_TYPE_USMALLINT);
                            u16.Append(val);
                            _colBuilders[i] = u16;
                            break;
                        case uint val:
                            var u32 = new U32Builder(TDengineDataType.TSDB_DATA_TYPE_UINT);
                            u32.Append(val);
                            _colBuilders[i] = u32;
                            break;
                        case ulong val:
                            var u64 = new U64Builder(TDengineDataType.TSDB_DATA_TYPE_UBIGINT);
                            u64.Append(val);
                            _colBuilders[i] = u64;
                            break;
                        case float val:
                            var f32 = new F32Builder(TDengineDataType.TSDB_DATA_TYPE_FLOAT);
                            f32.Append(val);
                            _colBuilders[i] = f32;
                            break;
                        case double val:
                            var f64 = new F64Builder(TDengineDataType.TSDB_DATA_TYPE_DOUBLE);
                            f64.Append(val);
                            _colBuilders[i] = f64;
                            break;
                        case DateTime val:
                            var dateTimeBuilder = new VariableLengthBuilder(TDengineDataType.TSDB_DATA_TYPE_BINARY);
                            dateTimeBuilder.AppendString(val.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK"));
                            _colBuilders[i] = dateTimeBuilder;
                            break;
                        case DateTimeOffset val:
                            var dateTimeOffsetBuilder =
                                new VariableLengthBuilder(TDengineDataType.TSDB_DATA_TYPE_BINARY);
                            dateTimeOffsetBuilder.AppendString(val.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK"));
                            _colBuilders[i] = dateTimeOffsetBuilder;
                            break;
                        case byte[] val:
                            var bytesBuilder = new VariableLengthBuilder(TDengineDataType.TSDB_DATA_TYPE_BINARY);
                            bytesBuilder.AppendBytes(val);
                            _colBuilders[i] = bytesBuilder;
                            break;
                        case string val:
                            var stringBuilder = new VariableLengthBuilder(TDengineDataType.TSDB_DATA_TYPE_BINARY);
                            stringBuilder.AppendString(val);
                            _colBuilders[i] = stringBuilder;
                            break;
                        default:
                            throw new ArgumentException(
                                $"Bind type {row[i].GetType()} not supported for query parameter");
                    }
                }
            }
            catch
            {
                _colBuilders = null;
                throw;
            }
        }
        
        public void BindRow(object[] row)
        {
            CheckPrepared();
            if (row.Length == 0)
            {
                return;
            }

            if (string.IsNullOrEmpty(_sql))
            {
                throw new InvalidOperationException("This statement does not prepared.");
            }

            if (_isInsert)
            {
                if (row.Length != _colBuilders.Length)
                {
                    throw new ArgumentException(
                        $"Expected {_colBuilders.Length} columns, but got {row.Length}");
                }

                CacheRowValue(row, _colBuilders, _colFields);
            }
            else
            {
                if (row.Length != _fieldsCount)
                {
                    throw new ArgumentException(
                        $"Expected {_fieldsCount} fields, but got {row.Length}");
                }

                CacheQueryRow(row);
            }

            _isColSet = true;
        }
    }
}