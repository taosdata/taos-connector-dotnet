using System;
using TDengine.Driver.Impl.StmtBuilder;

namespace TDengine.Driver.Client
{
    public abstract partial class AbstractStmt
    {
        private static void CacheColumns(Array array, TaosFieldE field, IFieldBuilder builder, int bindIndex)
        {
            var elementType = array.GetType().GetElementType();
            if (elementType == null)
            {
                throw new ArgumentException(
                    $"BindIndex: {bindIndex}, field name: {field.name}, Expected an array type, but received {array.GetType().Name}");
            }

            switch ((TDengineDataType)field.type)
            {
                case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                    if (builder is I8Builder boolBuilder)
                    {
                        if (elementType == typeof(bool?))
                        {
                            var boolArray = (bool?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (boolArray[i].HasValue)
                                {
                                    boolBuilder.Append(boolArray[i].Value ? (sbyte)1 : (sbyte)0);
                                }
                                else
                                {
                                    boolBuilder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(bool))
                        {
                            var boolArray = (bool[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                boolBuilder.Append(boolArray[i] ? (sbyte)1 : (sbyte)0);
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, BOOL database type requires bool[] or bool?[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for BOOL: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                    if (builder is I8Builder i8Builder)
                    {
                        if (elementType == typeof(sbyte?))
                        {
                            var sbyteArray = (sbyte?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (sbyteArray[i].HasValue)
                                {
                                    i8Builder.Append(sbyteArray[i].Value);
                                }
                                else
                                {
                                    i8Builder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(sbyte))
                        {
                            var sbyteArray = (sbyte[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                i8Builder.Append(sbyteArray[i]);
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, TINYINT database type requires sbyte[] or sbyte?[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for TINYINT: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                    if (builder is I16Builder i16Builder)
                    {
                        if (elementType == typeof(short?))
                        {
                            var shortArray = (short?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (shortArray[i].HasValue)
                                {
                                    i16Builder.Append(shortArray[i].Value);
                                }
                                else
                                {
                                    i16Builder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(short))
                        {
                            var shortArray = (short[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                i16Builder.Append(shortArray[i]);
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, SMALLINT database type requires short[] or short?[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for SMALLINT: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_INT:
                    if (builder is I32Builder i32Builder)
                    {
                        if (elementType == typeof(int?))
                        {
                            var intArray = (int?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (intArray[i].HasValue)
                                {
                                    i32Builder.Append(intArray[i].Value);
                                }
                                else
                                {
                                    i32Builder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(int))
                        {
                            var intArray = (int[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                i32Builder.Append(intArray[i]);
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, INT database type requires int[] or int?[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for INT: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                    if (builder is I64Builder i64Builder)
                    {
                        if (elementType == typeof(long?))
                        {
                            var longArray = (long?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (longArray[i].HasValue)
                                {
                                    i64Builder.Append(longArray[i].Value);
                                }
                                else
                                {
                                    i64Builder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(long))
                        {
                            var longArray = (long[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                i64Builder.Append(longArray[i]);
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, BIGINT database type requires long[] or long?[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for BIGINT: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                    if (builder is F32Builder floatBuilder)
                    {
                        if (elementType == typeof(float?))
                        {
                            var floatArray = (float?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (floatArray[i].HasValue)
                                {
                                    floatBuilder.Append(floatArray[i].Value);
                                }
                                else
                                {
                                    floatBuilder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(float))
                        {
                            var floatArray = (float[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                floatBuilder.Append(floatArray[i]);
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, FLOAT database type requires float[] or float?[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for FLOAT: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                    if (builder is F64Builder doubleBuilder)
                    {
                        if (elementType == typeof(double?))
                        {
                            var doubleArray = (double?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (doubleArray[i].HasValue)
                                {
                                    doubleBuilder.Append(doubleArray[i].Value);
                                }
                                else
                                {
                                    doubleBuilder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(double))
                        {
                            var doubleArray = (double[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                doubleBuilder.Append(doubleArray[i]);
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, DOUBLE database type requires double[] or double?[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for DOUBLE: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                case TDengineDataType.TSDB_DATA_TYPE_VARBINARY:
                    if (builder is VariableLengthBuilder variableLengthBuilder)
                    {
                        if (elementType == typeof(byte[]))
                        {
                            var byteArray = (byte[][])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (byteArray[i] != null)
                                {
                                    variableLengthBuilder.AppendBytes(byteArray[i]);
                                }
                                else
                                {
                                    variableLengthBuilder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(string))
                        {
                            var stringArray = (string[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (stringArray[i] != null)
                                {
                                    variableLengthBuilder.AppendString(stringArray[i]);
                                }
                                else
                                {
                                    variableLengthBuilder.AppendNull();
                                }
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, {TDengineConstant.GetFieldTypeName(field.type)} database type requires byte[][] or string[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for BINARY: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                    if (builder is I64Builder timestampBuilder)
                    {
                        if (elementType == typeof(DateTime?))
                        {
                            var dateTimeArray = (DateTime?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (dateTimeArray[i].HasValue)
                                {
                                    timestampBuilder.Append(TDengineConstant.ConvertDateTimeToTimestamp(
                                        dateTimeArray[i].Value, (TDenginePrecision)field.precision));
                                }
                                else
                                {
                                    timestampBuilder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(DateTime))
                        {
                            var dateTimeArray = (DateTime[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                timestampBuilder.Append(TDengineConstant.ConvertDateTimeToTimestamp(
                                    dateTimeArray[i], (TDenginePrecision)field.precision));
                            }
                        }
                        else if (elementType == typeof(long?))
                        {
                            var longArray = (long?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (longArray[i].HasValue)
                                {
                                    timestampBuilder.Append(longArray[i].Value);
                                }
                                else
                                {
                                    timestampBuilder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(long))
                        {
                            var longArray = (long[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                timestampBuilder.Append(longArray[i]);
                            }
                        }
                        else if (elementType == typeof(DateTimeOffset?))
                        {
                            var dateTimeOffsetArray = (DateTimeOffset?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (dateTimeOffsetArray[i].HasValue)
                                {
                                    timestampBuilder.Append(TDengineConstant.ConvertDateTimeOffsetToTimestamp(
                                        dateTimeOffsetArray[i].Value, (TDenginePrecision)field.precision));
                                }
                                else
                                {
                                    timestampBuilder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(DateTimeOffset))
                        {
                            var dateTimeOffsetArray = (DateTimeOffset[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                timestampBuilder.Append(TDengineConstant.ConvertDateTimeOffsetToTimestamp(
                                    dateTimeOffsetArray[i], (TDenginePrecision)field.precision));
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, TIMESTAMP database type requires one of the following array types: DateTime[], DateTime?[], long[], long?[], DateTimeOffset[], DateTimeOffset?[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for TIMESTAMP: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                    if (builder is VariableLengthBuilder ncharBuilder)
                    {
                        if (elementType == typeof(string))
                        {
                            var stringArray = (string[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (stringArray[i] != null)
                                {
                                    ncharBuilder.AppendString(stringArray[i]);
                                }
                                else
                                {
                                    ncharBuilder.AppendNull();
                                }
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, NCHAR database type requires string[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for NCHAR: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                    if (builder is U8Builder u8Builder)
                    {
                        if (elementType == typeof(byte?))
                        {
                            var byteArray = (byte?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (byteArray[i].HasValue)
                                {
                                    u8Builder.Append(byteArray[i].Value);
                                }
                                else
                                {
                                    u8Builder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(byte))
                        {
                            var byteArray = (byte[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                u8Builder.Append(byteArray[i]);
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, UTINYINT database type requires byte[] or byte?[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for UTINYINT: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_USMALLINT:
                    if (builder is U16Builder u16Builder)
                    {
                        if (elementType == typeof(ushort?))
                        {
                            var ushortArray = (ushort?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (ushortArray[i].HasValue)
                                {
                                    u16Builder.Append(ushortArray[i].Value);
                                }
                                else
                                {
                                    u16Builder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(ushort))
                        {
                            var ushortArray = (ushort[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                u16Builder.Append(ushortArray[i]);
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, USMALLINT database type requires ushort[] or ushort?[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for USMALLINT: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UINT:
                    if (builder is U32Builder u32Builder)
                    {
                        if (elementType == typeof(uint?))
                        {
                            var uintArray = (uint?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (uintArray[i].HasValue)
                                {
                                    u32Builder.Append(uintArray[i].Value);
                                }
                                else
                                {
                                    u32Builder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(uint))
                        {
                            var uintArray = (uint[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                u32Builder.Append(uintArray[i]);
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, UINT database type requires uint[] or uint?[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for UINT: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                    if (builder is U64Builder u64Builder)
                    {
                        if (elementType == typeof(ulong?))
                        {
                            var ulongArray = (ulong?[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (ulongArray[i].HasValue)
                                {
                                    u64Builder.Append(ulongArray[i].Value);
                                }
                                else
                                {
                                    u64Builder.AppendNull();
                                }
                            }
                        }
                        else if (elementType == typeof(ulong))
                        {
                            var ulongArray = (ulong[])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                u64Builder.Append(ulongArray[i]);
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, UBIGINT database type requires ulong[] or ulong?[], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for UBIGINT: {builder.DataType}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_GEOMETRY:
                    if (builder is VariableLengthBuilder geometryBuilder)
                    {
                        if (elementType == typeof(byte[]))
                        {
                            var byteArray = (byte[][])array;
                            for (var i = 0; i < array.Length; i++)
                            {
                                if (byteArray[i] != null)
                                {
                                    geometryBuilder.AppendBytes(byteArray[i]);
                                }
                                else
                                {
                                    geometryBuilder.AppendNull();
                                }
                            }
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"BindIndex: {bindIndex}, field name: {field.name}, GEOMETRY database type requires byte[][], but got an array of {elementType.Name}");
                        }
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"BindIndex: {bindIndex}, field name: {field.name}, bind param type not supported for GEOMETRY: {builder.DataType}");
                    }

                    break;
                default:
                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, {TDengineConstant.GetFieldTypeName(field.type)} database type not supported");
            }
        }
        
        public void BindColumn(TaosFieldE[] field, params Array[] arrays)
        {
            CheckPrepared();
            if (_isInsert)
            {
                if (_colBuilders.Length != arrays.Length)
                {
                    throw new ArgumentException(
                        $"Expected {_colBuilders.Length} columns, but got {arrays.Length}");
                }

                var rowCount = arrays[0].Length;
                if (rowCount == 0)
                {
                    throw new ArgumentException($"Expected non-empty arrays, but got empty array");
                }

                for (var i = 0; i < arrays.Length; i++)
                {
                    try
                    {
                        if (arrays[i].Length != rowCount)
                        {
                            throw new ArgumentException(
                                $"Expected all arrays to have the same length, but array {i} has length {arrays[i].Length}, expected {rowCount}");
                        }

                        CacheColumns(arrays[i], _colFields[i], _colBuilders[i], i);
                    }
                    catch
                    {
                        for (var j = 0; j < i; j++)
                        {
                            _colBuilders[j].Remove(rowCount);
                        }

                        throw;
                    }
                }

                IsColSet = true;
            }
            else
            {
                throw new InvalidOperationException(
                    "Does not support binding columns for non-insert statements.");
            }
        }
    }
}