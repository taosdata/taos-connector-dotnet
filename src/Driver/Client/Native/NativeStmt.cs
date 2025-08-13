using System;
using System.Collections.Generic;
using TDengine.Driver.Impl.NativeMethods;
using TDengine.Driver.Impl.StmtBuilder;

namespace TDengine.Driver.Client.Native
{
    public class NativeStmt : IStmt
    {
        private IntPtr _stmt;
        private readonly TimeZoneInfo _tz;
        private string _sql = string.Empty;
        private bool _isInsert;
        private int _fieldsCount;
        private TaosFieldAll[] _fields;
        private TaosFieldE[] _tagFields;
        private TaosFieldE[] _colFields;
        
        private IFieldBuilder[] _colBuilders;
        private IFieldBuilder[] _tagBuilders;
        private bool _needTableName;
        private Dictionary<string, Stmt2BindTableInfo> _tableInfos = new Dictionary<string, Stmt2BindTableInfo>();
        private Stmt2BindTableInfo? _currentTableInfo;
        private List<string> _tableNames = new List<string>();
        private bool _isTableNameSet;
        private bool _isTagsSet;
        private bool _addBatched;
        private int _currentRowIndex;
        private bool _duplicatedTableName;

        public NativeStmt(IntPtr stmt, TimeZoneInfo tz)
        {
            _stmt = stmt;
            _tz = tz;
        }

        private void CleanCache()
        {
            _sql = string.Empty;
            _isInsert = false;
            _fieldsCount = 0;
            _fields = null;
            _tagFields = null;
            _colFields = null;
            _colBuilders = null;
            _tagBuilders = null;
            _needTableName = false;
            _tableInfos = new Dictionary<string, Stmt2BindTableInfo>();
            _currentTableInfo = null;
            _tableNames = new List<string>();
            _isTableNameSet = false;
            _isTagsSet = false;
            _addBatched = false;
            _currentRowIndex = 0;
            _duplicatedTableName = false;
        }

        public void Prepare(string query)
        {
            CleanCache();
            var code = NativeMethods.TaosStmt2Prepare(_stmt, query);
            StmtCheckError(code);
            try
            {
                code = NativeMethods.TaosStmt2IsInsert(_stmt, out bool isInsert);
                StmtCheckError(code);
                _isInsert = isInsert;
                code = NativeMethods.TaosStmt2GetFields(_stmt, out int count, out var fields);
                StmtCheckError(code);
                _fieldsCount = count;
                _fields = fields;
                _sql = query;
                if (_isInsert)
                {
                    var tagCount = 0;
                    var colCount = 0;
                    for (var i = 0; i < _fieldsCount; i++)
                    {
                        switch ((TaosFieldType)_fields[i].field_type)
                        {
                            case TaosFieldType.TAOS_FIELD_TAG:
                                tagCount++;
                                break;
                            case TaosFieldType.TAOS_FIELD_COL:
                                colCount++;
                                break;
                        }
                    }

                    _colBuilders = new IFieldBuilder[colCount];
                    _colFields = new TaosFieldE[colCount];
                    _tagBuilders = new IFieldBuilder[tagCount];
                    _tagFields = new TaosFieldE[tagCount];
                    var tagIndex = 0;
                    var colIndex = 0;
                    for (var i = 0; i < _fields.Length; i++)
                    {
                        switch ((TaosFieldType)_fields[i].field_type)
                        {
                            case TaosFieldType.TAOS_FIELD_TAG:
                                _tagBuilders[tagIndex] = Builder.CreateBuilder((TDengineDataType)_fields[i].type);
                                _tagFields[tagIndex] = TDengineConstant.ConvertToTaosFieldE(fields[i]);
                                tagIndex++;
                                break;
                            case TaosFieldType.TAOS_FIELD_COL:
                                _colBuilders[colIndex] = Builder.CreateBuilder((TDengineDataType)_fields[i].type);
                                _colFields[colIndex] = TDengineConstant.ConvertToTaosFieldE(fields[i]);
                                colIndex++;
                                break;
                            case TaosFieldType.TAOS_FIELD_TBNAME:
                                _needTableName = true;
                                break;
                            default:
                                throw new NotSupportedException(
                                    $"stmt field type not support: {(TaosFieldType)_fields[i].field_type}");
                        }
                    }
                }
            }
            catch
            {
                CleanCache();
                throw;
            }
        }

        private void StmtCheckError(int code)
        {
            if (code == 0) return;
            var errorStr = NativeMethods.StmtErrorStr(_stmt);
            throw new TDengineError(code, errorStr);
        }

        public bool IsInsert()
        {
            return _isInsert;
        }

        public void SetTableName(string tableName)
        {
            if (_needTableName)
            {
                if (_isTableNameSet)
                {
                    throw new InvalidOperationException(
                        "Table name has already been set for current batch");
                }
                if (_tableInfos.TryGetValue(tableName, out var info))
                {
                    _duplicatedTableName = true;
                }
                else
                {
                    info = new Stmt2BindTableInfo
                    {
                        TagOffset = 0,
                        ColOffsets = null,
                        ColCounts = null
                    };
                }
                _currentTableInfo = info;
                _tableInfos[tableName] = info;
                _isTableNameSet = true;
            }
            else
            {
                throw new InvalidOperationException(
                    "Table name is not required for this statement or not supported in this context.");
            }
        }

        public void SetTags(object[] tags)
        {
            if (tags.Length == 0)
            {
                return;
            }

            if (_tagBuilders == null || _tagBuilders.Length == 0 || !_isInsert)
            {
                throw new InvalidOperationException("This statement does not need tags.");
            }

            if (_isTagsSet)
            {
                throw new InvalidOperationException("Tags have already been set for current batch");
            }

            if (tags.Length != _tagBuilders.Length)
            {
                throw new ArgumentException(
                    $"Expected {_tagBuilders.Length} tags, but got {tags.Length}");
            }

            CacheRowValue(tags, _tagBuilders, _tagFields);
            _isTagsSet = true;
        }

        private void CacheRowValue(object[] obj, IFieldBuilder[] builders, TaosFieldE[] fields)
        {
            for (var i = 0; i < builders.Length; i++)
            {
                if (builders[i] == null || Convert.IsDBNull(builders[i]))
                {
                    _tagBuilders[i].AppendNull();
                }
                else
                {
                    switch (obj[i])
                    {
                        case bool val:
                            if (_tagBuilders[i] is I8Builder i8)
                            {
                                i8.Append(val ? (sbyte)1 : (sbyte)0);
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type bool not supported for field {fields[i].name}");
                            }

                            break;
                        case sbyte val:
                            if (_tagBuilders[i] is I8Builder i8Builder)
                            {
                                i8Builder.Append(val);
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type sbyte not supported for field {fields[i].name}");
                            }

                            break;
                        case short val:
                            if (_tagBuilders[i] is I16Builder i16)
                            {
                                i16.Append(val);
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type short not supported for field {fields[i].name}");
                            }

                            break;
                        case int val:
                            if (_tagBuilders[i] is I32Builder i32)
                            {
                                i32.Append(val);
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type int not supported for field {fields[i].name}");
                            }

                            break;
                        case long val:
                            if (_tagBuilders[i] is I64Builder i64)
                            {
                                if (_tagBuilders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_BIGINT ||
                                    _tagBuilders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
                                {
                                    i64.Append(val);
                                }
                                else
                                {
                                    throw new NotSupportedException(
                                        $"Bind type long not supported for field {fields[i].name}");
                                }
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type long not supported for field {fields[i].name}");
                            }

                            break;
                        case byte val:
                            if (_tagBuilders[i] is U8Builder u8)
                            {
                                u8.Append(val);
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type byte not supported for field {fields[i].name}");
                            }

                            break;
                        case ushort val:
                            if (_tagBuilders[i] is U16Builder u16)
                            {
                                u16.Append(val);
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type ushort not supported for field {fields[i].name}");
                            }

                            break;
                        case uint val:
                            if (_tagBuilders[i] is U32Builder u32)
                            {
                                u32.Append(val);
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type uint not supported for field {fields[i].name}");
                            }

                            break;
                        case ulong val:
                            if (_tagBuilders[i] is U64Builder u64)
                            {
                                u64.Append(val);
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type ulong not supported for field {fields[i].name}");
                            }

                            break;
                        case float val:
                            if (_tagBuilders[i] is F32Builder f32)
                            {
                                f32.Append(val);
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type float not supported for field {fields[i].name}");
                            }

                            break;
                        case double val:
                            if (_tagBuilders[i] is F64Builder f64)
                            {
                                f64.Append(val);
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type double not supported for field {fields[i].name}");
                            }

                            break;
                        case DateTime val:
                            if (_tagBuilders[i] is I64Builder i64Builder)
                            {
                                if (_tagBuilders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
                                {
                                    i64Builder.Append(TDengineConstant.ConvertDateTimeToTimestamp(val,
                                        (TDenginePrecision)fields[i].precision));
                                }
                                else
                                {
                                    throw new NotSupportedException(
                                        $"Bind type DateTime not supported for field {fields[i].name}");
                                }
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type DateTime not supported for field {fields[i].name}");
                            }

                            break;
                        case DateTimeOffset val:
                            if (_tagBuilders[i] is I64Builder i64OffsetBuilder)
                            {
                                if (_tagBuilders[i].DataType == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
                                {
                                    i64OffsetBuilder.Append(TDengineConstant.ConvertDateTimeOffsetToTimestamp(val,
                                        (TDenginePrecision)fields[i].precision));
                                }
                                else
                                {
                                    throw new NotSupportedException(
                                        $"Bind type DateTimeOffset not supported for field {fields[i].name}");
                                }
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type DateTimeOffset not supported for field {fields[i].name}");
                            }

                            break;
                        case byte[] val:
                            if (_tagBuilders[i] is VariableLengthBuilder variableLengthBuilder)
                            {
                                variableLengthBuilder.AppendBytes(val);
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type byte[] not supported for field {fields[i].name}");
                            }

                            break;
                        case string val:
                            if (_tagBuilders[i] is VariableLengthBuilder variableLengthStringBuilder)
                            {
                                variableLengthStringBuilder.AppendString(val);
                            }
                            else
                            {
                                throw new NotSupportedException(
                                    $"Bind type string not supported for field {fields[i].name}");
                            }

                            break;
                        default:
                            throw new NotSupportedException(
                                $"Bind type {builders[i].GetType()} not supported for field {fields[i].name}");
                    }
                }
            }
        }

        public TaosFieldE[] GetTagFields()
        {
            return _tagFields;
        }

        public TaosFieldE[] GetColFields()
        {
            return _colFields;
        }

        public void BindRow(object[] row)
        {
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
                CacheRowValue(row,_colBuilders, _colFields);
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
                for (int i = 0; i < row.Length; i++)
                {
                    if (row[i] == null || Convert.IsDBNull(row[i]))
                    {
                        throw new ArgumentException("query parameter cannot be null or DBNull");
                    }

                    switch (row[i])
                    {
                        case bool val:
                            var i8Builder = new I8Builder(TDengineDataType.TSDB_DATA_TYPE_BLOB);
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
                            throw new NotSupportedException(
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

        public void BindColumn(TaosFieldE[] field, params Array[] arrays)
        {
            var multiBind = new TAOS_MULTI_BIND[arrays.Length];
            try
            {
                for (int i = 0; i < arrays.Length; i++)
                {
                    multiBind[i] = GenerateBindColumn(arrays[i], field[i], i);
                }

                NativeMethods.StmtBindParamBatch(_stmt, multiBind);
            }
            finally
            {
                // if GenerateBindColumn throws an exception or StmtBindParamBatch finishes,
                // free all allocated memory for multiBind
                foreach (var bind in multiBind)
                {
                    MultiBind.FreeTaosBind(bind);
                }
            }
        }

        private static TAOS_MULTI_BIND GenerateBindColumn(Array array, TaosFieldE field, int bindIndex)
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
                    if (elementType == typeof(bool?))
                    {
                        return MultiBind.MultiBindBool((bool?[])array);
                    }

                    if (elementType == typeof(bool))
                    {
                        return MultiBind.MultiBindBool((bool[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, BOOL database type requires bool[] or bool?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                    if (elementType == typeof(sbyte?))
                    {
                        return MultiBind.MultiBindTinyInt((sbyte?[])array);
                    }

                    if (elementType == typeof(sbyte))
                    {
                        return MultiBind.MultiBindTinyInt((sbyte[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, TINYINT database type requires sbyte[] or sbyte?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                    if (elementType == typeof(short?))
                    {
                        return MultiBind.MultiBindSmallInt((short?[])array);
                    }

                    if (elementType == typeof(short))
                    {
                        return MultiBind.MultiBindSmallInt((short[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, SMALLINT database type requires short[] or short?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_INT:
                    if (elementType == typeof(int?))
                    {
                        return MultiBind.MultiBindInt((int?[])array);
                    }

                    if (elementType == typeof(int))
                    {
                        return MultiBind.MultiBindInt((int[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, INT database type requires int[] or int?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                    if (elementType == typeof(long?))
                    {
                        return MultiBind.MultiBindBigInt((long?[])array);
                    }

                    if (elementType == typeof(long))
                    {
                        return MultiBind.MultiBindBigInt((long[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, BIGINT database type requires long[] or long?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                    if (elementType == typeof(float?))
                    {
                        return MultiBind.MultiBindFloat((float?[])array);
                    }

                    if (elementType == typeof(float))
                    {
                        return MultiBind.MultiBindFloat((float[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, FLOAT database type requires float[] or float?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                    if (elementType == typeof(double?))
                    {
                        return MultiBind.MultiBindDouble((double?[])array);
                    }

                    if (elementType == typeof(double))
                    {
                        return MultiBind.MultiBindDouble((double[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, DOUBLE database type requires double[] or double?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                    if (elementType == typeof(byte[]))
                    {
                        return MultiBind.MultiBindBytesArray((byte[][])array, TDengineDataType.TSDB_DATA_TYPE_BINARY);
                    }

                    if (elementType == typeof(string))
                    {
                        return MultiBind.MultiBindStringArray((string[])array, TDengineDataType.TSDB_DATA_TYPE_BINARY);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, BINARY/VARCHAR database type requires byte[][] or string[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                    if (elementType == typeof(DateTime?))
                    {
                        return MultiBind.MultiBindTimestamp((DateTime?[])array, (TDenginePrecision)field.precision);
                    }

                    if (elementType == typeof(DateTime))
                    {
                        return MultiBind.MultiBindTimestamp((DateTime[])array, (TDenginePrecision)field.precision);
                    }

                    if (elementType == typeof(long?))
                    {
                        return MultiBind.MultiBindTimestamp((long?[])array);
                    }

                    if (elementType == typeof(long))
                    {
                        return MultiBind.MultiBindTimestamp((long[])array);
                    }

                    if (elementType == typeof(DateTimeOffset?))
                    {
                        return MultiBind.MultiBindTimestamp((DateTimeOffset?[])array,
                            (TDenginePrecision)field.precision);
                    }

                    if (elementType == typeof(DateTimeOffset))
                    {
                        return MultiBind.MultiBindTimestamp((DateTimeOffset[])array,
                            (TDenginePrecision)field.precision);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, TIMESTAMP database type requires one of the following array types: DateTime[], DateTime?[], long[], long?[], DateTimeOffset[], DateTimeOffset?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                    if (elementType == typeof(string))
                    {
                        return MultiBind.MultiBindStringArray((string[])array, TDengineDataType.TSDB_DATA_TYPE_NCHAR);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, NCHAR database type requires string[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                    if (elementType == typeof(byte?))
                    {
                        return MultiBind.MultiBindUTinyInt((byte?[])array);
                    }

                    if (elementType == typeof(byte))
                    {
                        return MultiBind.MultiBindUTinyInt((byte[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, TINYINT UNSIGNED database type requires byte[] or byte?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_USMALLINT:
                    if (elementType == typeof(ushort?))
                    {
                        return MultiBind.MultiBindUSmallInt((ushort?[])array);
                    }

                    if (elementType == typeof(ushort))
                    {
                        return MultiBind.MultiBindUSmallInt((ushort[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, SMALLINT UNSIGNED database type requires ushort[] or ushort?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_UINT:
                    if (elementType == typeof(uint?))
                    {
                        return MultiBind.MultiBindUInt((uint?[])array);
                    }

                    if (elementType == typeof(uint))
                    {
                        return MultiBind.MultiBindUInt((uint[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, INT UNSIGNED database type requires uint[] or uint?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                    if (elementType == typeof(ulong?))
                    {
                        return MultiBind.MultiBindUBigInt((ulong?[])array);
                    }

                    if (elementType == typeof(ulong))
                    {
                        return MultiBind.MultiBindUBigInt((ulong[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, BIGINT UNSIGNED database type requires ulong[] or ulong?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                    if (elementType == typeof(byte[]))
                    {
                        return MultiBind.MultiBindBytesArray((byte[][])array, TDengineDataType.TSDB_DATA_TYPE_JSONTAG);
                    }

                    if (elementType == typeof(string))
                    {
                        return MultiBind.MultiBindStringArray((string[])array, TDengineDataType.TSDB_DATA_TYPE_JSONTAG);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, JSON database type requires byte[][] or string[], but got an array of {elementType.Name}");

                case TDengineDataType.TSDB_DATA_TYPE_VARBINARY:
                    if (elementType == typeof(byte[]))
                    {
                        return MultiBind.MultiBindBytesArray((byte[][])array,
                            TDengineDataType.TSDB_DATA_TYPE_VARBINARY);
                    }

                    if (elementType == typeof(string))
                    {
                        return MultiBind.MultiBindStringArray((string[])array,
                            TDengineDataType.TSDB_DATA_TYPE_VARBINARY);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, VARBINARY database type requires byte[][] or string[], but got an array of {elementType.Name}");

                case TDengineDataType.TSDB_DATA_TYPE_GEOMETRY:
                    if (elementType == typeof(byte[]))
                    {
                        return MultiBind.MultiBindBytesArray((byte[][])array,
                            TDengineDataType.TSDB_DATA_TYPE_GEOMETRY);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, GEOMETRY database type requires byte[][], but got an array of {elementType.Name}");

                default:
                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, {TDengineConstant.GetFieldTypeName(field.type)} database type not supported");
            }
        }

        public void AddBatch()
        {
            var code = NativeMethods.StmtAddBatch(_stmt);
            StmtCheckError(code);
        }

        public void Exec()
        {
            var code = NativeMethods.StmtExecute(_stmt);
            StmtCheckError(code);
        }

        public long Affected()
        {
            return NativeMethods.StmtAffetcedRowsOnce(_stmt);
        }

        public IRows Result()
        {
            if (IsInsert())
            {
                return new NativeRows((int)Affected());
            }

            var result = NativeMethods.StmtUseResult(_stmt);
            if (result == IntPtr.Zero)
            {
                throw new Exception("stmt is not query");
            }

            return new NativeRows(result, _tz, true);
        }

        public void Dispose()
        {
            if (_stmt != IntPtr.Zero)
            {
                NativeMethods.StmtClose(_stmt);
                _stmt = IntPtr.Zero;
            }
        }
    }
}