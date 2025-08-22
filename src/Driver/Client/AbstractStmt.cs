using System;
using System.Collections.Generic;
using TDengine.Driver.Impl.StmtBuilder;

namespace TDengine.Driver.Client
{
    public abstract class AbstractStmt : IStmt
    {
        private readonly int _binaryHeaderLength;
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
        private bool _isTableNameSet;
        private bool _isTagsSet;
        private bool _isColSet;
        private bool _addBatched;
        private bool _executed;
        private TableNameBuilder _tableNameBuilder;
        private int _affectedRows;

        protected AbstractStmt(int binaryHeaderLength = 0)
        {
            _binaryHeaderLength = binaryHeaderLength;
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
            _isTableNameSet = false;
            _isTagsSet = false;
            _addBatched = false;
            _executed = false;
            _tableNameBuilder = null;
        }

        private void CleanBatch()
        {
            _isTableNameSet = false;
            _isTagsSet = false;
            _currentTableInfo = null;
        }

        private void CleanExec()
        {
            if (!_isInsert)
            {
                _colBuilders = null;
            }

            _addBatched = false;
            _executed = true;
            _tableInfos.Clear();
        }


        private bool NeedTags => _tagBuilders != null && _tagBuilders.Length > 0;

        private bool IsTableNameSet
        {
            get => _isTableNameSet;
            set
            {
                _isTableNameSet = value;
                if (value)
                {
                    _addBatched = false;
                    _executed = false;
                }
            }
        }

        private bool IsTagsSet
        {
            get => _isTagsSet;
            set
            {
                _isTagsSet = value;
                if (value)
                {
                    _addBatched = false;
                    _executed = false;
                }
            }
        }

        private bool IsColSet
        {
            get => _isColSet;
            set
            {
                _isColSet = value;
                if (value)
                {
                    _addBatched = false;
                    _executed = false;
                }
            }
        }


        public void Prepare(string query)
        {
            CleanCache();
            try
            {
                PrepareInternal(query, out bool isInsert, out int count, out TaosFieldAll[] fields);
                _isInsert = isInsert;
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
                            case TaosFieldType.TAOS_FIELD_QUERY:
                            case TaosFieldType.TAOS_FIELD_TBNAME:
                                break;
                            default:
                                throw new NotSupportedException(
                                    $"stmt field type not support: {(TaosFieldType)_fields[i].field_type}");
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
                                _tableNameBuilder = new TableNameBuilder();
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

        protected abstract void PrepareInternal(string query, out bool isInsert, out int count,
            out TaosFieldAll[] fields);

        public bool IsInsert()
        {
            CheckPrepared();
            return _isInsert;
        }

        public void SetTableName(string tableName)
        {
            CheckPrepared();
            if (_needTableName)
            {
                if (IsTableNameSet)
                {
                    throw new InvalidOperationException(
                        "Table name has already been set for current batch");
                }

                if (string.IsNullOrEmpty(tableName))
                {
                    throw new ArgumentException("Table name cannot be null or empty");
                }

                _tableNameBuilder.Add(tableName);
                if (_tableInfos.TryGetValue(tableName, out var info))
                {
                }
                else
                {
                    info = new Stmt2BindTableInfo
                    {
                        TableName = tableName
                    };
                }

                _currentTableInfo = info;
                IsTableNameSet = true;
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

            if (IsTagsSet)
            {
                throw new InvalidOperationException("Tags have already been set for current batch");
            }

            if (tags.Length != _tagBuilders.Length)
            {
                throw new ArgumentException(
                    $"Expected {_tagBuilders.Length} tags, but got {tags.Length}");
            }

            CacheRowValue(tags, _tagBuilders, _tagFields);
            IsTagsSet = true;
        }

        private void CacheRowValue(object[] obj, IFieldBuilder[] builders, TaosFieldE[] fields)
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
                    for (int j = 0; j < i; j++)
                    {
                        builders[j].Remove(1);
                    }

                    throw;
                }
            }
        }

        public TaosFieldE[] GetTagFields()
        {
            CheckPrepared();
            return _tagFields;
        }

        public TaosFieldE[] GetColFields()
        {
            CheckPrepared();
            return _colFields;
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

        private void CheckPrepared()
        {
            if (string.IsNullOrEmpty(_sql))
            {
                throw new InvalidOperationException("This statement has not been prepared.");
            }
        }

        private void CacheColumns(Array array, TaosFieldE field, IFieldBuilder builder, int bindIndex)
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

        public void AddBatch()
        {
            // check if the statement is prepared
            CheckPrepared();
            // check if the table name is set if required
            if (_needTableName && !IsTableNameSet)
            {
                throw new InvalidOperationException("Table name must be set before adding a batch.");
            }

            // check if tags are set if required
            if (NeedTags && !IsTagsSet)
            {
                throw new InvalidOperationException("Tags must be set before adding a batch.");
            }

            // check if columns are set
            if (!IsColSet)
            {
                throw new InvalidOperationException("Columns must be set before adding a batch.");
            }

            // check row count
            var rowCount = _colBuilders[0].Count;
            for (var i = 0; i < _colBuilders.Length; i++)
            {
                if (_colBuilders[i].Count == 0)
                {
                    throw new InvalidOperationException($"Column at index {i} has no rows to add.");
                }

                if (_colBuilders[i].Count != rowCount)
                {
                    throw new InvalidOperationException(
                        $"Column at index {i} has a different row count than the first column. Expected {rowCount}, but got {_colBuilders[i].Count}.");
                }
            }

            Stmt2BindTableInfo tableInfo;
            if (_currentTableInfo.HasValue)
            {
                tableInfo = _currentTableInfo.Value;
            }
            else
            {
                if (!_tableInfos.TryGetValue(string.Empty, out tableInfo))
                {
                    tableInfo = new Stmt2BindTableInfo()
                    {
                        TableName = string.Empty
                    };
                }
            }

            // set table columns
            if (tableInfo.Cols == null || tableInfo.Cols.Length == 0)
            {
                tableInfo.Cols = new Stmt2BindColInfo[_colBuilders.Length];
                for (var i = 0; i < _colBuilders.Length; i++)
                {
                    tableInfo.Cols[i] = _colBuilders[i].ToStmt2BindColInfo();
                    _colBuilders[i].Clear();
                }
            }
            else
            {
                if (tableInfo.Cols.Length != _colBuilders.Length)
                {
                    throw new InvalidOperationException(
                        $"Column count mismatch. Expected {tableInfo.Cols.Length}, but got {_colBuilders.Length}.");
                }

                for (var i = 0; i < _colBuilders.Length; i++)
                {
                    tableInfo.Cols[i] = _colBuilders[i].AddToStmt2BindColInfo(tableInfo.Cols[i]);
                    _colBuilders[i].Clear();
                }
            }

            // set table tags
            if (NeedTags && IsTagsSet)
            {
                if (tableInfo.Tags == null || tableInfo.Tags.Length == 0)
                {
                    tableInfo.Tags = new Stmt2BindColInfo[_tagBuilders.Length];
                    for (var i = 0; i < _tagBuilders.Length; i++)
                    {
                        tableInfo.Tags[i] = _tagBuilders[i].ToStmt2BindColInfo();
                        _tagBuilders[i].Clear();
                    }
                }
                else
                {
                    // tag has been set, ignore the current tags
                    foreach (var t in _tagBuilders)
                    {
                        t.Clear();
                    }
                }
            }

            // cache to dictionary
            _tableInfos[tableInfo.TableName] = tableInfo;
            // reset the current table info

            _addBatched = true;
            CleanBatch();
        }

        public void Exec()
        {
            if (!_addBatched)
            {
                throw new InvalidOperationException("No batch added. Call AddBatch() before Exec().");
            }

            var buffer = GenerateBindBinary();

            // print buffer
            // StringBuilder sb = new StringBuilder();
            // for (int i = 0; i < buffer.Length; i++)
            // {
            //     sb.Append($"0x{buffer[i]:X2}");
            //     if (i < buffer.Length - 1)
            //         sb.Append(", ");
            //     if (i % 16 == 15)
            //         sb.AppendLine();
            // }
            // Console.WriteLine(sb.ToString());
            BindBinaryInternal(buffer, out var affectedRows);
            if (_isInsert)
            {
                _affectedRows = affectedRows;
            }

            CleanExec();
        }

        private byte[] GenerateBindBinary()
        {
            var tableCount = _tableInfos.Count;
            const uint fixedHeaderLen = 28;
            var tableNameLengthLen = (uint)0;
            var tableNameBufferLen = (uint)0;
            var tagsDataLengthLen = (uint)0;
            var tagsBufferLen = (uint)0;
            var colsDataLengthLen = (uint)(tableCount * 4);
            var colsBufferLen = (uint)0;
            // table name
            if (_needTableName)
            {
                tableNameLengthLen = (uint)(_tableNameBuilder.Length * 2);
                tableNameBufferLen = (uint)_tableNameBuilder.TotalBufferLen;
            }

            if (NeedTags)
            {
                tagsDataLengthLen = (uint)(tableCount * 4);
            }

            foreach (var tableInfo in _tableInfos.Values)
            {
                // tags
                if (NeedTags)
                {
                    foreach (var tag in tableInfo.Tags)
                    {
                        tagsBufferLen += tag.TotalLength;
                    }
                }

                // cols
                foreach (var col in tableInfo.Cols)
                {
                    colsBufferLen += col.TotalLength;
                }
            }

            var tableNameLength = tableNameLengthLen + tableNameBufferLen;
            var tagsDataLength = tagsDataLengthLen + tagsBufferLen;
            var colsDataLength = colsDataLengthLen + colsBufferLen;
            var totalBufferLen = fixedHeaderLen + tableNameLength + tagsDataLength + colsDataLength;
            var tableNameOffset = fixedHeaderLen;
            var tagsOffset = tableNameOffset + tableNameLength;
            var colsOffset = tagsOffset + tagsDataLength;
            var buffer = new byte[totalBufferLen + _binaryHeaderLength];
            WriteU32(buffer, _binaryHeaderLength+0, totalBufferLen); // TotalLength
            WriteU32(buffer, _binaryHeaderLength+4, (uint)tableCount); // Count
            WriteU32(buffer, _binaryHeaderLength+8, NeedTags ? (uint)_tagBuilders.Length : 0); // TagCount
            WriteU32(buffer, _binaryHeaderLength+12, (uint)_colBuilders.Length); // ColCount
            WriteU32(buffer, _binaryHeaderLength+16, _needTableName ? fixedHeaderLen : 0); // TableNamesOffset
            WriteU32(buffer, _binaryHeaderLength+20, NeedTags ? tagsOffset : 0); // TagsOffset
            WriteU32(buffer, _binaryHeaderLength+24, colsOffset); // ColsOffset
            var tableNameLengthOffset = _binaryHeaderLength + (int)tableNameOffset;
            var tableNameBufferOffset = tableNameLengthOffset + (int)tableNameLengthLen;
            var tagsLengthOffset = _binaryHeaderLength+(int)tagsOffset;
            var tagsBufferOffset = tagsLengthOffset + (int)tagsDataLengthLen;
            var colsLengthOffset = _binaryHeaderLength+(int)colsOffset;
            var colsBufferOffset = colsLengthOffset + (int)colsDataLengthLen;
            if (_needTableName)
            {
                Buffer.BlockCopy(_tableNameBuilder.GetLengths(), 0, buffer, tableNameLengthOffset,
                    (int)tableNameLengthLen);
                Buffer.BlockCopy(_tableNameBuilder.GetBytes(), 0, buffer, tableNameBufferOffset,
                    (int)tableNameBufferLen);
                for (var i = 0; i < _tableNameBuilder.Length; i++)
                {
                    var tableName = _tableNameBuilder.TableNames[i];
                    var tableInfo = _tableInfos[tableName];
                    if (NeedTags)
                    {
                        tagsBufferOffset = SetBindData(buffer, tagsBufferOffset, tableInfo.Tags,
                            out var tableTagLength);
                        WriteU32(buffer, tagsLengthOffset, (uint)tableTagLength); // TagsDataLength
                        tagsLengthOffset += 4;
                    }

                    colsBufferOffset = SetBindData(buffer, colsBufferOffset, tableInfo.Cols,
                        out var tableColLength);
                    WriteU32(buffer, colsLengthOffset, (uint)tableColLength); // ColData
                    colsLengthOffset += 4;
                }

                _tableNameBuilder.Clear();
            }
            else
            {
                var tableInfo = _tableInfos[string.Empty];
                if (NeedTags)
                {
                    SetBindData(buffer, tagsBufferOffset, tableInfo.Tags, out var tableTagLength);
                    WriteU32(buffer, tagsLengthOffset, (uint)tableTagLength); // TagsDataLength
                }

                SetBindData(buffer, colsBufferOffset, tableInfo.Cols, out var tableColLength);
                WriteU32(buffer, colsLengthOffset, (uint)tableColLength); // ColData
            }

            return buffer;
        }

        private int SetBindData(byte[] buffer, int offset, Stmt2BindColInfo[] fields, out int totalLen)
        {
            totalLen = 0;
            for (var i = 0; i < fields.Length; i++)
            {
                totalLen += (int)fields[i].TotalLength;
                WriteU32(buffer, offset, fields[i].TotalLength); // TotalLength
                offset += 4;
                WriteU32(buffer, offset, (uint)fields[i].DataType); // DataType
                offset += 4;
                WriteU32(buffer, offset, (uint)fields[i].Num); // Num
                offset += 4;
                if (fields[i].IsNull != null)
                {
                    Buffer.BlockCopy(fields[i].IsNull, 0, buffer, offset, fields[i].Num); // IsNull
                }

                // If IsNull is null, it means no null values, fill with 0, the buffer is already initialized with 0
                offset += fields[i].Num;
                buffer[offset] = fields[i].HaveLength; // HaveLength
                offset += 1;
                if (fields[i].HaveLength == 1 && fields[i].Length != null)
                {
                    Buffer.BlockCopy(fields[i].Length, 0, buffer, offset, fields[i].Num * 4); // Length
                    offset += fields[i].Num * 4;
                }

                WriteU32(buffer, offset, fields[i].BufferLength); // BufferLength
                offset += 4;
                if (fields[i].Buffer != null)
                {
                    Buffer.BlockCopy(fields[i].Buffer, 0, buffer, offset, fields[i].Buffer.Length); // Buffer
                    offset += fields[i].Buffer.Length;
                }
            }

            return totalLen;
        }

        // little-endian write int32 to buffer
        private void WriteU32(byte[] buffer, int offset, uint value)
        {
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
            buffer[offset + 2] = (byte)(value >> 16);
            buffer[offset + 3] = (byte)(value >> 24);
        }

        protected abstract void BindBinaryInternal(byte[] data, out int affectedRows);

        public long Affected()
        {
            return _affectedRows;
        }

        public IRows Result()
        {
            if (!_executed)
            {
                throw new InvalidOperationException("Statement has not been executed yet.");
            }

            if (_isInsert)
            {
                return InsertResultInternal(_affectedRows);
            }

            return QueryResultInternal();
        }

        protected abstract IRows QueryResultInternal();
        protected abstract IRows InsertResultInternal(int affectedRows);

        public abstract void Dispose();
    }
}