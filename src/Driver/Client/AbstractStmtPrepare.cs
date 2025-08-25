using System;
using TDengine.Driver.Impl.StmtBuilder;

namespace TDengine.Driver.Client
{
    public abstract partial class AbstractStmt
    {
        public void Prepare(string query)
        {
            CleanCache();
            try
            {
                bool isInsert;
                int count;
                TaosFieldAll[] fields;
                try
                {
                    PrepareInternal(query, out isInsert, out count, out fields);
                }
                catch (Exception e)
                {
                    if (IsConnectionAvailable(e)) throw;
                    // reconnect
                    ReconnectInternal();
                    // re-prepare
                    PrepareInternal(query, out isInsert, out count, out fields);
                }

                _isInsert = isInsert;
                _fieldsCount = count;
                _fields = fields;
                _sql = query;
                if (!_isInsert) return;
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
            catch
            {
                CleanCache();
                throw;
            }
        }

        protected abstract void PrepareInternal(string query, out bool isInsert, out int count,
            out TaosFieldAll[] fields);
    }
}