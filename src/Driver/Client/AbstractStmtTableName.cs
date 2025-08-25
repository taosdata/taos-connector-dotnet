using System;
using TDengine.Driver.Impl.StmtBuilder;

namespace TDengine.Driver.Client
{
    public abstract partial class AbstractStmt
    {
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

    }
}