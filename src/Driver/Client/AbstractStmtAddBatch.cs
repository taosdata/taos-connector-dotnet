using System;
using TDengine.Driver.Impl.StmtBuilder;

namespace TDengine.Driver.Client
{
    public abstract partial class AbstractStmt
    {
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
    }
}