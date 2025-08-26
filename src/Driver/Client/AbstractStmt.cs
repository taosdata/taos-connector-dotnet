using System;
using System.Collections.Generic;
using TDengine.Driver.Impl.StmtBuilder;

namespace TDengine.Driver.Client
{
    public abstract partial class AbstractStmt : IStmt
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
        private bool _schemaChanged;

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
            _schemaChanged = false;
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

        public abstract void Dispose();
    }
}