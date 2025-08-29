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

        private readonly Pool _bufferPool;
        private Queue<Stmt2BindTableInfo> _cachedTableInfos = new Queue<Stmt2BindTableInfo>();

        private Stmt2BindTableInfo GetStmt2BindTableInfo()
        {
            if (_cachedTableInfos.Count > 0)
            {
                return _cachedTableInfos.Dequeue();
            }

            return new Stmt2BindTableInfo();
        }

        private void ReturnStmt2BindTableInfo(Stmt2BindTableInfo info)
        {
            info.TableName = null;
            if (info.Tags != null)
            {
                _bufferPool.ReturnColInfos(info.Tags);
                info.Tags = null;
            }

            if (info.Cols != null)
            {
                _bufferPool.ReturnColInfos(info.Cols);
                info.Cols = null;
            }

            _cachedTableInfos.Enqueue(info);
        }

        protected AbstractStmt(int binaryHeaderLength = 0)
        {
            _binaryHeaderLength = binaryHeaderLength;
            _bufferPool = new Pool();
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
            foreach (var tableInfo in _tableInfos.Values)
            {
                ReturnStmt2BindTableInfo(tableInfo);
            }

            _tableInfos.Clear();
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
            foreach (var tableInfo in _tableInfos.Values)
            {
                ReturnStmt2BindTableInfo(tableInfo);
            }

            _tableInfos.Clear();
        }

        public abstract void Dispose();
    }
}