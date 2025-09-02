using System.Collections.Generic;

namespace TDengine.Driver.Client
{
    class Stmt2TableData
    {
        public string TableName;
        public List<object>[] Cols;
        public object[] Tags;
        
        public Stmt2TableData(List<object>[] cols)
        {
            TableName = string.Empty;
            Cols = cols;
        }

        public bool IsColSet => Cols[0].Count > 0;
        public int Rows => Cols[0].Count;
    }
    
    public abstract partial class AbstractStmt : IStmt
    {
        private readonly int _binaryHeaderLength;
        private string _sql = string.Empty;
        private bool _isInsert;
        private int _fieldsCount;
        private TaosFieldAll[] _fields;
        private TaosFieldE[] _tagFields;
        private TaosFieldE[] _colFields;

        // private IFieldBuilder[] _colBuilders;
        // private IFieldBuilder[] _tagBuilders;
        private bool _needTableName;
        
        private readonly Dictionary<string, Stmt2TableData> _tableInfos = new Dictionary<string, Stmt2TableData>();
        private Stmt2TableData _currentTableInfo;
        private bool _isTableNameSet;
        private bool _isTagsSet;
        private bool _isColSet;
        private bool _addBatched;
        private bool _executed;
        private int _affectedRows;
        private bool _schemaChanged;
        private TaosFieldE[] _queryFields;
        
        private readonly Queue<List<object>> _objectListQueue = new Queue<List<object>>();
        private readonly Queue<Stmt2TableData> _tableInfoQueue = new Queue<Stmt2TableData>();
        
        private bool TryGetTableInfo(out Stmt2TableData tableInfo)
        {
            if (_tableInfoQueue.Count > 0)
            {
                tableInfo = _tableInfoQueue.Dequeue();
                return true;
            }

            tableInfo = null;
            return false;
        }
        
        private void SetObjectList(List<object>[] lists)
        {
            for (int i = 0; i < lists.Length; i++)
            {
                lists[i] = GetObjectList();
            }
        }
        
        private List<object> GetObjectList()
        {
            return _objectListQueue.Count > 0 ? _objectListQueue.Dequeue() : new List<object>();
        }
        
        private void ReturnObjectLists(List<object>[] lists)
        {
            for (int i = 0; i < lists.Length; i++)
            {
                ReturnObjectList(lists[i]);
                lists[i] = null;
            }
        }
        private void ReturnObjectList(List<object> list)
        {
            list.Clear();
            _objectListQueue.Enqueue(list);
        }

        private Stmt2TableData NewStmt2TableData()
        {
            if (TryGetTableInfo(out var info))
            {
                SetObjectList(info.Cols);
            }
            else
            {
                // new one
                var lists = new List<object>[_isInsert? _colFields.Length: _fieldsCount];
                SetObjectList(lists);
                info = new Stmt2TableData(lists);
            }

            return info;
        }
        
        private void ReturnTableInfo(Stmt2TableData info)
        {
            if (info == null) return;
            ReturnObjectLists(info.Cols);
            info.Tags = null;
            info.TableName = string.Empty;
            _tableInfoQueue.Enqueue(info);
        }
        
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
            _needTableName = false;
            _tableInfos.Clear();
            _isTableNameSet = false;
            _isTagsSet = false;
            _addBatched = false;
            _executed = false;
            _schemaChanged = false;
            _currentTableInfo = null;
            // clean cached object lists and table info queue
            _tableInfoQueue.Clear();
            _objectListQueue.Clear();
        }

        private void CleanBatch()
        {
            _isTableNameSet = false;
            _isTagsSet = false;
            _currentTableInfo = NewStmt2TableData();
        }

        private void CleanExec()
        {
            if (!_isInsert)
            {
                _queryFields = null;
            }

            _addBatched = false;
            _executed = true;
            foreach (var tableInfo in _tableInfos.Values)
            {
                // return to cache
                ReturnTableInfo(tableInfo);
            }
            _tableInfos.Clear();
        }

        public abstract void Dispose();
    }
}