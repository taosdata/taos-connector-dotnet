using System.Collections.Generic;
using TDengine.Driver.Impl.StmtBuilder;

namespace TDengine.Driver.Client
{
    class Stmt2TableData
    {
        public string TableName;
        public List<object>[] Cols;
        public object[] Tags;
        
        public Stmt2TableData(int colCount)
        {
            TableName = string.Empty;
            Cols = new List<object>[colCount];
            for (int i = 0; i < colCount; i++)
            {
                Cols[i] = new List<object>(1);
            }
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
        
        private Dictionary<string, Stmt2TableData> _tableInfos = new Dictionary<string, Stmt2TableData>();
        private Stmt2TableData _currentTableInfo;
        private bool _isTableNameSet;
        private bool _isTagsSet;
        private bool _isColSet;
        private bool _addBatched;
        private bool _executed;
        private int _affectedRows;
        private bool _schemaChanged;
        private TaosFieldE[] _queryFields;
        
        
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
            // _colBuilders = null;
            // _tagBuilders = null;
            _needTableName = false;
            _tableInfos.Clear();
            _isTableNameSet = false;
            _isTagsSet = false;
            _addBatched = false;
            _executed = false;
            // _tableNameBuilder = null;
            _schemaChanged = false;
            _currentTableInfo = null;
        }

        private void CleanBatch()
        {
            _isTableNameSet = false;
            _isTagsSet = false;
            _currentTableInfo = new Stmt2TableData(_isInsert? _colFields.Length: _fieldsCount);
        }

        private void CleanExec()
        {
            if (!_isInsert)
            {
                _queryFields = null;
            }

            _addBatched = false;
            _executed = true;
            _tableInfos.Clear();
        }

        public abstract void Dispose();
    }
}