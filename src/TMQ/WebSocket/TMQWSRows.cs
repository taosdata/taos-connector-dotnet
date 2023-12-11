using System;
using System.Collections.Generic;
using TDengine.Driver;
using TDengine.Driver.Impl.WebSocketMethods;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;

namespace TDengine.TMQ.WebSocket
{
    public class TMQWSRows : ITMQRows
    {
        private readonly TMQConnection _connection;
        private readonly ulong _resultId;
        private int _currentRow;
        private List<TDengineMeta> _metas;
        private int _blockSize;
        private byte[] _block;
        private bool _completed;
        private readonly BlockReader _blockReader;
        

        public TMQWSRows(WSTMQPollResp result, TMQConnection connection, TimeZoneInfo tz)
        {
            _connection = connection;
            _resultId = result.MessageId;
            _blockReader = new BlockReader(24, tz);
        }

        public object GetValue(int ordinal)
        {
            return _blockReader.Read(_currentRow, ordinal);
        }

        public bool Read()
        {
            if (_completed) return false;
            if (_block == null)
            {
                FetchBlock();
                return !_completed;
            }

            _currentRow += 1;
            if (_currentRow != _blockSize) return true;
            FetchBlock();
            return !_completed;
        }

        private void FetchBlock()
        {
            var fetchResult = _connection.Fetch(_resultId);
            _completed = fetchResult.Completed;
            _blockSize = fetchResult.Rows;
            _currentRow = 0;
            if (_completed)
            {
                return;
            }

            FieldCount = fetchResult.FieldsCount;
            TableName = fetchResult.TableName;
            _metas = new List<TDengineMeta>();
            for (int i = 0; i < FieldCount; i++)
            {
                _metas.Add(new TDengineMeta
                {
                    name = fetchResult.FieldsNames[i],
                    type = fetchResult.FieldsTypes[i],
                    size = (int)fetchResult.FieldsLengths[i]
                });
            }

            _block = _connection.FetchBlock(_resultId);
            _blockReader.SetTMQBlock(_block, fetchResult.Precision);
        }

        public int FieldCount { get; private set; }
        
        public string TableName { get; private set; }
        public string GetName(int ordinal) => _metas[ordinal].name;
    }
}