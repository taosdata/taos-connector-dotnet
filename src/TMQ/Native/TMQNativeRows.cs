using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using TDengine.Driver;
using TDengine.Driver.Impl.NativeMethods;
using TDengineHelper;

namespace TDengine.TMQ.Native
{
    public class TMQNativeRows : ITMQRows
    {
        private IntPtr _result;
        private int _currentRow;
        private List<TDengineMeta> _metas;
        private int _blockSize;
        private IntPtr _block = IntPtr.Zero;
        private bool _completed;
        private BlockReader _blockReader;

        public TMQNativeRows(IntPtr result, TimeZoneInfo tz)
        {
            _result = result;
            _blockReader = new BlockReader(0, tz);
        }

        public object GetValue(int ordinal)
        {
            return _blockReader.Read(_currentRow, ordinal);
        }

        public bool Read()
        {
            if (_completed) return false;
            if (_block == IntPtr.Zero)
            {
                FetchBlock();
                return !_completed;
            }

            _currentRow += 1;
            if (_currentRow != _blockSize) return true;
            FetchBlock();
            return !_completed;
        }

        public int FieldCount { get; private set; }
        public string TableName { get; private set; }

        public string GetName(int ordinal) => _metas[ordinal].name;

        private void FetchBlock()
        {
            IntPtr numOfRowsPrt = Marshal.AllocHGlobal(sizeof(Int32));
            IntPtr pDataPtr = Marshal.AllocHGlobal(IntPtr.Size);
            try
            {
                var code = NativeMethods.FetchRawBlock(_result, numOfRowsPrt, pDataPtr);
                if (code != 0)
                {
                    throw new TDengineError(code, NativeMethods.Error(_result));
                }

                int numOfRows = Marshal.ReadInt32(numOfRowsPrt);
                if (numOfRows == 0)
                {
                    _completed = true;
                }
                else if (numOfRows < 0)
                {
                    throw new TDengineError(NativeMethods.ErrorNo(_result), NativeMethods.Error(_result));
                }
                else
                {
                    _blockSize = numOfRows;
                    _currentRow = 0;
                    FieldCount = NativeMethods.FieldCount(_result);
                    var tableName = NativeMethods.TmqGetTableName(_result);
                    TableName = StringHelper.PtrToStringUTF8(tableName);
                    _metas = NativeMethods.FetchFields(_result);
                    var dataPtr = Marshal.ReadIntPtr(pDataPtr);
                    var precision = NativeMethods.ResultPrecision(_result);
                    _block = dataPtr;
                    _blockReader.SetTMQBlock(dataPtr, precision);
                }
            }
            finally
            {
                Marshal.FreeHGlobal(numOfRowsPrt);
                Marshal.FreeHGlobal(pDataPtr);
            }
        }
    }
}