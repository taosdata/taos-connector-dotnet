using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using TDengine.Driver.Impl.NativeMethods;

namespace TDengine.Driver.Client.Native
{
    public class NativeRows : IRows
    {
        private IntPtr _result;
        private int _currentRow;
        private readonly bool _isUpdate;
        private readonly List<TDengineMeta> _metas;
        private int _blockSize;
        private IntPtr _block = IntPtr.Zero;
        private bool _completed;
        private readonly BlockReader _blockReader;
        private bool _disableFreeResult;

        public NativeRows(int affectedRows)
        {
            _isUpdate = true;
            AffectRows = affectedRows;
        }

        public NativeRows(IntPtr result, TimeZoneInfo tz, bool disableFreeResult)
        {
            _disableFreeResult = disableFreeResult;
            AffectRows = -1;
            FieldCount = NativeMethods.FieldCount(result);

            _metas = NativeMethods.FetchFields(result);
            _result = result;
            var types = new byte[_metas.Count];
            for (int i = 0; i < _metas.Count; i++)
            {
                types[i] = _metas[i].type;
            }

            _blockReader = new BlockReader(0, FieldCount, NativeMethods.ResultPrecision(result), types, tz);
        }
        
        public void Dispose()
        {
            if (_disableFreeResult)
            {
                return;
            }
            if (_result != IntPtr.Zero)
            {
                NativeMethods.FreeResult(_result);
                _result = IntPtr.Zero;
            }
        }

        public bool HasRows => _isUpdate == false;
        public int AffectRows { get; }
        public int FieldCount { get; }

        public long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return _blockReader.GetBytes(_currentRow, ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public char GetChar(int ordinal)
        {
            return _blockReader.GetChar(_currentRow, ordinal);
        }

        public long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return _blockReader.GetChars(_currentRow, ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public string GetDataTypeName(int ordinal) => _metas[ordinal].TypeName();

        public object GetValue(int ordinal)
        {
            return _blockReader.Read(_currentRow, ordinal);
        }

        public Type GetFieldType(int ordinal) => _metas[ordinal].ScanType();

        public int GetFieldSize(int ordinal) => _metas[ordinal].size;

        public string GetName(int ordinal) => _metas[ordinal].name;

        public int GetOrdinal(string name) => _metas.FindIndex(m => m.name == name);

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

        private void FetchBlock()
        {
            IntPtr numOfRowsPrt = Marshal.AllocHGlobal(sizeof(Int32));
            IntPtr pDataPtr = Marshal.AllocHGlobal(IntPtr.Size);
            try
            {
                int code = NativeMethods.FetchRawBlock(_result, numOfRowsPrt, pDataPtr);
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
                    var dataPtr = Marshal.ReadIntPtr(pDataPtr);
                    _block = dataPtr;
                    _blockReader.SetBlockPtr(dataPtr, _blockSize);
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