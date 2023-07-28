using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using TDengineDriver;

namespace TDengine.Data.Protocol.Native
{
    public class NativeRows : ITDengineRows
    {
        private IntPtr _result;
        private IntPtr _stmt;
        private IntPtr _current_row;
        private List<int> _current_length;
        private int _precision;
        private readonly bool _isUpdate;
        private readonly List<TDengineMeta> _metas;
        private readonly Encoding _encoding;
        private DateTime _timeZero;

        public NativeRows(IntPtr result)
        {
            _result = result;
            _isUpdate = TDengineDriver.TDengine.IsUpdateQuery(result);
            if (_isUpdate)
            {
                AffectRows = TDengineDriver.TDengine.AffectRows(result);
                return;
            }

            AffectRows = -1;
            _precision = TDengineDriver.TDengine.ResultPrecision(_result);
            _metas = TDengineDriver.TDengine.FetchFields(result);
            FieldCount = _metas.Count;
            _encoding = Encoding.UTF8;
            _timeZero = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        }

        public NativeRows(int affectedRows)
        {
            _isUpdate = true;
            AffectRows = affectedRows;
        }

        public NativeRows(IntPtr stmt, IntPtr result)
        {
            _result = result;
            _stmt = stmt;
            AffectRows = -1;
            _precision = TDengineDriver.TDengine.ResultPrecision(_result);
            _metas = TDengineDriver.TDengine.FetchFields(result);
            FieldCount = _metas.Count;
            _encoding = Encoding.UTF8;
            _timeZero = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        }

        public bool HasRows => _isUpdate == false;

        public int AffectRows { get; }

        public int FieldCount { get; }

        public void Dispose()
        {
            if (_result != IntPtr.Zero)
            {
                TDengineDriver.TDengine.FreeResult(_result);
                _result = IntPtr.Zero;
            }
            
            if (_stmt != IntPtr.Zero)
            {
                TDengineDriver.TDengine.StmtClose(_stmt);
                _stmt = IntPtr.Zero;
            }
        }

        public long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            if (!Util.IsVarData((TDengineDataType)_metas[ordinal].type))
            {
                throw new Exception("GetBytes cannot be used on non-character columns");
            }

            var maxLength = _current_length[ordinal];
            if (maxLength > length) maxLength = length;
            var tmp = new byte[maxLength];
            Marshal.Copy(Marshal.ReadIntPtr(_current_row + IntPtr.Size * ordinal), tmp, (int)dataOffset, maxLength);
            Array.Copy(tmp, buffer, maxLength);
            return maxLength;
        }

        public char GetChar(int ordinal)
        {
            if (!Util.IsVarData((TDengineDataType)_metas[ordinal].type))
            {
                throw new Exception("GetChar cannot be used on non-character columns");
            }

            var length = _current_length[ordinal];
            var byteArray = new byte[length];
            Marshal.Copy(_current_row + IntPtr.Size * ordinal, byteArray, 0, length);
            var data = _encoding.GetString(byteArray);
            return data[0];
        }

        public long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            if (!Util.IsVarData((TDengineDataType)_metas[ordinal].type))
            {
                throw new Exception("GetChar cannot be used on non-character columns");
            }

            var maxLength = _current_length[ordinal];
            if (maxLength > length) maxLength = length;
            var byteArray = new byte[maxLength];
            Marshal.Copy(_current_row + IntPtr.Size * ordinal, byteArray, (int)dataOffset, maxLength);
            var data = _encoding.GetString(byteArray);
            Array.Copy(data.ToCharArray(), buffer, maxLength);
            return maxLength;
        }

        public string GetDataTypeName(int ordinal) => _metas[ordinal].TypeName();

        public object GetValue(int ordinal)
        {
            var pdata = Marshal.ReadIntPtr(_current_row, IntPtr.Size * ordinal);
            if (pdata == IntPtr.Zero) return DBNull.Value;
            object data;
            var length = _current_length[ordinal];
            byte[] byteArray;
            switch ((TDengineDataType)_metas[ordinal].type)
            {
                case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                    data = Marshal.ReadByte(pdata) != 0;
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                    data = (sbyte)Marshal.ReadByte(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                    data = Marshal.ReadInt16(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_INT:
                    data = Marshal.ReadInt32(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                    data = Marshal.ReadInt64(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                    data = (float)Marshal.PtrToStructure(pdata, typeof(float));
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                    data = (double)Marshal.PtrToStructure(pdata, typeof(double));
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                    var ts = Marshal.ReadInt64(pdata);
                    switch ((Util.TSDB_TIME_PRECISION)_precision)
                    {
                        case Util.TSDB_TIME_PRECISION.TSDB_TIME_PRECISION_MILLI:
                            data = _timeZero.AddTicks(ts * 10000).ToLocalTime();
                            break;
                        case Util.TSDB_TIME_PRECISION.TSDB_TIME_PRECISION_MICRO:
                            data = _timeZero.AddTicks(ts * 10).ToLocalTime();
                            break;
                        case Util.TSDB_TIME_PRECISION.TSDB_TIME_PRECISION_NANO:
                            data = _timeZero.AddTicks(ts / 100).ToLocalTime();
                            break;
                        default:
                            throw new Exception($"TDengine unsupported precision {_precision}");
                    }

                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                    data = Marshal.ReadByte(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_USMALLINT:
                    data = (ushort)Marshal.ReadInt16(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UINT:
                    data = (uint)Marshal.ReadInt32(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                    data = (ulong)Marshal.ReadInt64(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                    byteArray = new byte[length];
                    Marshal.Copy(pdata, byteArray, 0, length);
                    data = byteArray;
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                    byteArray = new byte[length];
                    Marshal.Copy(pdata, byteArray, 0, length);
                    data = _encoding.GetString(byteArray);
                    break;
                default:
                    throw new Exception($"TDengine unsupported data type {_metas[ordinal].TypeName()}");
            }

            return data;
        }

        public int GetFieldSize(int ordinal) => _metas[ordinal].size;

        public Type GetFieldType(int ordinal) => Util.ScanType((TDengineDataType)_metas[ordinal].type);

        public string GetName(int ordinal) => _metas[ordinal].name;

        public int GetOrdinal(string name) => _metas.IndexOf(_metas.FirstOrDefault(m => m.name == name));

        public bool Read()
        {
            _current_row = TDengineDriver.TDengine.FetchRows(_result);
            if (_current_row == IntPtr.Zero) return false;
            FetchLengths();
            return true;
        }

        private void FetchLengths()
        {
            var lengthPtr = TDengineDriver.TDengine.FetchLengths(_result);
            _current_length = new List<int>(FieldCount);
            for (var i = 0; i < FieldCount; i++)
            {
                _current_length.Add(Marshal.ReadInt32(lengthPtr + sizeof(int) * i));
            }
        }
    }
}