using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using TDengine.Data.Client;
using TDengineDriver;

namespace TDengine.Data.Protocol.Native
{
    public class Util
    {
        private static readonly DateTime _timeZero = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static Type ScanType(TDengineDataType type)
        {
            switch (type)
            {
                case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                    return typeof(bool);
                case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                    return typeof(sbyte);
                case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                    return typeof(short);
                case TDengineDataType.TSDB_DATA_TYPE_INT:
                    return typeof(int);
                case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                    return typeof(long);
                case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                    return typeof(byte);
                case TDengineDataType.TSDB_DATA_TYPE_USMALLINT:
                    return typeof(ushort);
                case TDengineDataType.TSDB_DATA_TYPE_UINT:
                    return typeof(uint);
                case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                    return typeof(ulong);
                case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                    return typeof(float);
                case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                    return typeof(double);
                case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                    return typeof(byte[]);
                case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                    return typeof(DateTime);
                case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                    return typeof(string);
                case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                    return typeof(byte[]);
                default:
                    return typeof(DBNull);
            }
        }

        public enum TSDB_TIME_PRECISION : int
        {
            TSDB_TIME_PRECISION_MILLI = 0,
            TSDB_TIME_PRECISION_MICRO = 1,
            TSDB_TIME_PRECISION_NANO = 2
        }

        public static bool IsVarData(TDengineDataType type)
        {
            return type == TDengineDataType.TSDB_DATA_TYPE_BINARY || type == TDengineDataType.TSDB_DATA_TYPE_NCHAR || type == TDengineDataType.TSDB_DATA_TYPE_JSONTAG;
        }

        private enum fieldType
        {
            Col,
            Tag
        }

        public static IntPtr[] ParseParameters(TDengineParameterCollection parameters, TaosFieldE[] colFields,
            TaosFieldE[] tagFields, out List<TAOS_MULTI_BIND> data,
            out List<TAOS_MULTI_BIND> tags)
        {
            var colIndex = 0;
            var tagIndex = 0;
            data = new List<TAOS_MULTI_BIND>(colFields.Length);
            tags = new List<TAOS_MULTI_BIND>(tagFields.Length);
            List<IntPtr> needFreePointer = new List<IntPtr>();
            for (int i = 0; i < parameters.Count; i++)
            {
                var parameter = parameters[i];
                fieldType dataType;
                if (parameter.ParameterName.StartsWith("$"))
                {
                    dataType = fieldType.Tag;
                }
                else if (parameter.ParameterName.StartsWith("@"))
                {
                    dataType = fieldType.Col;
                }
                else
                {
                    continue;
                }

                TAOS_MULTI_BIND _bind = new TAOS_MULTI_BIND();
                _bind.num = 1;
                if (parameter.Value is null)
                {
                    var nullPtr = Marshal.AllocHGlobal(1);
                    needFreePointer.Add(nullPtr);
                    Marshal.WriteByte(nullPtr,1);
                    _bind.is_null = nullPtr;
                    switch (dataType)
                    {
                        case fieldType.Col:
                            _bind.buffer_type = colFields[colIndex].type;
                            colIndex += 1;
                            data.Add(_bind);
                            break;
                        case fieldType.Tag:
                            _bind.buffer_type = tagFields[tagIndex].type;
                            tagIndex += 1;
                            tags.Add(_bind);
                            break;
                    }
                }
                else
                {
                    IntPtr p;
                    byte[] bs;
                    int dbType;
                    IntPtr lPtr;
                    switch (parameter.Value)
                    {
                        // bool
                        case bool value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BOOL;
                            p = Marshal.AllocHGlobal(sizeof(bool));
                            needFreePointer.Add(p);
                            Marshal.WriteByte(p, Convert.ToByte(value));
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(bool);
                            break;
                        // int8
                        case sbyte value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT;
                            p = Marshal.AllocHGlobal(sizeof(sbyte));
                            needFreePointer.Add(p);
                            Marshal.WriteByte(p, BitConverter.GetBytes(value)[0]);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(sbyte);
                            break;
                        // int16
                        case short value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_SMALLINT;
                            p = Marshal.AllocHGlobal(sizeof(short));
                            needFreePointer.Add(p);
                            Marshal.WriteInt16(p, value);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(short);
                            break;
                        // int32
                        case int value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_INT;
                            p = Marshal.AllocHGlobal(sizeof(int));
                            needFreePointer.Add(p);
                            Marshal.WriteInt32(p, value);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(int);
                            break;
                        // int64
                        case long value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT;
                            p = Marshal.AllocHGlobal(sizeof(long));
                            needFreePointer.Add(p);
                            Marshal.WriteInt64(p, value);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(long);
                            break;
                        // uint8
                        case byte value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UTINYINT;
                            p = Marshal.AllocHGlobal(sizeof(byte));
                            needFreePointer.Add(p);
                            Marshal.WriteByte(p, value);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(byte);
                            break;
                        // uint16
                        case ushort value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_USMALLINT;
                            p = Marshal.AllocHGlobal(sizeof(ushort));
                            needFreePointer.Add(p);
                            bs = BitConverter.GetBytes(value);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(ushort);
                            break;
                        // uint32
                        case uint value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UINT;
                            p = Marshal.AllocHGlobal(sizeof(uint));
                            needFreePointer.Add(p);
                            bs = BitConverter.GetBytes(value);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(uint);
                            break;
                        // uint64
                        case ulong value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UBIGINT;
                            p = Marshal.AllocHGlobal(sizeof(ulong));
                            needFreePointer.Add(p);
                            bs = BitConverter.GetBytes(value);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(ulong);
                            break;
                        // float
                        case float value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT;
                            p = Marshal.AllocHGlobal(sizeof(float));
                            needFreePointer.Add(p);
                            bs = BitConverter.GetBytes(value);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(float);
                            break;
                        // double
                        case double value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE;
                            p = Marshal.AllocHGlobal(sizeof(double));
                            needFreePointer.Add(p);
                            bs = BitConverter.GetBytes(value);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(double);
                            break;
                        // timestamp
                        case DateTime value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP;
                            p = Marshal.AllocHGlobal(sizeof(long));
                            needFreePointer.Add(p);
                            var val = value.ToUniversalTime().Ticks - _timeZero.Ticks;
                            byte precision = 0;
                            switch (dataType)
                            {
                                case fieldType.Col:
                                    precision = colFields[colIndex].precision;
                                    break;
                                case fieldType.Tag:
                                    precision = tagFields[tagIndex].precision;
                                    break;
                            }

                            switch ((TSDB_TIME_PRECISION)precision)
                            {
                                case TSDB_TIME_PRECISION.TSDB_TIME_PRECISION_NANO:
                                    val *= 100;
                                    break;
                                case TSDB_TIME_PRECISION.TSDB_TIME_PRECISION_MICRO:
                                    val /= 10;
                                    break;
                                case TSDB_TIME_PRECISION.TSDB_TIME_PRECISION_MILLI:
                                    val /= 10000;
                                    break;
                            }

                            bs = BitConverter.GetBytes(val);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(long);
                            break;

                        // bytes
                        case byte[] value:
                            dbType = 0;
                            switch (dataType)
                            {
                                case fieldType.Col:
                                    dbType = colFields[colIndex].type;
                                    break;
                                case fieldType.Tag:
                                    dbType = tagFields[tagIndex].type;
                                    break;
                            }

                            _bind.buffer_type = dbType;
                            p = Marshal.AllocHGlobal(value.Length);
                            needFreePointer.Add(p);
                            Marshal.Copy(value, 0, p, value.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = (ulong)value.Length;
                            lPtr = Marshal.AllocHGlobal(sizeof(Int32));
                            needFreePointer.Add(lPtr);
                            Marshal.WriteInt32(lPtr, value.Length);
                            _bind.length = lPtr;
                            break;

                        // string
                        case string value:
                            dbType = 0;
                            switch (dataType)
                            {
                                case fieldType.Col:
                                    dbType = colFields[colIndex].type;
                                    break;
                                case fieldType.Tag:
                                    dbType = tagFields[tagIndex].type;
                                    break;
                            }

                            _bind.buffer_type = dbType;
                            bs = System.Text.Encoding.UTF8.GetBytes(value);
                            p = Marshal.AllocHGlobal(bs.Length);
                            needFreePointer.Add(p);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = (ulong)bs.Length;
                            lPtr = Marshal.AllocHGlobal(sizeof(Int32));
                            needFreePointer.Add(lPtr);
                            Marshal.WriteInt32(lPtr, bs.Length);
                            _bind.length = lPtr;
                            break;
                        default:
                            throw new NotSupportedException(
                                $"execute bind param not support type: {parameter.Value.GetType()}");
                    }
                    switch (dataType)
                    {
                        case fieldType.Col:
                            data.Add(_bind);
                            colIndex += 1;
                            break;
                        case fieldType.Tag:
                            tags.Add(_bind);
                            tagIndex += 1;
                            break;
                    }
                }
            }

            return needFreePointer.ToArray();
        }

        public static IntPtr[] ParseQueryParameters(TDengineParameterCollection parameters,
            out List<TAOS_MULTI_BIND> data)
        {
            data = new List<TAOS_MULTI_BIND>();
            List<IntPtr> needFreePointer = new List<IntPtr>();
            for (int i = 0; i < parameters.Count; i++)
            {
                var parameter = parameters[i];
                if (!parameter.ParameterName.StartsWith("@"))
                {
                    continue;
                }

                TAOS_MULTI_BIND _bind = new TAOS_MULTI_BIND();
                _bind.num = 1;
                if (parameter.Value is null)
                {
                    var nullPtr = Marshal.AllocHGlobal(sizeof(byte));
                    needFreePointer.Add(nullPtr);
                    Marshal.WriteByte(nullPtr,1);
                    _bind.is_null = nullPtr;
                    data.Add(_bind);
                }
                else
                {
                    IntPtr p;
                    byte[] bs;
                    IntPtr lPtr;
                    switch (parameter.Value)
                    {
                        // bool
                        case bool value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BOOL;
                            p = Marshal.AllocHGlobal(sizeof(bool));
                            needFreePointer.Add(p);
                            Marshal.WriteByte(p, Convert.ToByte(value));
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(bool);
                            break;
                        // int8
                        case sbyte value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT;
                            p = Marshal.AllocHGlobal(sizeof(sbyte));
                            needFreePointer.Add(p);
                            Marshal.WriteByte(p, BitConverter.GetBytes(value)[0]);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(sbyte);
                            break;
                        // int16
                        case short value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_SMALLINT;
                            p = Marshal.AllocHGlobal(sizeof(short));
                            needFreePointer.Add(p);
                            Marshal.WriteInt16(p, value);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(short);
                            break;
                        // int32
                        case int value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_INT;
                            p = Marshal.AllocHGlobal(sizeof(int));
                            needFreePointer.Add(p);
                            Marshal.WriteInt32(p, value);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(int);
                            break;
                        // int64
                        case long value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT;
                            p = Marshal.AllocHGlobal(sizeof(long));
                            needFreePointer.Add(p);
                            Marshal.WriteInt64(p, value);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(long);
                            break;
                        // uint8
                        case byte value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UTINYINT;
                            p = Marshal.AllocHGlobal(sizeof(byte));
                            needFreePointer.Add(p);
                            Marshal.WriteByte(p, value);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(byte);
                            break;
                        // uint16
                        case ushort value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_USMALLINT;
                            p = Marshal.AllocHGlobal(sizeof(ushort));
                            needFreePointer.Add(p);
                            bs = BitConverter.GetBytes(value);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(ushort);
                            break;
                        // uint32
                        case uint value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UINT;
                            p = Marshal.AllocHGlobal(sizeof(uint));
                            needFreePointer.Add(p);
                            bs = BitConverter.GetBytes(value);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(uint);
                            break;
                        // uint64
                        case ulong value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UBIGINT;
                            p = Marshal.AllocHGlobal(sizeof(ulong));
                            needFreePointer.Add(p);
                            bs = BitConverter.GetBytes(value);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(ulong);
                            break;
                        // float
                        case float value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT;
                            p = Marshal.AllocHGlobal(sizeof(float));
                            needFreePointer.Add(p);
                            bs = BitConverter.GetBytes(value);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(float);
                            break;
                        // double
                        case double value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE;
                            p = Marshal.AllocHGlobal(sizeof(double));
                            needFreePointer.Add(p);
                            bs = BitConverter.GetBytes(value);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = sizeof(double);
                            break;
                        // timestamp
                        case DateTime value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BINARY;
                            bs = System.Text.Encoding.UTF8.GetBytes(value.ToString("O"));
                            p = Marshal.AllocHGlobal(bs.Length);
                            needFreePointer.Add(p);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = (ulong)bs.Length;
                            lPtr = Marshal.AllocHGlobal(sizeof(Int32));
                            needFreePointer.Add(lPtr);
                            Marshal.WriteInt32(lPtr, bs.Length);
                            _bind.length = lPtr;
                            break;
                        // bytes
                        case byte[] value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BINARY;
                            p = Marshal.AllocHGlobal(value.Length);
                            needFreePointer.Add(p);
                            Marshal.Copy(value, 0, p, value.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = (ulong)value.Length;
                            lPtr = Marshal.AllocHGlobal(sizeof(Int32));
                            needFreePointer.Add(lPtr);
                            Marshal.WriteInt32(lPtr, value.Length);
                            _bind.length = lPtr;
                            break;

                        // string
                        case string value:
                            _bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BINARY;
                            bs = System.Text.Encoding.UTF8.GetBytes(value);
                            p = Marshal.AllocHGlobal(bs.Length);
                            needFreePointer.Add(p);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            _bind.buffer = p;
                            _bind.buffer_length = (ulong)bs.Length;
                            lPtr = Marshal.AllocHGlobal(sizeof(Int32));
                            needFreePointer.Add(lPtr);
                            Marshal.WriteInt32(lPtr, bs.Length);
                            _bind.length = lPtr;
                            break;
                        default:
                            throw new NotSupportedException(
                                $"query bind param not support type: {parameter.Value.GetType()}");
                    }
                }

                data.Add(_bind);
            }

            return needFreePointer.ToArray();
        }

        public static string StmtParseTableName(TDengineParameterCollection parameters)
        {
            for (int i = 0; i < parameters.Count; i++)
            {
                var parameter = parameters[i];
                if (parameter.ParameterName.StartsWith("#"))
                {
                    return parameter.Value as string;
                }
            }

            return string.Empty;
        }
    }
}