using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using TDengineHelper;
using TDengineDriver;

namespace TDengineWS.Impl
{
    public static class LibTaosWS
    {
        // public const string LibName = "/root/git_space/test/taos-connector-rust/target/libtaosws/libtaosws.so";
        public const string LibName = "taosws";

        [DllImport(LibName, EntryPoint = "ws_enable_log", CallingConvention = CallingConvention.Cdecl)]
        static extern public void WSEnableLog();

        [DllImport(LibName, EntryPoint = "ws_connect_with_dsn", CallingConvention = CallingConvention.Cdecl)]
        static extern public IntPtr _WSConnectWithDSN(IntPtr dsn);
        static public IntPtr WSConnectWithDSN(string dsn)
        {
            UTF8PtrStruct uTF8PtrStruct = new UTF8PtrStruct(dsn);
            try
            {
                return _WSConnectWithDSN(uTF8PtrStruct.utf8Ptr);
            }
            finally
            {
                uTF8PtrStruct.UTF8FreePtr();
            }
        }

        [DllImport(LibName, EntryPoint = "ws_get_server_info", CallingConvention = CallingConvention.Cdecl)]
        static extern public string WSGetServerInfo(IntPtr wsTaos);

        [DllImport(LibName, EntryPoint = "ws_close", CallingConvention = CallingConvention.Cdecl)]
        static extern public void WSClose(IntPtr wsTaos);

        [DllImport(LibName, EntryPoint = "ws_query", CallingConvention = CallingConvention.Cdecl)]
        static extern private IntPtr _WSQuery(IntPtr wsTaos, IntPtr sql);

        static public IntPtr WSQuery(IntPtr wsTaos, string sql)
        {
            UTF8PtrStruct uTF8PtrStruct = new UTF8PtrStruct(sql);
            try
            {
                return _WSQuery(wsTaos, uTF8PtrStruct.utf8Ptr);
            }
            finally
            {
                uTF8PtrStruct.UTF8FreePtr();
            }
        }

        [DllImport(LibName, EntryPoint = "ws_stop_query", CallingConvention = CallingConvention.Cdecl)]
        static extern private void _WSStopQuery(IntPtr wsTaos, IntPtr sql);

        static public void WSStopQuery(IntPtr wsTaos, string sql)
        {
            UTF8PtrStruct uTF8PtrStruct = new UTF8PtrStruct(sql);
            try
            {
                _WSStopQuery(wsTaos, uTF8PtrStruct.utf8Ptr);
            }
            finally
            {
                uTF8PtrStruct.UTF8FreePtr();
            }
        }

        [DllImport(LibName, EntryPoint = "ws_query_timeout", CallingConvention = CallingConvention.Cdecl)]
        static extern private IntPtr _WSQueryTimeout(IntPtr wsTaos, IntPtr sql, uint seconds);

        static public IntPtr WSQueryTimeout(IntPtr wsTaos, string sql, uint seconds)
        {
            UTF8PtrStruct uTF8PtrStruct = new UTF8PtrStruct(sql);
            try
            {
                return _WSQueryTimeout(wsTaos, uTF8PtrStruct.utf8Ptr, seconds);
            }
            finally
            {
                uTF8PtrStruct.UTF8FreePtr();
            }
        }

        [DllImport(LibName, EntryPoint = "ws_take_timing", CallingConvention = CallingConvention.Cdecl)]
        static extern public Int64 WSTakeTiming(IntPtr wsRes);

        [DllImport(LibName, EntryPoint = "ws_errno", CallingConvention = CallingConvention.Cdecl)]
        static extern public int WSErrorNo(IntPtr wsRes);

        [DllImport(LibName, EntryPoint = "ws_errstr", CallingConvention = CallingConvention.Cdecl)]
        static extern public string WSErrorStr(IntPtr wsRes);

        [DllImport(LibName, EntryPoint = "ws_affected_rows", CallingConvention = CallingConvention.Cdecl)]
        static extern public Int32 WSAffectRows(IntPtr wsRes);

        [DllImport(LibName, EntryPoint = "ws_field_count", CallingConvention = CallingConvention.Cdecl)]
        static extern public Int32 WSFieldCount(IntPtr wsRes);

        [DllImport(LibName, EntryPoint = "ws_is_update_query", CallingConvention = CallingConvention.Cdecl)]
        static extern public byte ws_is_update_query(IntPtr wsRes);
        static public bool WSIsUpdateQuery(IntPtr wsRes)
        {
            return ws_is_update_query(wsRes) == 1;
        }

        [DllImport(LibName, EntryPoint = "ws_fetch_fields", CallingConvention = CallingConvention.Cdecl)]
        static extern private IntPtr ws_fetch_fields(IntPtr wsRes);

        static public List<TDengineMeta> WSGetFields(IntPtr wsRes)
        {
            List<TDengineMeta> metaList = new List<TDengineMeta>();
            if (wsRes == IntPtr.Zero)
            {
                return metaList;
            }

            int fieldCount = WSFieldCount(wsRes);
            IntPtr fieldsPtr = ws_fetch_fields(wsRes);

            for (int i = 0; i < fieldCount; ++i)
            {
                int offset = i * (int)TaosField.STRUCT_SIZE;
                TDengineMeta meta = new TDengineMeta();

                meta.name = Marshal.PtrToStringAnsi(fieldsPtr + offset);
                meta.type = Marshal.ReadByte(fieldsPtr + offset + (int)TaosField.TYPE_OFFSET);
                meta.size = Marshal.ReadInt16(fieldsPtr + offset + (int)TaosField.BYTES_OFFSET);

                metaList.Add(meta);
            }

            return metaList;
        }

        [DllImport(LibName, EntryPoint = "ws_fetch_block", CallingConvention = CallingConvention.Cdecl)]
        static extern private int WSFetchBlock(IntPtr wsRes, IntPtr pptr, IntPtr rowsPtr);

        static public List<object> WSGetData(IntPtr wsRes)
        {
            IntPtr rowsPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Int32)));
            IntPtr dataPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(IntPtr)));
            IntPtr typePtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(byte)));
            IntPtr lenPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(UInt32)));

            List<object> list = new List<object>();
            try
            {
                while (true)
                {
                    int code = LibTaosWS.WSFetchBlock(wsRes, dataPtr, rowsPtr);
                    int cols = WSFieldCount(wsRes);
                    int rows = Marshal.ReadInt32(rowsPtr);

                    if (code == 0)
                    {
                        if (rows == 0)
                        {
                            break;
                        }
                        for (int row = 0; row < rows; row++)
                        {
                            for (int col = 0; col < cols; col++)
                            {
                                IntPtr valPtr = WSGetValueInBlock(wsRes, row, col, typePtr, lenPtr);
                                int type = Marshal.ReadByte(typePtr);
                                int len = Marshal.ReadInt32(lenPtr);
                                if (valPtr == IntPtr.Zero)
                                {
                                    list.Add("NULL");
                                    continue;
                                }
                                else
                                {
                                    list.Add(Ptr2Data(valPtr, (TDengineDataType)type, len));
                                }
                            }
                        }
                    }
                    else
                    {
                        throw new Exception($"WS fetch block failed,reason:{WSErrorStr(wsRes)},code:{WSErrorNo(wsRes)}");
                    }
                }

                return list;
            }
            finally
            {
                Marshal.FreeHGlobal(typePtr);
                Marshal.FreeHGlobal(lenPtr);
                Marshal.FreeHGlobal(rowsPtr);
                Marshal.FreeHGlobal(dataPtr);
            }
        }

        [DllImport(LibName, EntryPoint = "ws_free_result", CallingConvention = CallingConvention.Cdecl)]
        static extern public void WSFreeResult(IntPtr wsRes);

        [DllImport(LibName, EntryPoint = "ws_result_precision", CallingConvention = CallingConvention.Cdecl)]
        static extern public Int32 WSResultPrecision(IntPtr wsRes);

        [DllImport(LibName, EntryPoint = "ws_get_value_in_block", CallingConvention = CallingConvention.Cdecl)]
        static extern public IntPtr WSGetValueInBlock(IntPtr wsRes, Int32 row, Int32 col, IntPtr tyPtr, IntPtr lenPtr);

        [DllImport(LibName, EntryPoint = "ws_timestamp_to_rfc3339", CallingConvention = CallingConvention.Cdecl)]
        static extern public void WSTimestampToRFC3339(IntPtr wsRes, Int32 row, Int32 col, IntPtr tyPtr, IntPtr lenPtr);

        // ======================== WS_STMT ============================
        [DllImport(LibName, EntryPoint = "ws_stmt_init", CallingConvention = CallingConvention.Cdecl)]
        static extern public IntPtr WSStmtInit(IntPtr wsTaos);

        [DllImport(LibName, EntryPoint = "ws_stmt_prepare", CallingConvention = CallingConvention.Cdecl)]
        static extern private int _ws_stmt_prepare(IntPtr wsStmt, IntPtr sql, UInt64 len);

        static public int WSStmtPrepare(IntPtr wsStmt, string sql)
        {
            UTF8PtrStruct uTF8PtrStruct = new UTF8PtrStruct(sql);
            try
            {
                return _ws_stmt_prepare(wsStmt, uTF8PtrStruct.utf8Ptr, (UInt64)uTF8PtrStruct.utf8StrLength);
            }
            finally
            {
                uTF8PtrStruct.UTF8FreePtr();
            }
        }

        [DllImport(LibName, EntryPoint = "ws_stmt_set_tbname", CallingConvention = CallingConvention.Cdecl)]
        static extern public int _WSStmtSetTbname(IntPtr wsStmt, IntPtr tableName);

        static public int WSStmtSetTbname(IntPtr wsStmt, string tableName)
        {
            UTF8PtrStruct uTF8PtrStruct = new UTF8PtrStruct(tableName);
            try
            {
                return _WSStmtSetTbname(wsStmt, uTF8PtrStruct.utf8Ptr);
            }
            finally
            {
                uTF8PtrStruct.UTF8FreePtr();
            }
        }

        [DllImport(LibName, EntryPoint = "ws_stmt_set_tbname_tags", CallingConvention = CallingConvention.Cdecl)]
        static extern private int _WSStmtSetTbnameTags(IntPtr wsStmt, IntPtr tableName, TAOS_MULTI_BIND[] wsMBinds, Int32 len);

        static public int WSStmtSetTbnameTags(IntPtr wsStmt, string tableName, TAOS_MULTI_BIND[] wsMBinds, Int32 len)
        {
            UTF8PtrStruct uTF8PtrStruct = new UTF8PtrStruct(tableName);
            try
            {

                return _WSStmtSetTbnameTags(wsStmt, uTF8PtrStruct.utf8Ptr, wsMBinds, len);

            }
            finally
            {
                uTF8PtrStruct.UTF8FreePtr();
            }

        }

        [DllImport(LibName, EntryPoint = "ws_stmt_is_insert", CallingConvention = CallingConvention.Cdecl)]
        static extern public int WSStmtIsInsert(IntPtr wsStmt, IntPtr isInsertPtr);

        [DllImport(LibName, EntryPoint = "ws_stmt_set_tags", CallingConvention = CallingConvention.Cdecl)]
        static extern public int WSStmtSetTags(IntPtr wsStmt, TAOS_MULTI_BIND[] wsMBind, Int32 len);

        [DllImport(LibName, EntryPoint = "ws_stmt_bind_param_batch", CallingConvention = CallingConvention.Cdecl)]
        static extern public int WSStmtBindParamBatch(IntPtr wsStmt, TAOS_MULTI_BIND[] wsMBind, Int32 len);

        [DllImport(LibName, EntryPoint = "ws_stmt_add_batch", CallingConvention = CallingConvention.Cdecl)]
        static extern public int WSStmtAddBatch(IntPtr wsStmt);

        [DllImport(LibName, EntryPoint = "ws_stmt_execute", CallingConvention = CallingConvention.Cdecl)]
        static extern public int WSStmtExecute(IntPtr wsStmt, IntPtr affectRowsPtr);

        [DllImport(LibName, EntryPoint = "ws_stmt_affected_rows", CallingConvention = CallingConvention.Cdecl)]
        static extern public int WSStmtAffectRows(IntPtr wsStmt);

        [DllImport(LibName, EntryPoint = "ws_stmt_errstr", CallingConvention = CallingConvention.Cdecl)]
        static extern public string WSStmtErrorStr(IntPtr wsStmt);

        [DllImport(LibName, EntryPoint = "ws_stmt_close", CallingConvention = CallingConvention.Cdecl)]
        static extern public void WSStmtClose(IntPtr wsStmt);


        static private object Ptr2Data(IntPtr dataPtr, TDengineDataType dengineDataType, int len)
        {
            object data;

            switch (dengineDataType)
            {
                case TDengineDataType.TSDB_DATA_TYPE_NULL:
                    data = "NULL";
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                    data = Marshal.ReadByte(dataPtr) == 1;
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                    data = (sbyte)Marshal.ReadByte(dataPtr);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                    data = Marshal.ReadInt16(dataPtr);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_INT:
                    data = Marshal.ReadInt32(dataPtr);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                    data = Marshal.ReadInt64(dataPtr);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                    data = Marshal.PtrToStructure(dataPtr, typeof(float));
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                    data = Marshal.PtrToStructure(dataPtr, typeof(double));
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                    data = Marshal.PtrToStringAnsi(dataPtr, len);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                    data = Marshal.ReadInt64(dataPtr);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                    data = Marshal.PtrToStringAnsi(dataPtr, len);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                    data = Marshal.ReadByte(dataPtr);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_USMALLINT:
                    data = (ushort)Marshal.ReadInt16(dataPtr);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UINT:
                    data = (uint)Marshal.ReadInt32(dataPtr);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                    data = (ulong)Marshal.ReadInt64(dataPtr);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                    data = Marshal.PtrToStringAnsi(dataPtr, len);
                    break;
                default:
                    throw new ArgumentException($"unsupported TDengine type {(int)dengineDataType}");

            }
            return data;

        }

    }
}
