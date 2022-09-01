using System;
using System.Runtime.InteropServices;

namespace TDengineDriver
{
    public enum TDengineDataType
    {
        TSDB_DATA_TYPE_NULL = 0,     // 1 bytes
        TSDB_DATA_TYPE_BOOL = 1,     // 1 bytes
        TSDB_DATA_TYPE_TINYINT = 2,  // 1 bytes
        TSDB_DATA_TYPE_SMALLINT = 3, // 2 bytes
        TSDB_DATA_TYPE_INT = 4,      // 4 bytes
        TSDB_DATA_TYPE_BIGINT = 5,   // 8 bytes
        TSDB_DATA_TYPE_FLOAT = 6,    // 4 bytes
        TSDB_DATA_TYPE_DOUBLE = 7,   // 8 bytes
        TSDB_DATA_TYPE_BINARY = 8,   // string
        TSDB_DATA_TYPE_TIMESTAMP = 9,// 8 bytes
        TSDB_DATA_TYPE_NCHAR = 10,   // unicode string
        TSDB_DATA_TYPE_UTINYINT = 11,// 1 byte
        TSDB_DATA_TYPE_USMALLINT = 12,// 2 bytes
        TSDB_DATA_TYPE_UINT = 13,    // 4 bytes
        TSDB_DATA_TYPE_UBIGINT = 14,   // 8 bytes
        TSDB_DATA_TYPE_JSONTAG = 15   //4096 bytes 
    }

    public enum TDengineInitOption
    {
        TSDB_OPTION_LOCALE = 0,
        TSDB_OPTION_CHARSET = 1,
        TSDB_OPTION_TIMEZONE = 2,
        TSDB_OPTION_CONFIGDIR = 3,
        TSDB_OPTION_SHELL_ACTIVITY_TIMER = 4
    }
    public enum TDengineSchemalessProtocol
    {
        TSDB_SML_UNKNOWN_PROTOCOL = 0,
        TSDB_SML_LINE_PROTOCOL = 1,
        TSDB_SML_TELNET_PROTOCOL = 2,
        TSDB_SML_JSON_PROTOCOL = 3

    }
    public enum TDengineSchemalessPrecision
    {
        TSDB_SML_TIMESTAMP_NOT_CONFIGURED = 0,
        TSDB_SML_TIMESTAMP_HOURS = 1,
        TSDB_SML_TIMESTAMP_MINUTES = 2,
        TSDB_SML_TIMESTAMP_SECONDS = 3,
        TSDB_SML_TIMESTAMP_MILLI_SECONDS = 4,
        TSDB_SML_TIMESTAMP_MICRO_SECONDS = 5,
        TSDB_SML_TIMESTAMP_NANO_SECONDS = 6
    }
    enum TaosField
    {
        STRUCT_SIZE = 72,
        NAME_LENGTH = 65,
        TYPE_OFFSET = 65,
        BYTES_OFFSET = 68,

    }
    public class TDengineMeta
    {
        public string name=string.Empty;
        public short size;
        public byte type;
        public string TypeName()
        {
            switch ((TDengineDataType)type)
            {
                case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                    return "BOOL";
                case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                    return "TINYINT";
                case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                    return "SMALLINT";
                case TDengineDataType.TSDB_DATA_TYPE_INT:
                    return "INT";
                case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                    return "BIGINT";
                case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                    return "TINYINT UNSIGNED";
                case TDengineDataType.TSDB_DATA_TYPE_USMALLINT:
                    return "SMALLINT UNSIGNED";
                case TDengineDataType.TSDB_DATA_TYPE_UINT:
                    return "INT UNSIGNED";
                case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                    return "BIGINT UNSIGNED";
                case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                    return "FLOAT";
                case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                    return "DOUBLE";
                case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                    return "BINARY";
                case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                    return "TIMESTAMP";
                case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                    return "NCHAR";
                case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                    return "JSON";
                default:
                    return "undefine";
            }
        }
    }

    public class StmtFields
    {
        internal string name { get; } = String.Empty;
        internal sbyte type { get; }
        internal byte preicision { get; }
        internal byte scale { get; }
        internal int bytes { get; }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct TAOS_MULTI_BIND
    {
        // column type
        public int buffer_type;

        // array, one or more lines column value
        public IntPtr buffer;

        //length of element in TAOS_MULTI_BIND.buffer (for binary and nchar it is the longest element's length)
        public ulong buffer_length;

        //array, actual data length for each value
        public IntPtr length;

        //array, indicates each column value is null or not
        public IntPtr is_null;

        // line number, or the values number in buffer 
        public int num;
    }

    /// <summary>
    /// User defined callback function for interface "QueryAsync()"
    /// ,actually is a delegate in .Net.
    /// This function aim to handle the taoRes which points to
    /// the caller method's sql resultset.
    /// </summary>
    /// <param name="param"> This parameter will sent by caller method (QueryAsync()).</param>
    /// <param name="taoRes"> This is the retrieved by caller method's sql.</param>
    /// <param name="code"> 0 for indicate operation success and negative for operation fail.</param>
    public delegate void QueryAsyncCallback(IntPtr param, IntPtr taoRes, int code);

    /// <summary>
    /// User defined callback function for interface "FetchRowAsync()"
    /// ,actually is a delegate in .Net.
    /// This callback allow applications to get each row of the
    /// batch records by calling FetchRowAsync() forward iteration.
    /// After reading all the records in a block, the application needs to continue calling 
    /// FetchRowAsync() in this callback function to obtain the next batch of records for 
    /// processing until the number of records
    /// </summary>
    /// <param name="param">The parameter passed by <see cref="FetchRowAsync"/></param>
    /// <param name="taoRes">Query status</param>
    /// <param name="numOfRows"> The number of rows of data obtained (not a function of
    /// the entire query result set). When the number is zero (the result is returned) 
    /// or the number of records is negative (the query fails).</param>
    public delegate void FetchRawBlockAsyncCallback(IntPtr param, IntPtr taoRes, int numOfRows);

    public enum TsdbServerStatus
    {
        TSDB_SRV_STATUS_UNAVAILABLE = 0,
        TSDB_SRV_STATUS_NETWORK_OK = 1,
        TSDB_SRV_STATUS_SERVICE_OK = 2,
        TSDB_SRV_STATUS_SERVICE_DEGRADED = 3,
        TSDB_SRV_STATUS_EXTING = 4,
    }
}
