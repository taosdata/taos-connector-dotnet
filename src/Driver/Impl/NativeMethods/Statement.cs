using System;
using System.Runtime.InteropServices;
using TDengineHelper;

namespace TDengine.Driver.Impl.NativeMethods
{
    public static partial class NativeMethods
    {
        // ================================ stmt ==========================
        /// <summary>
        /// init a TAOS_STMT object for later use.
        /// </summary>
        /// <param name=DLLName>a valid taos connection</param>
        /// <returns>
        /// Not NULL returned for success, NULL for failure. And it should be freed with taos_stmt_close.
        /// </returns>
        [DllImport(DLLName, EntryPoint = "taos_stmt_init", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr StmtInit(IntPtr taos);
        // TAOS_STMT *taos_stmt_init(TAOS *taos);

        // DLL_EXPORT TAOS_STMT *taos_stmt_init_with_reqid(TAOS *taos, int64_t reqid);
        [DllImport(DLLName, EntryPoint = "taos_stmt_init_with_reqid", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr StmtInitWithReqid(IntPtr taos, long reqid);

        /// <summary>
        /// prepare a sql statement，'sql' should be a valid INSERT/SELECT statement.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <param name="sql">sql string,used to bind parameters with</param>
        /// <param name="length">no used</param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport(DLLName, EntryPoint = "taos_stmt_prepare", CallingConvention = CallingConvention.Cdecl)]
        private static extern int _StmtPrepare(IntPtr stmt, IntPtr sql, ulong length);

        public static int StmtPrepare(IntPtr stmt, string sql)
        {
            UTF8PtrStruct _ = new UTF8PtrStruct(sql);
            int code = _StmtPrepare(stmt, _.utf8Ptr, (ulong)_.utf8StrLength);
            _.UTF8FreePtr();
            return code;
        }
        // int taos_stmt_prepare(TAOS_STMT* stmt, const char* sql, unsigned long length);

        /// <summary>
        /// For INSERT only.
        /// set a table name for binding table name as parameter and tag values for all  tag parameters.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <param name="name">use to set table name</param>
        /// <param name="tags">
        /// is an array contains all tag values,each item in the array represents a tag column's value.
        ///  the item number and sequence should keep consistence with that in stable tag definition.
        /// </param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport(DLLName, EntryPoint = "taos_stmt_set_tbname_tags", CallingConvention = CallingConvention.Cdecl)]
        private static extern int StmtSetTbnameTags(IntPtr stmt, IntPtr name, TAOS_MULTI_BIND[] tags);

        //int taos_stmt_set_tbname_tags(TAOS_STMT* stmt, const char* name, TAOS_MULTI_BIND *tags);
        public static int StmtSetTbnameTags(IntPtr stmt, string name, TAOS_MULTI_BIND[] tags)
        {
            UTF8PtrStruct utf8PtrStruct = new UTF8PtrStruct(name);
            int code = StmtSetTbnameTags(stmt, utf8PtrStruct.utf8Ptr, tags);
            utf8PtrStruct.UTF8FreePtr();

            return code;
        }


        /// <summary>
        /// For INSERT only. Used to bind table name as a parameter for the input stmt object.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <param name="name">table name you want to  bind</param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport(DLLName, EntryPoint = "taos_stmt_set_tbname", CallingConvention = CallingConvention.Cdecl)]
        private static extern int _StmtSetTbname(IntPtr stmt, IntPtr name);

        //int  taos_stmt_set_tbname(TAOS_STMT *stmt, const char *name);
        public static int StmtSetTbname(IntPtr stmt, string tableName)
        {
            UTF8PtrStruct utf8PtrStruct = new UTF8PtrStruct(tableName);
            try
            {
                return _StmtSetTbname(stmt, utf8PtrStruct.utf8Ptr);
            }
            finally
            {
                utf8PtrStruct.UTF8FreePtr();
            }
        }

        [DllImport(DLLName, EntryPoint = "taos_stmt_set_tags", CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtSetTags(IntPtr stmt, TAOS_MULTI_BIND[] tags);
        //int taos_stmt_set_tags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags);

        /// <summary>
        /// For INSERT only.
        /// Set a table name for binding table name as parameter. Only used for binding all tables
        /// in one stable, user application must call 'loadTableInfo' API to load all table
        /// meta before calling this API. If the table meta is not cached locally, it will return error.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <param name="name">table name which is belong to an stable</param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport(DLLName, EntryPoint = "taos_stmt_set_sub_tbname", CallingConvention = CallingConvention.Cdecl)]
        public static extern int _StmtSetSubTbname(IntPtr stmt, IntPtr name);

        //int taos_stmt_set_sub_tbname(TAOS_STMT *stmt, const char *name);
        public static int StmtSetSubTbname(IntPtr stmt, string chileTableName)
        {
            UTF8PtrStruct utf8PtrStruct = new UTF8PtrStruct(chileTableName);
            try
            {
                return _StmtSetSubTbname(stmt, utf8PtrStruct.utf8Ptr);
            }
            finally
            {
                utf8PtrStruct.UTF8FreePtr();
            }
        }

        [DllImport(DLLName, EntryPoint = "taos_stmt_get_tag_fields", CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtGetTagFields(IntPtr stmt, out int fieldNum, out IntPtr fields);
        //int taos_stmt_get_tag_fields(TAOS_STMT* stmt, int* fieldNum, TAOS_FIELD_E** fields);

        [DllImport(DLLName, EntryPoint = "taos_stmt_get_col_fields", CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtGetColFields(IntPtr stmt, out int fieldNum, out IntPtr fields);
        //int taos_stmt_get_col_fields(TAOS_STMT* stmt, int* fieldNum, TAOS_FIELD_E** fields);

        [DllImport(DLLName, EntryPoint = "taos_stmt_is_insert", CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtIsInsert(IntPtr stmt, IntPtr insert);
        //int taos_stmt_is_insert(TAOS_STMT* stmt, int* insert);

        [DllImport(DLLName, EntryPoint = "taos_stmt_reclaim_fields", CallingConvention = CallingConvention.Cdecl)]
        public static extern void StmtReclaimFields(IntPtr stmt, IntPtr fields);
        //  void taos_stmt_reclaim_fields(TAOS_STMT *stmt, TAOS_FIELD_E *fields);


        [DllImport(DLLName, EntryPoint = "taos_stmt_num_params", CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtNumParams(IntPtr stmt, IntPtr num);
        //int taos_stmt_num_params(TAOS_STMT *stmt, int *nums);

        [DllImport(DLLName, EntryPoint = "taos_stmt_get_param", CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtGetParam(IntPtr stmt, int index, IntPtr type, IntPtr bytes);
        //int taos_stmt_get_param(TAOS_STMT* stmt, int idx, int* type, int* bytes);

        /// <summary>
        /// for INSERT only
        /// bind one or multiple lines data. The parameter 'bind'
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <param name="bind">
        /// points to an array contains one or more lines data.Each item in array represents a column's value(s),
        /// the item number and sequence should keep consistence with columns in sql statement.
        /// </param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport(DLLName, EntryPoint = "taos_stmt_bind_param_batch", CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtBindParamBatch(IntPtr stmt, TAOS_MULTI_BIND[] bind);
        //int taos_stmt_bind_param_batch(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind);

        [DllImport(DLLName, EntryPoint = "taos_stmt_bind_param", CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtBindParam(IntPtr stmt, TAOS_MULTI_BIND[] bind);
        //int taos_stmt_bind_param(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind);

        /// <summary>
        /// bind a single column's data, INTERNAL used and for INSERT only.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <param name="bind">points to a column's data which could be the one or more lines. </param>
        /// <param name="colIdx">the column's index in prepared sql statement, it starts from 0.</param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport(DLLName, EntryPoint = "taos_stmt_bind_single_param_batch",
            CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtBindSingleParamBatch(IntPtr stmt,TAOS_MULTI_BIND bind, int colIdx);
        //int taos_stmt_bind_single_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx);

        /// <summary>
        /// For INSERT only.
        /// add all current bound parameters to batch process. Must be called after each call to
        /// StmtBindParam/StmtBindSingleParamBatch, or all columns binds for one or more lines
        /// with StmtBindSingleParamBatch. User application can call any bind parameter
        /// API again to bind more data lines after calling to this API.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport(DLLName, EntryPoint = "taos_stmt_add_batch", CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtAddBatch(IntPtr stmt);
        //int taos_stmt_add_batch(TAOS_STMT *stmt);

        /// <summary>
        /// actually execute the INSERT/SELECT sql statement.
        /// User application can continue to bind new data after calling to this API.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <returns></returns>
        [DllImport(DLLName, EntryPoint = "taos_stmt_execute", CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtExecute(IntPtr stmt);
        //int taos_stmt_execute(TAOS_STMT *stmt);

        /// <summary>
        /// For SELECT only,getting the query result. User application should free it with API 'FreeResult' at the end.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <returns>Not NULL for success, NULL for failure.</returns>
        [DllImport(DLLName, EntryPoint = "taos_stmt_use_result", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr StmtUseResult(IntPtr stmt);
        // TAOS_RES *taos_stmt_use_result(TAOS_STMT *stmt);

        /// <summary>
        /// close STMT object and free resources.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <returns>0 for success, non-zero for failure.</returns>
        [DllImport(DLLName, EntryPoint = "taos_stmt_close", CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtClose(IntPtr stmt);
        //int taos_stmt_close(TAOS_STMT *stmt);

        /// <summary>
        /// get detail error message when got failure for any stmt API call. If not failure, the result
        /// returned in this API is unknown.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <returns>point the error message</returns>
        [DllImport(DLLName, EntryPoint = "taos_stmt_errstr", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr StmtErrPtr(IntPtr stmt);
        // char* taos_stmt_errstr(TAOS_STMT* stmt);

        /// <summary>
        /// get detail error message when got failure for any stmt API call. If not failure, the result
        /// returned in this API is unknown.
        /// </summary>
        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
        /// <returns>error string</returns>
        public static string StmtErrorStr(IntPtr stmt)
        {
            IntPtr stmtErrPrt = StmtErrPtr(stmt);
            return StringHelper.PtrToStringUTF8(stmtErrPrt);
        }

        [DllImport(DLLName, EntryPoint = "taos_stmt_affected_rows", CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtAffectRows(IntPtr stmt);
        //int taos_stmt_affected_rows(TAOS_STMT* stmt);

        [DllImport(DLLName, EntryPoint = "taos_stmt_affected_rows_once", CallingConvention = CallingConvention.Cdecl)]
        public static extern int StmtAffetcedRowsOnce(IntPtr stmt);
        //int taos_stmt_affected_rows_once(TAOS_STMT* stmt);
    }
}