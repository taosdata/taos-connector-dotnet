
//using System;
//using System.Collections.Generic;
//using System.Runtime.InteropServices;

//namespace TDengineDriver
//{
//    public class TDengine
//    {
//        public const int TSDB_CODE_SUCCESS = 0;
//        public const string DLLName = "taos";

//        [DllImport(DLLName, EntryPoint = "taos_cleanup", CallingConvention = CallingConvention.Cdecl)]
//        static extern public void Cleanup();

//        [DllImport(DLLName, EntryPoint = "taos_options", CallingConvention = CallingConvention.Cdecl)]
//        static extern public void Options(int option, string value);

//        [DllImport(DLLName, EntryPoint = "taos_connect", CallingConvention = CallingConvention.Cdecl)]
//        static extern public IntPtr Connect(string ip, string user, string password, string db, short port);

//        [DllImport(DLLName, EntryPoint = "taos_errstr", CallingConvention = CallingConvention.Cdecl)]
//        static extern private IntPtr taos_errstr(IntPtr res);
//        static public string Error(IntPtr res)
//        {
//            IntPtr errPtr = taos_errstr(res);
//            return Marshal.PtrToStringAnsi(errPtr);
//        }

//        [DllImport(DLLName, EntryPoint = "taos_errno", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int ErrorNo(IntPtr res);

//        [DllImport(DLLName, EntryPoint = "taos_query", CallingConvention = CallingConvention.Cdecl)]
//        // static extern public IntPtr Query(IntPtr conn, string sqlstr);
//        static extern private IntPtr Query(IntPtr conn, IntPtr byteArr);

//        static public IntPtr Query(IntPtr conn, string command)
//        {
//            IntPtr res = IntPtr.Zero;

//            IntPtr commandBuffer = Marshal.StringToCoTaskMemUTF8(command);
//            res = Query(conn, commandBuffer);
//            Marshal.FreeCoTaskMem(commandBuffer);
//            return res;
//        }

//        [DllImport(DLLName, EntryPoint = "taos_affected_rows", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int AffectRows(IntPtr res);

//        [DllImport(DLLName, EntryPoint = "taos_field_count", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int FieldCount(IntPtr res);

//        [DllImport(DLLName, EntryPoint = "taos_fetch_fields", CallingConvention = CallingConvention.Cdecl)]
//        static extern private IntPtr taos_fetch_fields(IntPtr res);
//        static public List<TDengineMeta> FetchFields(IntPtr res)
//        {

//            List<TDengineMeta> metaList = new List<TDengineMeta>();
//            if (res == IntPtr.Zero)
//            {
//                return metaList;
//            }

//            int fieldCount = FieldCount(res);
//            IntPtr fieldsPtr = taos_fetch_fields(res);

//            for (int i = 0; i < fieldCount; ++i)
//            {
//                int offset = i * (int)TaosField.STRUCT_SIZE;
//                // Console.WriteLine("offset:{0}",offset);
//                TDengineMeta meta = new TDengineMeta();
//                meta.name = Marshal.PtrToStringAnsi(fieldsPtr + offset, (int)TaosField.NAME_LENGTH);
//                // Console.WriteLine("fetchFeilds().name:{0}",meta.name);
//                meta.type = Marshal.ReadByte(fieldsPtr + offset + (int)TaosField.TYPE_OFFSET);
//                // Console.WriteLine("fetchFeilds().type:{0}",meta.type);
//                meta.size = Marshal.ReadInt16(fieldsPtr + offset + (int)TaosField.BYTES_OFFSET);
//                // Console.WriteLine("fetchFeilds().size:{0}",meta.size);
//                metaList.Add(meta);
//            }

//            return metaList;
//        }

//        [DllImport(DLLName, EntryPoint = "taos_fetch_row", CallingConvention = CallingConvention.Cdecl)]
//        static extern public IntPtr FetchRows(IntPtr res);

//        [DllImport(DLLName, EntryPoint = "taos_free_result", CallingConvention = CallingConvention.Cdecl)]
//        static extern public void FreeResult(IntPtr res);

//        [DllImport(DLLName, EntryPoint = "taos_close", CallingConvention = CallingConvention.Cdecl)]
//        static extern public void Close(IntPtr taos);

//        //get precision of restultset
//        [DllImport(DLLName, EntryPoint = "taos_result_precision", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int ResultPrecision(IntPtr taos);

//        /// <summary>
//        /// user application must call this API to  load all tables meta,
//        /// </summary>
//        /// <param name=DLLName>taos connection</param>
//        /// <param name="tableList">table name List</param>
//        /// <returns></returns>
//        [DllImport(DLLName, EntryPoint = "taos_load_table_info", CallingConvention = CallingConvention.Cdecl)]
//        static extern private int LoadTableInfoDll(IntPtr taos, string tableList);

//        /// <summary>
//        /// user application  call this API to load all tables meta,this method call the native
//        /// method LoadTableInfoDll.
//        /// this method must be called before StmtSetSubTbname(IntPtr stmt, string name);
//        /// </summary>
//        /// <param name=DLLName>taos connection</param>
//        /// <param name="tableList">tables need to load meta info are form in an array</param>
//        /// <returns></returns>
//        static public int LoadTableInfo(IntPtr taos, string[] tableList)
//        {
//            string listStr = string.Join(",", tableList);
//            return LoadTableInfoDll(taos, listStr);
//        }

//        [DllImport(DLLName, EntryPoint = "taos_fetch_lengths", CallingConvention = CallingConvention.Cdecl)]
//        static extern public IntPtr FetchLengths(IntPtr taos);


//        //================================ schemaless ====================
//        [DllImport(DLLName, SetLastError = true, EntryPoint = "taos_schemaless_insert", CallingConvention = CallingConvention.Cdecl)]
//        static extern public IntPtr SchemalessInsert(IntPtr taos, string[] lines, int numLines, int protocol, int precision);

//        // ================================ stmt ==========================
//        /// <summary>
//        /// init a TAOS_STMT object for later use.
//        /// </summary>
//        /// <param name=DLLName>a valid taos connection</param>
//        /// <returns>
//        /// Not NULL returned for success, NULL for failure. And it should be freed with taos_stmt_close. 
//        /// </returns>
//        [DllImport(DLLName, EntryPoint = "taos_stmt_init", CallingConvention = CallingConvention.Cdecl)]
//        static extern public IntPtr StmtInit(IntPtr taos);
//        // TAOS_STMT *taos_stmt_init(TAOS *taos);

//        /// <summary>
//        /// prepare a sql statement，'sql' should be a valid INSERT/SELECT statement.
//        /// </summary>
//        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
//        /// <param name="sql">sql string,used to bind parameters with</param>
//        /// <param name="length">no used</param>
//        /// <returns>0 for success, non-zero for failure.</returns>
//        [DllImport(DLLName, EntryPoint = "taos_stmt_prepare", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtPrepare(IntPtr stmt, string sql, ulong length);
//        // int taos_stmt_prepare(TAOS_STMT* stmt, const char* sql, unsigned long length);

//        /// <summary>
//        /// For INSERT only.
//        /// set a table name for binding table name as parameter and tag values for all  tag parameters. 
//        /// </summary>
//        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
//        /// <param name="name">use to set table name</param>
//        /// <param name="tags">
//        /// is an array contains all tag values,each item in the array represents a tag column's value.
//        ///  the item number and sequence should keep consistence with that in stable tag definition.
//        /// </param>
//        /// <returns>0 for success, non-zero for failure.</returns>
//        [DllImport(DLLName, EntryPoint = "taos_stmt_set_tbname_tags", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtSetTbnameTags(IntPtr stmt, string name, TAOS_MULTI_BIND[] tags);
//        //int taos_stmt_set_tbname_tags(TAOS_STMT* stmt, const char* name, TAOS_MULTI_BIND *tags);

//        /// <summary>
//        /// For INSERT only. Used to bind table name as a parmeter for the input stmt object.
//        /// </summary>
//        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
//        /// <param name="name">table name you want to  bind</param>
//        /// <returns>0 for success, non-zero for failure.</returns>
//        [DllImport(DLLName, EntryPoint = "taos_stmt_set_tbname", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtSetTbname(IntPtr stmt, string name);
//        //int  taos_stmt_set_tbname(TAOS_STMT *stmt, const char *name);

//        [DllImport(DLLName, EntryPoint = "taos_stmt_set_tags", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtSetTags(IntPtr stmt, ref TAOS_MULTI_BIND tags);
//        //int taos_stmt_set_tags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags);

//        /// <summary>
//        /// For INSERT only. 
//        /// Set a table name for binding table name as parameter. Only used for binding all tables 
//        /// in one stable, user application must call 'loadTableInfo' API to load all table 
//        /// meta before calling this API. If the table meta is not cached locally, it will return error.
//        /// </summary>
//        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
//        /// <param name="name">table name which is belong to an stable</param>
//        /// <returns>0 for success, non-zero for failure.</returns>
//        [DllImport(DLLName, EntryPoint = "taos_stmt_set_sub_tbname", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtSetSubTbname(IntPtr stmt, string name);
//        //int taos_stmt_set_sub_tbname(TAOS_STMT *stmt, const char *name);

//        [DllImport(DLLName, EntryPoint = "taos_stmt_get_tag_fields", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtGetTagFields(IntPtr stmt, IntPtr fieldNum, IntPtr fields);
//        //int taos_stmt_get_tag_fields(TAOS_STMT* stmt, int* fieldNum, TAOS_FIELD_E** fields);

//        [DllImport(DLLName, EntryPoint = "taos_stmt_get_col_fields", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtGetColFields(IntPtr stmt, IntPtr fieldNum, IntPtr fields);
//        //int taos_stmt_get_col_fields(TAOS_STMT* stmt, int* fieldNum, TAOS_FIELD_E** fields);

//        [DllImport(DLLName, EntryPoint = "taos_stmt_is_insert", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtIsInsert(IntPtr stmt, IntPtr insert);
//        //int taos_stmt_is_insert(TAOS_STMT* stmt, int* insert);

//        [DllImport(DLLName, EntryPoint = "taos_stmt_num_params", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtNumParams(IntPtr stmt, IntPtr num);
//        //int taos_stmt_num_params(TAOS_STMT *stmt, int *nums);

//        [DllImport(DLLName, EntryPoint = "taos_stmt_get_param", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtGetParam(IntPtr stmt, int index, IntPtr type, IntPtr bytes);
//        //int taos_stmt_get_param(TAOS_STMT* stmt, int idx, int* type, int* bytes);

//        /// <summary>
//        /// for INSERT only
//        /// bind one or multiple lines data. The parameter 'bind'  
//        /// </summary>
//        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
//        /// <param name="bind">
//        /// points to an array contains one or more lines data.Each item in array represents a column's value(s),
//        /// the item number and sequence should keep consistence with columns in sql statement. 
//        /// </param>
//        /// <returns>0 for success, non-zero for failure.</returns>
//        [DllImport(DLLName, EntryPoint = "taos_stmt_bind_param_batch", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtBindParamBatch(IntPtr stmt, TAOS_MULTI_BIND[] bind);
//        //int taos_stmt_bind_param_batch(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind);

//        /// <summary>
//        /// bind a single column's data, INTERNAL used and for INSERT only. 
//        /// </summary>
//        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
//        /// <param name="bind">points to a column's data which could be the one or more lines. </param>
//        /// <param name="colIdx">the column's index in prepared sql statement, it starts from 0.</param>
//        /// <returns>0 for success, non-zero for failure.</returns>
//        [DllImport(DLLName, EntryPoint = "taos_stmt_bind_single_param_batch", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtBindSingleParamBatch(IntPtr stmt, ref TAOS_MULTI_BIND bind, int colIdx);
//        //int taos_stmt_bind_single_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx);

//        /// <summary>
//        /// For INSERT only.
//        /// add all current bound parameters to batch process. Must be called after each call to 
//        /// StmtBindParam/StmtBindSingleParamBatch, or all columns binds for one or more lines 
//        /// with StmtBindSingleParamBatch. User application can call any bind parameter 
//        /// API again to bind more data lines after calling to this API.
//        /// </summary>
//        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
//        /// <returns>0 for success, non-zero for failure.</returns>
//        [DllImport(DLLName, EntryPoint = "taos_stmt_add_batch", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtAddBatch(IntPtr stmt);
//        //int taos_stmt_add_batch(TAOS_STMT *stmt);

//        /// <summary>
//        /// actually execute the INSERT/SELECT sql statement. 
//        /// User application can continue to bind new data after calling to this API.
//        /// </summary>
//        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
//        /// <returns></returns>
//        [DllImport(DLLName, EntryPoint = "taos_stmt_execute", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtExecute(IntPtr stmt);
//        //int taos_stmt_execute(TAOS_STMT *stmt);

//        /// <summary>
//        /// For SELECT only,getting the query result. User application should free it with API 'FreeResult' at the end.
//        /// </summary>
//        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
//        /// <returns>Not NULL for success, NULL for failure.</returns>
//        [DllImport(DLLName, EntryPoint = "taos_stmt_use_result", CallingConvention = CallingConvention.Cdecl)]
//        static extern public IntPtr StmtUseResult(IntPtr stmt);
//        // TAOS_RES *taos_stmt_use_result(TAOS_STMT *stmt);

//        /// <summary>
//        /// close STMT object and free resources.
//        /// </summary>
//        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
//        /// <returns>0 for success, non-zero for failure.</returns>
//        [DllImport(DLLName, EntryPoint = "taos_stmt_close", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtClose(IntPtr stmt);
//        //int taos_stmt_close(TAOS_STMT *stmt);

//        /// <summary>
//        /// get detail error message when got failure for any stmt API call. If not failure, the result 
//        /// returned in this API is unknown.
//        /// </summary>
//        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
//        /// <returns>point the error message</returns>
//        [DllImport(DLLName, EntryPoint = "taos_stmt_errstr", CallingConvention = CallingConvention.Cdecl)]
//        static extern private IntPtr StmtErrPtr(IntPtr stmt);
//        // char* taos_stmt_errstr(TAOS_STMT* stmt);

//        /// <summary>
//        /// get detail error message when got failure for any stmt API call. If not failure, the result 
//        /// returned in this API is unknown.
//        /// </summary>
//        /// <param name="stmt">could be the value returned by 'StmtInit', that may be a valid object or NULL.</param>
//        /// <returns>error string</returns>
//        static public string StmtErrorStr(IntPtr stmt)
//        {
//            IntPtr stmtErrPrt = StmtErrPtr(stmt);
//            return Marshal.PtrToStringUTF8(stmtErrPrt);
//        }

//        [DllImport(DLLName, EntryPoint = "taos_stmt_affected_rows", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtAffectRows(IntPtr stmt);
//        //int taos_stmt_affected_rows(TAOS_STMT* stmt);

//        [DllImport(DLLName, EntryPoint = "taos_stmt_affected_rows_once", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int StmtAffetcedRowsOnce(IntPtr stmt);
//        //int taos_stmt_affected_rows_once(TAOS_STMT* stmt);

//        // ========================== Async Query====================
//        /// <summary>
//        /// This API uses non-blocking call mode.
//        /// Application can open multiple tables and manipulate(query or insert) opened table concurrently. 
//        /// So applications must ensure that opetations on the same table is completely serialized.
//        /// Because that will cause some query and insert operations cannot be performed.
//        /// </summary>
//        /// <param name=DLLName> A taos connection return by Connect()</param>
//        /// <param name="sql">sql command need to execute</param>
//        /// <param name="fq">User-defined callback function. <see cref="QueryAsyncCallback"/></param>
//        /// <param name="param">the parameter for callback</param>       
//        [DllImport(DLLName, EntryPoint = "taos_query_a", CallingConvention = CallingConvention.Cdecl)]
//        static extern public void QueryAsync(IntPtr taos, string sql, QueryAsyncCallback fq, IntPtr param);

//        /// <summary>
//        /// Get the result set of asynchronous queries in batch, 
//        /// which can only be used with QueryAsync().<c>FetchRowAsyncCallback<c>
//        /// </summary>
//        /// <param name="taoRes"> The result set returned when backcall QueryAsyncCallback </param>
//        /// <param name="fq"> Callback function.<see cref="FetchRowAsyncCallback"/></param>
//        /// <param name="param"> The parameter for callback FetchRowAsyncCallback </param>
//        [DllImport(DLLName, EntryPoint = "taos_fetch_rows_a", CallingConvention = CallingConvention.Cdecl)]
//        static extern public void FetchRowAsync(IntPtr taoRes, FetchRowAsyncCallback fq, IntPtr param);

//        // rawblock way
//        [DllImport(DLLName, EntryPoint = "taos_fetch_raw_block_a", CallingConvention = CallingConvention.Cdecl)]
//        static extern public void FetchRawBlockAsync(IntPtr taoRes, QueryAsyncCallback fq, IntPtr param);
//        //void taos_fetch_raw_block_a(TAOS_RES *res, __taos_async_fn_t fp, void *param);

//        [DllImport(DLLName, EntryPoint = "taos_get_raw_block", CallingConvention = CallingConvention.Cdecl)]
//        static extern public void GetRawBlock(IntPtr taoRes);
//        //const void* taos_get_raw_block(TAOS_RES * res);

//        // Using TMQ in TDengine 3.0 to  instead Subscribe,Consume,Unsubscribe

//        // ================================add =========================
//        //int taos_select_db(TAOS *taos, const char *db);
//        [DllImport(DLLName, EntryPoint = "taos_select_db", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int SelectDatabase(IntPtr taos, string database);

//        //int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields);

//        //void  taos_stop_query(TAOS_RES *res);
//        [DllImport(DLLName, EntryPoint = "taos_stop_query", CallingConvention = CallingConvention.Cdecl)]
//        static extern public void StopQuery(IntPtr res);

//        //bool taos_is_update_query(TAOS_RES *res);
//        [DllImport(DLLName, EntryPoint = "taos_is_update_query", CallingConvention = CallingConvention.Cdecl)]
//        static extern public bool IsUpdateQuery(IntPtr res);

//        //int taos_validate_sql(TAOS *taos, const char *sql);
//        [DllImport(DLLName, EntryPoint = "taos_validate_sql", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int ValidateSQL(IntPtr taos, string sql);

//        // void taos_reset_current_db(TAOS *taos);
//        [DllImport(DLLName, EntryPoint = "taos_reset_current_db", CallingConvention = CallingConvention.Cdecl)]
//        static extern public void ResetCurrentDatabase(IntPtr taos);

//        // TAOS_ROW *taos_result_block(TAOS_RES *res);

//        // char *taos_get_server_info(TAOS *taos);
//        [DllImport(DLLName, EntryPoint = "taos_get_server_info", CallingConvention = CallingConvention.Cdecl)]
//        static extern public string GetServerInfo(IntPtr taos);

//        // char *taos_get_client_info();
//        [DllImport(DLLName, EntryPoint = "taos_get_client_info", CallingConvention = CallingConvention.Cdecl)]
//        static extern public string GetClinetInfo();
//        // ====================== 3.0 =====================
//        //bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col);
//        [DllImport(DLLName, EntryPoint = "taos_is_null", CallingConvention = CallingConvention.Cdecl)]
//        static extern public bool IsNull(IntPtr res, Int32 row, Int32 col);

//        //int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows)
//        [DllImport(DLLName, EntryPoint = "taos_fetch_block", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int taos_fetch_block(IntPtr res, IntPtr rows);

//        //int taos_fetch_raw_block(TAOS_RES *res, int* numOfRows, void** pData);
//        [DllImport(DLLName, EntryPoint = "taos_fetch_raw_block", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int FetchRawBlock(IntPtr res, IntPtr numOfRows, IntPtr pData);

//        //int* taos_get_column_data_offset(TAOS_RES *res, int columnIndex);
//        [DllImport(DLLName, EntryPoint = "taos_get_column_data_offset", CallingConvention = CallingConvention.Cdecl)]
//        static extern public IntPtr GetColDataOffset(IntPtr res, int columnIndex);

//        // TSDB_SERVER_STATUS taos_check_server_status(const char *fqdn, int port, char *details, int maxlen);
//        [DllImport(DLLName, EntryPoint = "taos_check_server_status", CallingConvention = CallingConvention.Cdecl)]
//        static extern public int CheckServerStatus(string fqdn, int port, string detail, int maxlength);
//    }

//}