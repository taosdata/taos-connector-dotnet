using System;
using System.Runtime.InteropServices;

namespace TDengineTMQ.Impl
{
    internal static class NativeTMQMethods
    {
        public const string DLLName = "taos";

        /* -------------------------- TEMP ------------------------------- */
        //[DllImport(DLLName, EntryPoint = "taos_connect", CallingConvention = CallingConvention.Cdecl)]
        //internal static extern IntPtr taos_connect(string ip, string user, string password, string db, short port);

        //[DllImport(DLLName, EntryPoint = "taos_close", CallingConvention = CallingConvention.Cdecl)]
        //internal static extern void taos_close(IntPtr taos);

        //[DllImport(DLLName, EntryPoint = "taos_get_server_info", CallingConvention = CallingConvention.Cdecl)]
        //internal static extern string taos_get_server_info(IntPtr taos);

        // char *taos_get_client_info();
        [DllImport(DLLName, EntryPoint = "taos_get_client_info", CallingConvention = CallingConvention.Cdecl)]
        internal static extern string taos_get_client_info();

        /* --------------------------TMQ INTERFACE------------------------------- */

        [DllImport(DLLName, EntryPoint = "tmq_err2str", CallingConvention = CallingConvention.Cdecl)]
        internal static extern string tmq_err2str(Int32 errorCode);
        //const char *tmq_err2str(int32_t code);

        /* --------------------------TMQ Config---------------------------------- */
        [DllImport(DLLName, EntryPoint = "tmq_conf_new", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr tmq_conf_new();
        //tmq_conf_t    *tmq_conf_new();

        [DllImport(DLLName, EntryPoint = "tmq_conf_set", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int tmq_conf_set(IntPtr conf, string key, string value);
        //tmq_conf_res_t tmq_conf_set(tmq_conf_t* conf, const char* key, const char* value);

        [DllImport(DLLName, EntryPoint = "tmq_conf_destroy", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void tmq_conf_destroy(IntPtr conf);
        //void tmq_conf_destroy(tmq_conf_t* conf);

        [DllImport(DLLName, EntryPoint = "tmq_conf_set_auto_commit_cb", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void tmq_conf_set_auto_commit_cb(IntPtr conf, LibTMQ.tmq_commit_cb callback, IntPtr param);
        //void tmq_conf_set_auto_commit_cb(tmq_conf_t* conf, tmq_commit_cb* cb, void* param);

        /* --------------------------TMQ Consumer-------------------------------- */
        [DllImport(DLLName, EntryPoint = "tmq_consumer_new", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr tmq_consumer_new(IntPtr tmqConf, IntPtr errStr, int errStrLength);
        //tmq_t *tmq_consumer_new(tmq_conf_t* conf, char* errstr, int32_t errstrLen);

        [DllImport(DLLName, EntryPoint = "tmq_subscribe", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int tmq_subscribe(IntPtr tmq, IntPtr topicList);
        //int32_t   tmq_subscribe(tmq_t *tmq, const tmq_list_t *topic_list);

        [DllImport(DLLName, EntryPoint = "tmq_unsubscribe", CallingConvention = CallingConvention.Cdecl)]
        internal static extern Int32 tmq_unsubscribe(IntPtr tmq);
        //int32_t   tmq_unsubscribe(tmq_t *tmq);

        [DllImport(DLLName, EntryPoint = "tmq_subscription", CallingConvention = CallingConvention.Cdecl)]
        internal static extern Int32 tmq_subscription(IntPtr tmq, out IntPtr topicListPtr);
        //int32_t   tmq_subscription(tmq_t *tmq, tmq_list_t **topics);

        [DllImport(DLLName, EntryPoint = "tmq_consumer_poll", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr tmq_consumer_poll(IntPtr tmq, Int64 timeout);
        //TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t timeout);

        [DllImport(DLLName, EntryPoint = "tmq_consumer_close", CallingConvention = CallingConvention.Cdecl)]
        internal static extern Int32 tmq_consumer_close(IntPtr tmq);
        //int32_t   tmq_consumer_close(tmq_t *tmq);

        [DllImport(DLLName, EntryPoint = "tmq_commit_sync", CallingConvention = CallingConvention.Cdecl)]
        internal static extern Int32 tmq_commit_sync(IntPtr tmq, IntPtr msg);
        //int32_t   tmq_commit_sync(tmq_t *tmq, const TAOS_RES *msg);

        [DllImport(DLLName, EntryPoint = "tmq_commit_async", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void tmq_commit_async(IntPtr tmq, IntPtr msg, LibTMQ.tmq_commit_cb callback, IntPtr param);
        //void tmq_commit_async(tmq_t* tmq, const TAOS_RES* msg, tmq_commit_cb *cb, void* param);

        /* --------------------------TMQ Message-------------------------------- */

        [DllImport(DLLName, EntryPoint = "tmq_get_res_type", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int tmq_get_res_type(IntPtr taosRes);
        //tmq_res_t tmq_get_res_type(TAOS_RES* res);

        [DllImport(DLLName, EntryPoint = "tmq_get_raw_meta", CallingConvention = CallingConvention.Cdecl)]
        internal static extern Int32 tmq_get_raw_meta(IntPtr taosRes, out IntPtr rawMetaPtr, out IntPtr rawMetaLengthPtr);
        //int32_t tmq_get_raw_meta(TAOS_RES* res, const void** raw_meta, int32_t *raw_meta_len);

        [DllImport(DLLName, EntryPoint = "tmq_get_topic_name", CallingConvention = CallingConvention.Cdecl)]
        internal static extern string tmq_get_topic_name(IntPtr taosRes);
        //const char* tmq_get_topic_name(TAOS_RES * res);

        [DllImport(DLLName, EntryPoint = "tmq_get_db_name", CallingConvention = CallingConvention.Cdecl)]
        internal static extern string tmq_get_db_name(IntPtr taosRes);
        //const char* tmq_get_db_name(TAOS_RES * res);

        [DllImport(DLLName, EntryPoint = "tmq_get_vgroup_id", CallingConvention = CallingConvention.Cdecl)]
        internal static extern Int32 tmq_get_vgroup_id(IntPtr taosRes);
        //int32_t tmq_get_vgroup_id(TAOS_RES* res);

        [DllImport(DLLName, EntryPoint = "tmq_get_table_name", CallingConvention = CallingConvention.Cdecl)]
        internal static extern string tmq_get_table_name(IntPtr taosRes);
        //const char* tmq_get_table_name(TAOS_RES * res);

        /* --------------------------TMQ Topic List-------------------------------- */
        [DllImport(DLLName, EntryPoint = "tmq_list_new", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr tmq_list_new();
        // tmq_list_t *tmq_list_new();


        [DllImport(DLLName, EntryPoint = "tmq_list_append", CallingConvention = CallingConvention.Cdecl)]
        internal static extern Int32 tmq_list_append(IntPtr tmqList, string str);
        // int32_t tmq_list_append(tmq_list_t *, const char *);


        [DllImport(DLLName, EntryPoint = "tmq_list_destroy", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void tmq_list_destroy(IntPtr tmqList);
        // void tmq_list_destroy(tmq_list_t *);

        [DllImport(DLLName, EntryPoint = "tmq_list_get_size", CallingConvention = CallingConvention.Cdecl)]
        internal static extern Int32 tmq_list_get_size(IntPtr tmqList);
        // int32_t tmq_list_get_size(const tmq_list_t *);

        [DllImport(DLLName, EntryPoint = "tmq_list_to_c_array", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr tmq_list_to_c_array(IntPtr tmqList);
        // char **tmq_list_to_c_array(const tmq_list_t *);
    }
}
