using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Linq;

namespace TDengineTMQ.Impl
{
    // NEW ENUM 3.0 FOR TMQ   
    //public enum TMQRespErr
    //{
    //    TMQ_RESP_ERR__FAIL = -1,
    //    TMQ_RESP_ERR__SUCCESS = 0,
    //}

    internal enum TMQ_CONF_RES
    {
        TMQ_CONF_UNKNOWN = -2,
        TMQ_CONF_INVALID = -1,
        TMQ_CONF_OK = 0,
    }

    internal enum TMQ_RES
    {
        TMQ_RES_INVALID = -1,
        TMQ_RES_DATA = 1,
        TMQ_RES_TABLE_META = 2,
    }

    internal static class LibTMQ
    {
        static object loadLockObj = new object();
        static bool isInitialized = false;

        public static bool IsInitialized
        {
            get
            {
                lock (loadLockObj)
                {
                    return isInitialized;
                }
            }
        }
        public static bool Initialize(string userSpecifiedpath)
        {
            lock (loadLockObj)
            {
                if (isInitialized)
                {
                    return false;
                }
                LoadDelegate(userSpecifiedpath);
                isInitialized = true;
                return isInitialized;
            }

        }
        internal static bool LoadDelegate(string userSpecifiedpath)
        {
            var delegates = new List<Type>();
            delegates.Add(typeof(NativeTMQMethods));

            // try to set Delegates
            foreach (var f in delegates)
            {
                if (SetDelegates(f))
                {
                    return true;
                }
            }
            throw new DllNotFoundException("failed to load");
        }

        static bool SetDelegates(Type nativeMethodClass)
        {
            var methods = nativeMethodClass.GetRuntimeMethods().ToArray();

            _connect = (Func<string, string, string, string, short, IntPtr>)methods.Single(m => m.Name == "Connect").CreateDelegate(typeof(Func<string, string, string, string, short, IntPtr>));
            _close = (Action<IntPtr>)methods.Single(m => m.Name == "Close").CreateDelegate(typeof(Action<IntPtr>));
            _getServerInfo = (Func<IntPtr, string>)methods.Single(m => m.Name == "GetServerInfo").CreateDelegate(typeof(Func<IntPtr, string>));
            _getClinetInfo = (Func<string>)methods.Single(methods => methods.Name == "GetClinetInfo").CreateDelegate(typeof(Func<string>));

            /* --------------------------TMQ INTERFACE------------------------------- */

            //_tmq_commit_cb = (Action<IntPtr,Int32,IntPtr>)methods.Single(m => m.Name == "tmq_commit_cb").CreateDelegate(typeof(Action<IntPtr, Int32, IntPtr>));
            _err2str = (Func<Int32, string>)methods.Single(m => m.Name == "tmq_err2str").CreateDelegate(typeof(Func<Int32, string>));

            _tmq_conf_new = (Func<IntPtr>)methods.Single(m => m.Name == "tmq_conf_new").CreateDelegate(typeof(Func<IntPtr>));
            _tmq_conf_set = (Func<IntPtr, string, string, int>)methods.Single(m => m.Name == "tmq_conf_set").CreateDelegate(typeof(Func<IntPtr, string, string, int>));
            _tmq_conf_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "tmq_conf_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _tmq_conf_set_auto_commit_cb = (Action<IntPtr, tmq_commit_cb, IntPtr>)methods.Single(m => m.Name == "tmq_conf_set_auto_commit_cb").CreateDelegate(typeof(Action<IntPtr, tmq_commit_cb, IntPtr>));

            _tmq_consumer_new = (Func<IntPtr, IntPtr, int, IntPtr>)methods.Single(m => m.Name == "tmq_consumer_new").CreateDelegate(typeof(Func<IntPtr, IntPtr, int, IntPtr>));
            _tmq_subscribe = (Func<IntPtr, IntPtr, Int32>)methods.Single(m => m.Name == "tmq_subscribe").CreateDelegate(typeof(Func<IntPtr, IntPtr, int>));
            _tmq_unsubscribe = (Func<IntPtr, Int32>)methods.Single(m => m.Name == "tmq_unsubscribe").CreateDelegate(typeof(Func<IntPtr, Int32>));
            _tmq_subscription = (Func<IntPtr, IntPtr, Int32>)methods.Single(m => m.Name == "tmq_subscription").CreateDelegate(typeof(Func<IntPtr, IntPtr, Int32>));
            _tmq_consumer_poll = (Func<IntPtr, Int64, IntPtr>)methods.Single(m => m.Name == "tmq_consumer_poll").CreateDelegate(typeof(Func<IntPtr, Int64, IntPtr>));
            _tmq_consumer_close = (Func<IntPtr, Int32>)methods.Single(m => m.Name == "tmq_consumer_close").CreateDelegate(typeof(Func<IntPtr, Int32>));
            _tmq_commit_sync = (Func<IntPtr, IntPtr, Int32>)methods.Single(m => m.Name == "tmq_commit_sync").CreateDelegate(typeof(Func<IntPtr, IntPtr, Int32>));
            _tmq_commit_async = (Action<IntPtr, IntPtr, tmq_commit_cb, IntPtr>)methods.Single(m => m.Name == "tmq_commit_async ").CreateDelegate(typeof(Action<IntPtr, IntPtr, tmq_commit_cb, IntPtr>));



            _tmq_get_res_type = (Func<IntPtr, int>)methods.Single(m => m.Name == "tmq_get_res_type").CreateDelegate(typeof(Func<IntPtr, int>));
            _tmq_get_raw_meta = (Func<IntPtr, IntPtr, IntPtr, int>)methods.Single(m => m.Name == "tmq_get_raw_meta").CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr, int>));
            _tmq_get_topic_name = (Func<IntPtr, string>)methods.Single(m => m.Name == "tmq_get_topic_name").CreateDelegate(typeof(Func<IntPtr, string>));
            _tmq_get_db_name = (Func<IntPtr, string>)methods.Single(m => m.Name == "tmq_get_db_name").CreateDelegate(typeof(Func<IntPtr, string>));
            _tmq_get_vgroup_id = (Func<IntPtr, Int32>)methods.Single(m => m.Name == "tmq_get_vgroup_id").CreateDelegate(typeof(Func<IntPtr, Int32>));
            _tmq_get_table_name = (Func<IntPtr, string>)methods.Single(m => m.Name == "tmq_get_table_name").CreateDelegate(typeof(Func<IntPtr, string>));

            _tmq_list_new = (Func<IntPtr>)methods.Single(m => m.Name == "tmq_list_new").CreateDelegate(typeof(Func<IntPtr>));
            _tmq_list_append = (Func<IntPtr, string, Int32>)methods.Single(m => m.Name == "tmq_list_append").CreateDelegate(typeof(Func<IntPtr, string, Int32>));
            _tmq_list_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "tmq_list_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _tmq_list_get_size = (Func<IntPtr, Int32>)methods.Single(m => m.Name == "tmq_list_get_size").CreateDelegate(typeof(Func<IntPtr, Int32>));
            _tmq_list_to_c_array = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "tmq_list_to_c_array").CreateDelegate(typeof(Func<IntPtr, Int32>));


            try
            {
                //do something
            }
            catch
            {
                return false;
            }
            return true;

        }
        private static Func<string, string, string, string, short, IntPtr> _connect;
        internal static IntPtr Connect(string host, string user, string passwd, string db, short port) => _connect(host, user, passwd, db, port);

        private static Action<IntPtr> _close;
        internal static void Close(IntPtr taos) => _close(taos);

        private static Func<IntPtr, string> _getServerInfo;
        internal static string GetServerInfo(IntPtr taos) => _getServerInfo(taos);

        private static Func<string> _getClinetInfo;
        internal static string GetClinetInfo() => _getClinetInfo();

        //typedef void (tmq_commit_cb(tmq_t*, int32_t code, void* param));
        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void tmq_commit_cb(IntPtr tmq, Int32 code, IntPtr param);

        //private static Action<IntPtr, Int32, IntPtr> _tmq_commit_cb;
        //internal static void tmq_commit_cb(IntPtr tmq, Int32 code, IntPtr param) => _tmq_commit_cb(tmq,code,param);

        private static Func<Int32, string> _err2str;
        internal static string err2str(Int32 code) => _err2str(code);

        // config
        private static Func<IntPtr> _tmq_conf_new;
        internal static IntPtr tmq_conf_new() => _tmq_conf_new();

        private static Func<IntPtr, string, string, int> _tmq_conf_set;
        internal static int tmq_conf_set(IntPtr conf, string key, string value) => _tmq_conf_set(conf, key, value);

        private static Action<IntPtr> _tmq_conf_destroy;
        internal static void tmq_conf_destroy(IntPtr conf) => _tmq_conf_destroy(conf);

        private static Action<IntPtr, tmq_commit_cb, IntPtr> _tmq_conf_set_auto_commit_cb;
        internal static void tmq_conf_set_auto_commit_cb(IntPtr config, tmq_commit_cb _tmq_commit_cb, IntPtr param) => _tmq_conf_set_auto_commit_cb(config, _tmq_commit_cb, param);

        //consumer
        private static Func<IntPtr, IntPtr, int, IntPtr> _tmq_consumer_new;
        internal static IntPtr tmq_consumer_new(IntPtr tmqConf, IntPtr errStrPtr, int errStrLength) => _tmq_consumer_new(tmqConf, errStrPtr, errStrLength);

        private static Func<IntPtr, IntPtr, Int32> _tmq_subscribe;
        internal static void tmq_subscribe(IntPtr tmq, IntPtr topicList) => _tmq_subscribe(tmq, topicList);



        private static Func<IntPtr, Int32> _tmq_unsubscribe;
        internal static Int32 tmq_unsubscribe(IntPtr tmq) => _tmq_unsubscribe(tmq);

        private static Func<IntPtr, IntPtr, Int32> _tmq_subscription;
        internal static Int32 tmq_subscription(IntPtr tmq, IntPtr topicListPtr) => _tmq_subscription(tmq, topicListPtr);

        private static Func<IntPtr, Int64, IntPtr> _tmq_consumer_poll;
        internal static IntPtr tmq_consumer_poll(IntPtr tmq, Int64 timeout) => _tmq_consumer_poll(tmq, timeout);

        private static Func<IntPtr, Int32> _tmq_consumer_close;
        internal static Int32 tmq_consumer_close(IntPtr tmq) => _tmq_consumer_close(tmq);

        private static Func<IntPtr, IntPtr, Int32> _tmq_commit_sync;
        internal static Int32 tmq_commit_sync(IntPtr tmq, IntPtr msg) => _tmq_commit_sync(tmq, msg);

        private static Action<IntPtr, IntPtr, tmq_commit_cb, IntPtr> _tmq_commit_async;
        internal static void tmq_commit_async(IntPtr tmq, IntPtr msg, tmq_commit_cb callback, IntPtr param) => _tmq_commit_async(tmq, msg, callback, param);

        // consumer result
        private static Func<IntPtr, int> _tmq_get_res_type;
        internal static int tmq_get_res_type(IntPtr taosRes) => _tmq_get_res_type(taosRes);

        private static Func<IntPtr, IntPtr, IntPtr, int> _tmq_get_raw_meta;
        internal static Int32 tmq_get_raw_meta(IntPtr taosRes, IntPtr rawMetaPtr, IntPtr rawMetaLengthPtr) => _tmq_get_raw_meta(taosRes, rawMetaPtr, rawMetaLengthPtr);

        private static Func<IntPtr, string> _tmq_get_topic_name;
        internal static string tmq_get_topic_name(IntPtr taosRes) => _tmq_get_topic_name(taosRes);

        private static Func<IntPtr, string> _tmq_get_db_name;
        internal static string tmq_get_db_name(IntPtr taosRes) => _tmq_get_db_name(taosRes);

        private static Func<IntPtr, Int32> _tmq_get_vgroup_id;
        internal static Int32 tmq_get_vgroup_id(IntPtr taosRes) => _tmq_get_vgroup_id(taosRes);

        private static Func<IntPtr, string> _tmq_get_table_name;
        internal static string tmq_get_table_name(IntPtr taosRes) => _tmq_get_table_name(taosRes);

        //topic list
        private static Func<IntPtr> _tmq_list_new;
        internal static IntPtr tmq_list_new() => _tmq_list_new();

        private static Func<IntPtr, string, Int32> _tmq_list_append;
        internal static Int32 tmq_list_append(IntPtr tmqList, string str) => _tmq_list_append(tmqList, str);

        private static Action<IntPtr> _tmq_list_destroy;
        internal static void tmq_list_destroy(IntPtr tmqList) => _tmq_list_destroy(tmqList);

        private static Func<IntPtr, Int32> _tmq_list_get_size;
        internal static Int32 tmq_list_get_size(IntPtr tmqList) => _tmq_list_get_size(tmqList);

        private static Func<IntPtr, IntPtr> _tmq_list_to_c_array;
        internal static IntPtr tmq_list_to_c_array(IntPtr tmqList) => _tmq_list_to_c_array(tmqList);



    }
}
