using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;


namespace TDengineTMQ.Impl
{
    internal class TMQSafeHandle
    {
        private void NullReferenceHandler(IntPtr ptr)
        {
            if (ptr == IntPtr.Zero)
            {
                throw new NullReferenceException();
            }
        }
        private void ErrorHandler(int code)
        {
            throw new Exception($"TDengine TMQ Error:{LibTMQ.err2str(code)} \t {code}");
        }
        internal IntPtr NewTopicList()
        {
            var topicList = IntPtr.Zero;
            topicList = LibTMQ.tmq_list_new();
            if (topicList == IntPtr.Zero)
            {
                throw new Exception("Create topic failed");
            }
            return topicList;
        }

        internal void TopicListAppend(IntPtr topicList, string str)
        {
            NullReferenceHandler(topicList);
            if (LibTMQ.tmq_list_append(topicList, str) != 0)
            {
                throw new Exception($"add new topic ${str} failed");
            }
        }

        internal void TopicListDestroy(IntPtr topicList)
        {
            NullReferenceHandler(topicList);
            LibTMQ.tmq_list_destroy(topicList);
        }

        internal Int32 TopicListGetSize(IntPtr topicList)
        {
            NullReferenceHandler(topicList);
            return LibTMQ.tmq_list_get_size(topicList);
        }

        internal List<string> GetTopicList(IntPtr topicList)
        {
            NullReferenceHandler(topicList);
            int listSize = TopicListGetSize(topicList);
            var tmp = new List<String>();
            IntPtr topicIntPtr = LibTMQ.tmq_list_to_c_array(topicList);
            for (int i = 0; i < listSize; i++)
            {
                var topic = Marshal.PtrToStringUTF8(topicIntPtr + (i * IntPtr.Size));
                Console.WriteLine("TMQListToCArray topic[{0}]:{1}", i, topic);
                tmp.Add(topic);
            }
            return tmp;
        }

        internal TMQ_RES GetResType(IntPtr message)
        {
            NullReferenceHandler(message);
            return (TMQ_RES)LibTMQ.tmq_get_res_type(message);

        }

        internal string GetTopicName(IntPtr message)
        {
            NullReferenceHandler(message);
            return LibTMQ.tmq_get_topic_name(message);
        }

        internal Int32 GetVGroupID(IntPtr message)
        {
            NullReferenceHandler(message);
            return LibTMQ.tmq_get_vgroup_id(message);
        }

        internal string GetTableName(IntPtr message)
        {
            NullReferenceHandler(message);
            return LibTMQ.tmq_get_table_name(message);
        }

        //Message value
        //Message meta 

        //config

        internal IntPtr ConfNew()
        {
            return LibTMQ.tmq_conf_new();
        }

        internal void ConfSet(IntPtr conf, string key, string value)
        {
            NullReferenceHandler(conf);
            var code = LibTMQ.tmq_conf_set(conf, key, value);
            if ((TMQ_CONF_RES)code == TMQ_CONF_RES.TMQ_CONF_UNKNOWN)
            {
                throw new Exception("set config failed,since TMQ_CONF_UNKNOWN ");
            }
            else if ((TMQ_CONF_RES)code == TMQ_CONF_RES.TMQ_CONF_INVALID)
            {
                throw new Exception($"set config failed,since TMQ_CONF_INVALID key:{key} value:{value}");
            }
        }
        internal void ConfSet(IntPtr conf, IEnumerable<KeyValuePair<string, string>> values)
        {
            NullReferenceHandler(conf);
            foreach (var v in values)
            {
                int code = LibTMQ.tmq_conf_set(conf, v.Key, v.Value);
                if ((TMQ_CONF_RES)code == TMQ_CONF_RES.TMQ_CONF_UNKNOWN)
                {
                    throw new Exception("set config failed,since TMQ_CONF_UNKNOWN ");
                }
                else if ((TMQ_CONF_RES)code == TMQ_CONF_RES.TMQ_CONF_INVALID)
                {
                    throw new Exception($"set config failed,since TMQ_CONF_INVALID key:{v.Key} value:{v.Value}");
                }
            }
        }
        internal void ConfDestroy(IntPtr conf)
        {
            NullReferenceHandler(conf);
            LibTMQ.tmq_conf_destroy(conf);
        }

        internal void ConfSetAutoCimmitCallBack(IntPtr conf, LibTMQ.tmq_commit_cb cb, IntPtr param)
        {
            NullReferenceHandler(conf);
            LibTMQ.tmq_conf_set_auto_commit_cb(conf, cb, param);
        }

        // consumer
        internal IntPtr ConsumerNew(IEnumerable<KeyValuePair<string, string>> tmqConf)
        {
            var confPtr = this.ConfNew();
            NullReferenceHandler(confPtr);
            this.ConfSet(confPtr, tmqConf);

            int errStringLength = 1024;
            IntPtr errStrPtr = Marshal.AllocHGlobal(errStringLength);
            var consumer = LibTMQ.tmq_consumer_new(confPtr, errStrPtr, errStringLength);
            try
            {
                // error happened while create new consumer
                if (consumer == IntPtr.Zero)
                {
                    // read Error string 
                    string errStr = Marshal.PtrToStringUTF8(errStrPtr, errStringLength);
                    throw new Exception($"Create new Consumer failed, reason:{errStr}");
                }
            }
            finally
            {
                Marshal.FreeHGlobal(errStrPtr);
                this.ConfDestroy(confPtr);
            }
            return consumer;
        }

        internal void Subscribe(IntPtr tmq, IEnumerable<string> topicList)
        {
            NullReferenceHandler(tmq);
            IntPtr topicPtr = LibTMQ.tmq_list_new();
            NullReferenceHandler(topicPtr);
            foreach (var topic in topicList)
            {
                LibTMQ.tmq_list_append(topicPtr, topic);
            }

            LibTMQ.tmq_subscribe(tmq, topicPtr);
        }

        internal Int32 Unsubscribe(IntPtr tmq)
        {
            NullReferenceHandler(tmq);
            return LibTMQ.tmq_unsubscribe(tmq);
        }

        //return topic list 
        internal List<string> Subscription(IntPtr tmq)
        {
            NullReferenceHandler(tmq);
            IntPtr topicList = IntPtr.Zero;
            IntPtr topicListPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(IntPtr)));
            List<string> topics;
            try
            {
                int code = LibTMQ.tmq_subscription(tmq, topicListPtr);
                if (code == 0)
                {
                    topicList = Marshal.ReadIntPtr(topicListPtr);
                    var c_array_leng = LibTMQ.tmq_list_get_size(topicList);
                    var c_array = LibTMQ.tmq_list_to_c_array(topicList);

                    topics = new List<string>(c_array_leng);
                    var offset = Marshal.SizeOf(typeof(IntPtr));

                    for (int i = 0; i < c_array_leng; i++)
                    {
                        string tmpStr = Marshal.PtrToStringUTF8(c_array + (i * offset));
                        topics.Add(tmpStr);
                    }
                }
                else
                {
                    throw new Exception("get current topic list failed");
                }

            }
            finally
            {
                Marshal.FreeHGlobal(topicListPtr);
            }
            return topics;
        }

        internal ConsumeResult<TopicPartition, KeyValuePair<List<TDengineMeta>, List<Object>>> ConsumerPoll(IntPtr tmq, Int64 timeout)
        {
            NullReferenceHandler(tmq);
            IntPtr taosRes = LibTMQ.tmq_consumer_poll(tmq, timeout);
            NullReferenceHandler(taosRes);

            TopicPartition topicPartition = new TopicPartition(LibTMQ.tmq_get_topic_name(taosRes), LibTMQ.tmq_get_vgroup_id(taosRes), LibTMQ.tmq_get_db_name(taosRes), LibTMQ.tmq_get_table_name(taosRes));
            List<TDengineMeta> metas = new List<TDengineMeta>() { };// TDengine.FetchFields(taosRes)
            List<Object> records = new List<object>() { 12345, true, "nchar" }; // TDengine.FetchRawBlock(taosRes)

            ConsumeResult<TopicPartition, KeyValuePair<List<TDengineMeta>, List<Object>>> consumerResult = new ConsumeResult<TopicPartition, KeyValuePair<List<TDengineMeta>, List<object>>>(KeyValuePair.Create(topicPartition, KeyValuePair.Create(metas, records))); ;

            // need to free taosRes with taos_free()
            return consumerResult;
        }

        internal Int32 ConsumerClose(IntPtr tmq)
        {
            NullReferenceHandler(tmq);
            return LibTMQ.tmq_consumer_close(tmq);
        }

        internal void CommitSync(IntPtr tmq, IntPtr msg)
        {
            NullReferenceHandler(tmq);
            NullReferenceHandler(msg);
            int code = -1;
            if ((code = LibTMQ.tmq_commit_sync(tmq, msg)) != 0)
            {
                throw new Exception($"Sync Commit has failed {LibTMQ.err2str(code)}");
            }
        }

        internal void CommitAsync(IntPtr tmq, IntPtr msg, LibTMQ.tmq_commit_cb callback, IntPtr param)
        {
            NullReferenceHandler(tmq);
            NullReferenceHandler(msg);
            LibTMQ.tmq_commit_async(tmq, msg, callback, param);
        }

    }
}

