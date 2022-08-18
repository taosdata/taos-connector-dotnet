using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using TDengineDriver;
using TDengineDriver.Impl;

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
        private void ErrorHandler(string errMsg, int code)
        {
            string errStr = Marshal.PtrToStringUTF8(LibTMQ.err2str(code));
            throw new Exception($"TDengine TMQ Error:{errMsg} failed,reason:{errStr} \t code: {code}");
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
            string topicName = Marshal.PtrToStringUTF8(LibTMQ.tmq_get_topic_name(message));
            return topicName;
        }

        internal Int32 GetVGroupID(IntPtr message)
        {
            NullReferenceHandler(message);
            return LibTMQ.tmq_get_vgroup_id(message);
        }
        internal string GetDBName(IntPtr message)
        {
            NullReferenceHandler(message);
            string db = Marshal.PtrToStringUTF8(LibTMQ.tmq_get_db_name(message));
            return db;
        }

        internal string GetTableName(IntPtr message)
        {
            NullReferenceHandler(message);
            string tableName = Marshal.PtrToStringUTF8(LibTMQ.tmq_get_table_name(message));
            return tableName;
        }


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
            //foreach (var cfg in builder.Config)
            //{
            //    Console.WriteLine(cfg.Key, cfg.Value);
            //}
            this.ConfSet(confPtr, tmqConf);

            int errStringLength = 256;
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
                else
                {
                    return consumer;
                }
            }
            finally
            {
                Marshal.FreeHGlobal(errStrPtr);
                this.ConfDestroy(confPtr);
            }

        }

        internal void Subscribe(IntPtr tmq, IEnumerable<string> topicList)
        {
            NullReferenceHandler(tmq);
            IntPtr topicPtr = LibTMQ.tmq_list_new();
            NullReferenceHandler(topicPtr);

            foreach (var topic in topicList)
            {
                int code = LibTMQ.tmq_list_append(topicPtr, topic);
                if (code != 0)
                {
                    this.ErrorHandler("tmq_list_append", code);
                }
            }
            LibTMQ.tmq_subscribe(tmq, topicPtr);
            LibTMQ.tmq_list_destroy(topicPtr);
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
            IntPtr topicListPtr = LibTMQ.tmq_list_new();
            IntPtr topicListPPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(IntPtr)));
            Marshal.WriteIntPtr(topicListPPtr, topicListPtr);
            List<string> topics;
            try
            {
                int code = LibTMQ.tmq_subscription(tmq, topicListPPtr);

                if (code == 0)
                {
                    var c_array_leng = LibTMQ.tmq_list_get_size(topicListPtr);
                    var c_array_ptr = LibTMQ.tmq_list_to_c_array(topicListPtr);

                    topics = new List<string>(c_array_leng);
                    var offset = Marshal.SizeOf(typeof(IntPtr));

                    for (int i = 0; i < c_array_leng; i++)
                    {
                        string tmpStr = Marshal.PtrToStringUTF8(Marshal.ReadIntPtr(c_array_ptr + (i * offset)));
                        topics.Add(tmpStr);
                    }
                    return topics;
                }
                else
                {
                    this.ErrorHandler("tmq_subscription", code);
                    return null;
                }

            }
            finally
            {
                LibTMQ.tmq_list_destroy(topicListPtr);
                Marshal.FreeHGlobal(topicListPPtr);

            }

        }

        internal ConsumeResult ConsumerPoll(IntPtr tmq, Int64 timeout)
        {
            NullReferenceHandler(tmq);
            IntPtr taosRes = LibTMQ.tmq_consumer_poll(tmq, timeout);

            ConsumeResult consumeResult = new ConsumeResult();

            if (taosRes == IntPtr.Zero)
            {
                return consumeResult;
            }

            IntPtr numOfRowsPrt = Marshal.AllocHGlobal(sizeof(Int32));
            IntPtr pDataPtr = Marshal.AllocHGlobal(IntPtr.Size);
            IntPtr pData;

            try
            {
                while (true)
                {
                    int code = TDengine.FetchRawBlock(taosRes, numOfRowsPrt, pDataPtr);

                    if (code != 0)
                    {
                        throw new Exception($"TMQ fetch_raw_block failed,code {code} reason:{TDengine.Error(taosRes)}");
                    }
                    int numOfRows = Marshal.ReadInt32(numOfRowsPrt);
                    int numOfFileds = TDengine.FieldCount(taosRes);
                    pData = Marshal.ReadIntPtr(pDataPtr);

                    if (numOfRows == 0)
                    {
                        break;
                    }
                    else
                    {

                        string topic = this.GetTopicName(taosRes);
                        Int32 vGourpId = this.GetVGroupID(taosRes);
                        string db = this.GetDBName(taosRes);
                        string? table = this.GetTableName(taosRes);

                        List<TDengineMeta> metaList = LibTaos.GetMeta(taosRes);
                        List<Object> dataList = LibTaos.ReadRawBlock(pData, metaList, numOfRows);

                        consumeResult.Add(new TopicPartition(topic, vGourpId, db, table), new TaosResult(metaList, dataList));
                        consumeResult.Offset = taosRes;

                    }
                }

                return consumeResult;
            }
            finally
            {

                Marshal.FreeHGlobal(numOfRowsPrt);
                Marshal.FreeHGlobal(pDataPtr);
                //TDengine.FreeResult(taosRes);
            }

        }

        internal Int32 ConsumerClose(IntPtr tmq)
        {
            NullReferenceHandler(tmq);
            return LibTMQ.tmq_consumer_close(tmq);
        }

        internal void CommitSync(IntPtr tmq, ConsumeResult consumeResult)
        {
            NullReferenceHandler(tmq);
            //NullReferenceHandler(consumeResult.msg);

            //ConsumeResult<TopicPartition, KeyValuePair<List<TDengineMeta>, List<Object>>> message = new ConsumeResult(msg.key,msg.value);
            int code = -1;
            if ((code = LibTMQ.tmq_commit_sync(tmq, consumeResult.Offset)) != 0)
            {
                ErrorHandler("Sync Commit", code);
            }
            TDengine.FreeResult(consumeResult.Offset);
        }

        internal void CommitAsync(IntPtr tmq, ConsumeResult consumeResult, LibTMQ.tmq_commit_cb callback, IntPtr? param)
        {
            NullReferenceHandler(tmq);
            NullReferenceHandler(consumeResult.Offset);
            //LibTMQ.tmq_commit_async(tmq, consumeResult.msg, callback, IntPtr.Zero);
        }

    }
}

