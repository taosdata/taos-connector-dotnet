using System;
using TDengineDriver;
using TDengineDriver.Impl;
using System.Collections.Generic;
namespace Test.Utils.ResultSet
{
    public class ResultSet
    {
        internal List<TDengineMeta> ResultMeta { get; set; }
        internal List<Object> ResultData { get; set; }
        public ResultSet(IntPtr res)
        {

            ResultMeta = LibTaos.GetMeta(res);
            ResultData = LibTaos.GetData(res);
            TDengine.FreeResult(res);
        }

        public ResultSet(List<TDengineMeta> meta, List<Object> data)
        {
            ResultMeta = meta;
            ResultData = data;
        }

    }

}