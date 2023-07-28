using System;

namespace TDengine.Data.Client
{
    
    public class TDengineError: Exception
    {
        public int Code { get; }
        public string Error { get; }

        public TDengineError(int code,string error):base($"code:[0x{code:x}],error:{error}")
        {
            Code = code;
            Error = error;
        }
    }
}