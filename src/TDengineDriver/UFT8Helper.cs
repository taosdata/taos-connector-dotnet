using System;
using System.Runtime.InteropServices;
using System.Text;


namespace TDengineDriver
{
    internal static class UFT8Helper
    {

        public static unsafe IntPtr StringToCoTaskMemUTF8(string s)
        {
            if (s is null)
            {
                return IntPtr.Zero;
            }
            int nb = Encoding.UTF8.GetMaxByteCount(s.Length);
            IntPtr pMem = Marshal.AllocCoTaskMem(nb + 1);
            int nbWritten;
            byte* pbMem = (byte*)pMem;
            fixed (char* firstChar = s)
            {
                nbWritten = Encoding.UTF8.GetBytes(firstChar, s.Length, pbMem, nb);
            }
            pbMem[nbWritten] = 0;
            return pMem;
        }


    }
    public struct UTF8PtrStruct
    {
        public IntPtr utf8Ptr { get; set; }
        public int utf8StrLength { get; set; }

        public UTF8PtrStruct(string str)
        {

#if NETSTANDARD2_1
            utf8StrLength = Encoding.UTF8.GetByteCount(str);
            utf8Ptr = Marshal.StringToCoTaskMemUTF8(str);
#else
            
            var utf8Bytes = Encoding.UTF8.GetBytes(str);
            utf8StrLength = utf8Bytes.Length + 1;
            byte[] targetUtf8Bytes = new byte[utf8StrLength];
            utf8Bytes.CopyTo(targetUtf8Bytes, 0);

            foreach (var b in targetUtf8Bytes)
            {
                //Console.WriteLine("{0}",b);
            }
            
            utf8Ptr = Marshal.AllocHGlobal(utf8StrLength);
            Marshal.Copy(targetUtf8Bytes, 0, utf8Ptr, utf8StrLength);
#endif
        }
        public void UTF8FreePtr()
        {
#if NETSTANDARD2_1_OR_GREATER
            Marshal.FreeCoTaskMem(utf8Ptr);
#else 
            Marshal.FreeHGlobal(utf8Ptr);
#endif
        }

    }
}
