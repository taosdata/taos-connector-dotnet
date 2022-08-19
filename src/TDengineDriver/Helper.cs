using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace TDengineDriver
{
    internal static class Helper
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
}
