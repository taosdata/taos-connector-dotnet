using TDengineDriver;

namespace TDengineWS.Impl
{
    public static class WSMultiBind
    {
        public static TAOS_MULTI_BIND WSBindBool(bool?[] boolArr)
        {
            return TaosMultiBind.MultiBindBool(boolArr);
        }

        public static TAOS_MULTI_BIND WSBindTinyInt(sbyte?[] sbyteArr)
        {
            return TaosMultiBind.MultiBindTinyInt(sbyteArr);
        }

        public static TAOS_MULTI_BIND WSBindUTinyInt(byte?[] byteArr)
        {
            return TaosMultiBind.MultiBindUTinyInt(byteArr);
        }

        public static TAOS_MULTI_BIND WSBindSmallInt(short?[] shortArr)
        {
            return TaosMultiBind.MultiBindSmallInt(shortArr);
        }

        public static TAOS_MULTI_BIND WSBindUSamllInt(ushort?[] ushortArr)
        {
            return TaosMultiBind.MultiBindUSmallInt(ushortArr);
        }

        public static TAOS_MULTI_BIND WSBindInt(int?[] intArr)
        {
            return TaosMultiBind.MultiBindInt(intArr);
        }

        public static TAOS_MULTI_BIND WSBindUInt(uint?[] uintArr)
        {
            return TaosMultiBind.MultiBindUInt(uintArr);
        }

        public static TAOS_MULTI_BIND WSBindBigInt(long?[] longArr)
        {
            return TaosMultiBind.MultiBindBigint(longArr);
        }
        public static TAOS_MULTI_BIND WSBindTimestamp(long[] longArr)
        {
            return TaosMultiBind.MultiBindTimestamp(longArr);
        }
        public static TAOS_MULTI_BIND WSBindUBigInt(ulong?[] ulongArr)
        {
            return TaosMultiBind.MultiBindUBigInt(ulongArr);
        }

        public static TAOS_MULTI_BIND WSBindFloat(float?[] floatArr)
        {
            return TaosMultiBind.MultiBindFloat(floatArr);
        }

        public static TAOS_MULTI_BIND WSBindDouble(double?[] doubleArr)
        {
            return TaosMultiBind.MultiBindDouble(doubleArr);
        }

        public static TAOS_MULTI_BIND WSBindBinary(string[] strArr)
        {
            return TaosMultiBind.MultiBindBinary(strArr);
        }
        public static TAOS_MULTI_BIND WSBindVarchar(string[] strArr)
        {
            return WSBindBinary(strArr);
        }

        public static TAOS_MULTI_BIND WSBindNchar(string[] strArr)
        {
            return TaosMultiBind.MultiBindNchar(strArr);
        }

        /// <summary>
        /// Currently this method is only used for bind JSON tag value
        /// </summary>
        /// <param name="strArr"></param>
        /// <returns></returns>
        public static TAOS_MULTI_BIND WSBindJSON(string[] strArr)
        {
            return TaosMultiBind.MultiBindJson(strArr);
        }

        //FRee memory method meed to implement
    }
}
