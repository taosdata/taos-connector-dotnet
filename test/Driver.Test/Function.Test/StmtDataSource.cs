using TDengineDriver;
using TDengineWS.Impl;
using System;
using System.Collections.Generic;
namespace Test.Utils.DataSource
{
    public class StmtDataSource
    {
        public static long[] tsArr = { 1637064040000, 1637064041000, 1637064042000, 1637064043000, 1637064044000 };
        public static bool?[] boolArr = { true, false, null, true, true };
        public static sbyte?[] tinyIntArr = { sbyte.MinValue, 0, null, 1, sbyte.MaxValue };
        public static short?[] shortArr = { short.MinValue, 1, null, 2, short.MaxValue };
        public static int?[] intArr = { int.MinValue, 2, null, 3, int.MaxValue };
        public static long?[] longArr = { long.MinValue, 3, null, 4L, long.MaxValue };
        public static float?[] floatArr = { float.MinValue, -3.1415F, null, 3.1415F, float.MaxValue };
        public static double?[] doubleArr = { double.MinValue, -3.1415926535897932D, null, 3.1415926535897932D, double.MaxValue };
        public static byte?[] uTinyIntArr = { byte.MinValue, 4, null, 5, byte.MaxValue };
        public static ushort?[] uShortArr = { ushort.MinValue, 5, null, 6, ushort.MaxValue };
        public static uint?[] uIntArr = { uint.MinValue, 6, null, 7, uint.MaxValue };
        public static ulong?[] uLongArr = { ulong.MinValue, 7L, null, 8L, long.MaxValue };
        public static string[] binaryArr = { "binary_数据_壹", String.Empty, "binary_数据_叁", "binary_数据_肆", "binary_数据_伍" };
        public static string[] ncharArr = { "nchar_数值_甲", String.Empty, "nchar_数值_丙", "nchar_数值_丁", "nchar_数值_戊" };



        public static Object[] tag1 = { true, (sbyte)-1, (short)-2, (int)-3, (long)-4, (byte)1, (ushort)2, (uint)3, (ulong)4, 3.1415F, 3.1415926535897932D, "binary_tag_壹_Ⅰ", "nchar_tag_壹_Ⅰ" };
        public static Object[] tag2 = { false, (sbyte)1, (short)2, (int)3, (long)4, (byte)2, (ushort)3, (uint)4, (ulong)5, 3.1415F * 2, 3.1415926535897932D * 2, "binary_tag_贰_Ⅱ", "nchar_tag_贰_Ⅱ" };
        public static Object[] tag3 = { false, (sbyte)2, (short)3, (int)4, (long)5, (byte)3, (ushort)4, (uint)5, (ulong)6, 3.1415F * 3, 3.1415926535897932D * 3, "binary_tag_叁_Ⅲ", "nchar_tag_叁_Ⅲ" };

        public static string[] jsonTag1 = { "{\"key1\":\"taosdata\",\"key2\":null,\"key3\":\"TDengine涛思数据\",\"key4\":1,\"key5\":true}" };
        public static string[] jsonTag2 = { "{\"key1\":\"taosdata\",\"key2\":null,\"key3\":\"TDengine涛思数据\",\"key4\":2,\"key5\":false}" };
        public static string[] jsonTag3 = { "{\"key1\":\"taosdata\",\"key2\":null,\"key3\":\"TDengine涛思数据\",\"key4\":3,\"key5\":true}" };

        public static TAOS_MULTI_BIND[] GetTags(int seq)
        {
            if (seq > 3 || seq < 1)
            {
                throw new Exception("seq should in range from 1-3");
            }

            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[13];
            if (seq == 1)
            {
                mBinds[0] = TaosMultiBind.MultiBindBool(new bool?[] { true });
                mBinds[1] = TaosMultiBind.MultiBindTinyInt(new sbyte?[] { -1 });
                mBinds[2] = TaosMultiBind.MultiBindSmallInt(new short?[] { -2 });
                mBinds[3] = TaosMultiBind.MultiBindInt(new int?[] { -3 });
                mBinds[4] = TaosMultiBind.MultiBindBigint(new long?[] { -4 });
                mBinds[5] = TaosMultiBind.MultiBindUTinyInt(new byte?[] { 1 });
                mBinds[6] = TaosMultiBind.MultiBindUSmallInt(new ushort?[] { 2 });
                mBinds[7] = TaosMultiBind.MultiBindUInt(new uint?[] { 3 });
                mBinds[8] = TaosMultiBind.MultiBindUBigInt(new ulong?[] { 4 });
                mBinds[9] = TaosMultiBind.MultiBindFloat(new float?[] { 3.1415F });
                mBinds[10] = TaosMultiBind.MultiBindDouble(new double?[] { 3.1415926535897932D });
                mBinds[11] = TaosMultiBind.MultiBindBinary(new string?[] { "binary_tag_壹_Ⅰ" });
                mBinds[12] = TaosMultiBind.MultiBindNchar(new string?[] { "nchar_tag_壹_Ⅰ" });
            }
            if (seq == 2)
            {
                mBinds[0] = TaosMultiBind.MultiBindBool(new bool?[] { false });
                mBinds[1] = TaosMultiBind.MultiBindTinyInt(new sbyte?[] { 1 });
                mBinds[2] = TaosMultiBind.MultiBindSmallInt(new short?[] { 2 });
                mBinds[3] = TaosMultiBind.MultiBindInt(new int?[] { 3 });
                mBinds[4] = TaosMultiBind.MultiBindBigint(new long?[] { 4 });
                mBinds[5] = TaosMultiBind.MultiBindUTinyInt(new byte?[] { 2 });
                mBinds[6] = TaosMultiBind.MultiBindUSmallInt(new ushort?[] { 3 });
                mBinds[7] = TaosMultiBind.MultiBindUInt(new uint?[] { 4 });
                mBinds[8] = TaosMultiBind.MultiBindUBigInt(new ulong?[] { 5 });
                mBinds[9] = TaosMultiBind.MultiBindFloat(new float?[] { 3.1415F * 2 });
                mBinds[10] = TaosMultiBind.MultiBindDouble(new double?[] { 3.1415926535897932D * 2 });
                mBinds[11] = TaosMultiBind.MultiBindBinary(new string?[] { "binary_tag_贰_Ⅱ" });
                mBinds[12] = TaosMultiBind.MultiBindNchar(new string?[] { "nchar_tag_贰_Ⅱ" });
            }
            if (seq == 3)
            {
                mBinds[0] = TaosMultiBind.MultiBindBool(new bool?[] { true });
                mBinds[1] = TaosMultiBind.MultiBindTinyInt(new sbyte?[] { 2 });
                mBinds[2] = TaosMultiBind.MultiBindSmallInt(new short?[] { 3 });
                mBinds[3] = TaosMultiBind.MultiBindInt(new int?[] { 4 });
                mBinds[4] = TaosMultiBind.MultiBindBigint(new long?[] { 5 });
                mBinds[5] = TaosMultiBind.MultiBindUTinyInt(new byte?[] { 3 });
                mBinds[6] = TaosMultiBind.MultiBindUSmallInt(new ushort?[] { 4 });
                mBinds[7] = TaosMultiBind.MultiBindUInt(new uint?[] { 5 });
                mBinds[8] = TaosMultiBind.MultiBindUBigInt(new ulong?[] { 6 });
                mBinds[9] = TaosMultiBind.MultiBindFloat(new float?[] { 3.1415F * 3 });
                mBinds[10] = TaosMultiBind.MultiBindDouble(new double?[] { 3.1415926535897932D * 3 });
                mBinds[11] = TaosMultiBind.MultiBindBinary(new string?[] { "binary_tag_叁_Ⅲ" });
                mBinds[12] = TaosMultiBind.MultiBindNchar(new string?[] { "nchar_tag_叁_Ⅲ" });
            }

            return mBinds;
        }


        public static TAOS_MULTI_BIND[] GetWSTags(int seq)
        {
            if (seq > 3 || seq < 1)
            {
                throw new Exception("seq should in range from 1-3");
            }

            TAOS_MULTI_BIND[] wsMBinds = new TAOS_MULTI_BIND[13];
            if (seq == 1)
            {
                wsMBinds[0] = WSMultiBind.WSBindBool(new bool?[] { true });
                wsMBinds[1] = WSMultiBind.WSBindTinyInt(new sbyte?[] { -1 });
                wsMBinds[2] = WSMultiBind.WSBindSmallInt(new short?[] { -2 });
                wsMBinds[3] = WSMultiBind.WSBindInt(new int?[] { -3 });
                wsMBinds[4] = WSMultiBind.WSBindBigInt(new long?[] { -4 });
                wsMBinds[5] = WSMultiBind.WSBindUTinyInt(new byte?[] { 1 });
                wsMBinds[6] = WSMultiBind.WSBindUSmallInt(new ushort?[] { 2 });
                wsMBinds[7] = WSMultiBind.WSBindUInt(new uint?[] { 3 });
                wsMBinds[8] = WSMultiBind.WSBindUBigInt(new ulong?[] { 4 });
                wsMBinds[9] = WSMultiBind.WSBindFloat(new float?[] { 3.1415F });
                wsMBinds[10] = WSMultiBind.WSBindDouble(new double?[] { 3.1415926535897932D });
                wsMBinds[11] = WSMultiBind.WSBindBinary(new string?[] { "binary_tag_壹_Ⅰ" });
                wsMBinds[12] = WSMultiBind.WSBindNchar(new string?[] { "nchar_tag_壹_Ⅰ" });
            }
            if (seq == 2)
            {
                wsMBinds[0] = WSMultiBind.WSBindBool(new bool?[] { false });
                wsMBinds[1] = WSMultiBind.WSBindTinyInt(new sbyte?[] { 1 });
                wsMBinds[2] = WSMultiBind.WSBindSmallInt(new short?[] { 2 });
                wsMBinds[3] = WSMultiBind.WSBindInt(new int?[] { 3 });
                wsMBinds[4] = WSMultiBind.WSBindBigInt(new long?[] { 4 });
                wsMBinds[5] = WSMultiBind.WSBindUTinyInt(new byte?[] { 2 });
                wsMBinds[6] = WSMultiBind.WSBindUSmallInt(new ushort?[] { 3 });
                wsMBinds[7] = WSMultiBind.WSBindUInt(new uint?[] { 4 });
                wsMBinds[8] = WSMultiBind.WSBindUBigInt(new ulong?[] { 5 });
                wsMBinds[9] = WSMultiBind.WSBindFloat(new float?[] { 3.1415F * 2 });
                wsMBinds[10] = WSMultiBind.WSBindDouble(new double?[] { 3.1415926535897932D * 2 });
                wsMBinds[11] = WSMultiBind.WSBindBinary(new string?[] { "binary_tag_贰_Ⅱ" });
                wsMBinds[12] = WSMultiBind.WSBindNchar(new string?[] { "nchar_tag_贰_Ⅱ" });
            }
            if (seq == 3)
            {
                wsMBinds[0] = WSMultiBind.WSBindBool(new bool?[] { true });
                wsMBinds[1] = WSMultiBind.WSBindTinyInt(new sbyte?[] { 2 });
                wsMBinds[2] = WSMultiBind.WSBindSmallInt(new short?[] { 3 });
                wsMBinds[3] = WSMultiBind.WSBindInt(new int?[] { 4 });
                wsMBinds[4] = WSMultiBind.WSBindBigInt(new long?[] { 5 });
                wsMBinds[5] = WSMultiBind.WSBindUTinyInt(new byte?[] { 3 });
                wsMBinds[6] = WSMultiBind.WSBindUSmallInt(new ushort?[] { 4 });
                wsMBinds[7] = WSMultiBind.WSBindUInt(new uint?[] { 5 });
                wsMBinds[8] = WSMultiBind.WSBindUBigInt(new ulong?[] { 6 });
                wsMBinds[9] = WSMultiBind.WSBindFloat(new float?[] { 3.1415F * 3 });
                wsMBinds[10] = TaosMultiBind.MultiBindDouble(new double?[] { 3.1415926535897932D * 3 });
                wsMBinds[11] = TaosMultiBind.MultiBindBinary(new string?[] { "binary_tag_叁_Ⅲ" });
                wsMBinds[12] = TaosMultiBind.MultiBindNchar(new string?[] { "nchar_tag_叁_Ⅲ" });
            }

            return wsMBinds;
        }


        public static TAOS_MULTI_BIND[] GetJsonTag(int seq)
        {
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[1];
            switch (seq)
            {
                case 1:
                    {
                        mBinds[0] = TaosMultiBind.MultiBindJson(jsonTag1);
                    }
                    break;
                case 2:
                    {
                        mBinds[0] = TaosMultiBind.MultiBindJson(jsonTag2);
                    }
                    break;
                case 3:
                    {
                        mBinds[0] = TaosMultiBind.MultiBindJson(jsonTag3);
                    }
                    break;
                default: throw new ArgumentOutOfRangeException("seq should in range from 1-3");
            }
            return mBinds;
        }

        public static TAOS_MULTI_BIND[] GetWSJsonTag(int seq)
        {
            TAOS_MULTI_BIND[] wsMBinds = new TAOS_MULTI_BIND[1];
            switch (seq)
            {
                case 1:
                    {
                        wsMBinds[0] = WSMultiBind.WSBindJSON(jsonTag1);
                    }
                    break;
                case 2:
                    {
                        wsMBinds[0] = WSMultiBind.WSBindJSON(jsonTag2);
                    }
                    break;
                case 3:
                    {
                        wsMBinds[0] = WSMultiBind.WSBindJSON(jsonTag3);
                    }
                    break;
                default: throw new ArgumentOutOfRangeException("seq should in range from 1-3");
            }
            return wsMBinds;
        }

        public static TAOS_MULTI_BIND[] GetColDataMBind()
        {
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[14];
            mBinds[0] = TaosMultiBind.MultiBindTimestamp(StmtDataSource.tsArr);
            mBinds[1] = TaosMultiBind.MultiBindTinyInt(StmtDataSource.tinyIntArr);
            mBinds[2] = TaosMultiBind.MultiBindSmallInt(StmtDataSource.shortArr);
            mBinds[3] = TaosMultiBind.MultiBindInt(StmtDataSource.intArr);
            mBinds[4] = TaosMultiBind.MultiBindBigint(StmtDataSource.longArr);
            mBinds[5] = TaosMultiBind.MultiBindUTinyInt(StmtDataSource.uTinyIntArr);
            mBinds[6] = TaosMultiBind.MultiBindUSmallInt(StmtDataSource.uShortArr);
            mBinds[7] = TaosMultiBind.MultiBindUInt(StmtDataSource.uIntArr);
            mBinds[8] = TaosMultiBind.MultiBindUBigInt(StmtDataSource.uLongArr);
            mBinds[9] = TaosMultiBind.MultiBindFloat(StmtDataSource.floatArr);
            mBinds[10] = TaosMultiBind.MultiBindDouble(StmtDataSource.doubleArr);
            mBinds[11] = TaosMultiBind.MultiBindBinary(StmtDataSource.binaryArr);
            mBinds[12] = TaosMultiBind.MultiBindNchar(StmtDataSource.ncharArr);
            mBinds[13] = TaosMultiBind.MultiBindBool(StmtDataSource.boolArr);
            return mBinds;
        }

        public static TAOS_MULTI_BIND[] GetWSColDataMBind()
        {
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[14];
            mBinds[0] = WSMultiBind.WSBindTimestamp(StmtDataSource.tsArr);
            mBinds[1] = WSMultiBind.WSBindTinyInt(StmtDataSource.tinyIntArr);
            mBinds[2] = WSMultiBind.WSBindSmallInt(StmtDataSource.shortArr);
            mBinds[3] = WSMultiBind.WSBindInt(StmtDataSource.intArr);
            mBinds[4] = WSMultiBind.WSBindBigInt(StmtDataSource.longArr);
            mBinds[5] = WSMultiBind.WSBindUTinyInt(StmtDataSource.uTinyIntArr);
            mBinds[6] = WSMultiBind.WSBindUSmallInt(StmtDataSource.uShortArr);
            mBinds[7] = WSMultiBind.WSBindUInt(StmtDataSource.uIntArr);
            mBinds[8] = WSMultiBind.WSBindUBigInt(StmtDataSource.uLongArr);
            mBinds[9] = WSMultiBind.WSBindFloat(StmtDataSource.floatArr);
            mBinds[10] = WSMultiBind.WSBindDouble(StmtDataSource.doubleArr);
            mBinds[11] = WSMultiBind.WSBindBinary(StmtDataSource.binaryArr);
            mBinds[12] = WSMultiBind.WSBindNchar(StmtDataSource.ncharArr);
            mBinds[13] = WSMultiBind.WSBindBool(StmtDataSource.boolArr);
            return mBinds;
        }



        public static TAOS_MULTI_BIND[] GetQueryConditionMBind(List<object> conditionList)
        {
            TAOS_MULTI_BIND[] queryCondition = new TAOS_MULTI_BIND[14];

            for (int i = 0; i < conditionList.Count; i++)
            {
                if ((i % 14 == 0) && (conditionList[i] is long))
                {
                    queryCondition[i] = TaosMultiBind.MultiBindTimestamp(new long[] { (long)conditionList[i] });
                }
                else if (conditionList[i] is sbyte)
                {
                    queryCondition[i] = TaosMultiBind.MultiBindTinyInt(new sbyte?[] { (sbyte)conditionList[i] });
                }
                else if (conditionList[i] is short)
                {
                    queryCondition[i] = TaosMultiBind.MultiBindSmallInt(new short?[] { (short)conditionList[i] });
                }
                else if (conditionList[i] is int)
                {
                    queryCondition[i] = TaosMultiBind.MultiBindInt(new int?[] { (int)conditionList[i] });
                }
                else if (conditionList[i] is long)
                {
                    queryCondition[i] = TaosMultiBind.MultiBindBigint(new long?[] { (long)conditionList[i] });
                }
                else if (conditionList[i] is byte)
                {
                    queryCondition[i] = TaosMultiBind.MultiBindUTinyInt(new byte?[] { (byte)conditionList[i] });
                }
                else if (conditionList[i] is ushort)
                {
                    queryCondition[i] = TaosMultiBind.MultiBindUSmallInt(new ushort?[] { (ushort)conditionList[i] });
                }
                else if (conditionList[i] is uint)
                {
                    queryCondition[i] = TaosMultiBind.MultiBindUInt(new uint?[] { (uint)conditionList[i] });
                }
                else if (conditionList[i] is ulong)
                {
                    queryCondition[i] = TaosMultiBind.MultiBindUBigInt(new ulong?[] { (ulong)conditionList[i] });
                }
                else if (conditionList[i] is float)
                {
                    queryCondition[i] = TaosMultiBind.MultiBindFloat(new float?[] { (float)conditionList[i] });
                }
                else if (conditionList[i] is double)
                {
                    queryCondition[i] = TaosMultiBind.MultiBindDouble(new double?[] { (double)conditionList[i] });
                }
                else if (conditionList[i] is string)
                {
                    if (i % 11 == 0)
                    {
                        queryCondition[i] = TaosMultiBind.MultiBindBinary(new string[] { (string)conditionList[i] });
                    }
                    if (i % 12 == 0)
                    {
                        queryCondition[i] = TaosMultiBind.MultiBindNchar(new string[] { (string)conditionList[i] });
                    }
                }
                else if (conditionList[i] is bool)
                {
                    queryCondition[i] = TaosMultiBind.MultiBindBool(new bool?[] { (bool)conditionList[i] });
                }
                else
                {
                    throw new Exception($"unknown type {conditionList[i].GetType()}");
                }

            }

            return queryCondition;
        }
        private static List<Object> GetTagData(int seq, bool ifJson = false)
        {
            List<Object> tagData = new();
            switch (seq)
            {
                case 1:
                    {
                        if (ifJson)
                        {
                            foreach (Object tag in jsonTag1)
                            {
                                tagData.Add(tag);
                            }

                        }
                        else
                        {
                            foreach (Object tag in tag1)
                            {
                                tagData.Add(tag);
                            }
                        }
                    }
                    break;
                case 2:
                    {
                        if (ifJson)
                        {
                            foreach (Object tag in jsonTag2)
                            {
                                tagData.Add(tag);
                            }

                        }
                        else
                        {
                            foreach (Object tag in tag2)
                            {
                                tagData.Add(tag);
                            }
                        }
                    }
                    break;
                case 3:
                    {
                        if (ifJson)
                        {
                            foreach (Object tag in jsonTag3)
                            {
                                tagData.Add(tag);
                            }

                        }
                        else
                        {
                            foreach (Object tag in tag3)
                            {
                                tagData.Add(tag);
                            }
                        }
                    }
                    break;
                default: throw new ArgumentOutOfRangeException("seq should in range from 1-3");
            }
            return tagData;

        }

        public static List<Object> STableRowData(int sequence = 1)
        {
            if (sequence > 3)
            {
                throw new ArgumentOutOfRangeException("seq should in range from 1-3");
            }

            List<Object> rowData = new();
            List<Object> tagData = GetTagData(sequence);
            for (int i = 0; i < tsArr.Length; i++)
            {
                rowData.Add(tsArr[i]);
                rowData.Add(tinyIntArr[i].Equals(null) ? "NULL" : tinyIntArr[i]);
                rowData.Add(shortArr[i].Equals(null) ? "NULL" : shortArr[i]);
                rowData.Add(intArr[i].Equals(null) ? "NULL" : intArr[i]);
                rowData.Add(longArr[i].Equals(null) ? "NULL" : longArr[i]);
                rowData.Add(uTinyIntArr[i].Equals(null) ? "NULL" : uTinyIntArr[i]);
                rowData.Add(uShortArr[i].Equals(null) ? "NULL" : uShortArr[i]);
                rowData.Add(uIntArr[i].Equals(null) ? "NULL" : uIntArr[i]);
                rowData.Add(uLongArr[i].Equals(null) ? "NULL" : uLongArr[i]);
                rowData.Add(floatArr[i].Equals(null) ? "NULL" : floatArr[i]);
                rowData.Add(doubleArr[i].Equals(null) ? "NULL" : doubleArr[i]);
                rowData.Add(String.IsNullOrEmpty(binaryArr[i]) ? "NULL" : binaryArr[i]);
                rowData.Add(String.IsNullOrEmpty(ncharArr[i]) ? "NULL" : ncharArr[i]);
                rowData.Add(boolArr[i].Equals(null) ? "NULL" : boolArr[i]);
                rowData.AddRange(tagData);
            }

            return rowData;
        }

        public static List<Object> NTableRowData()
        {
            List<Object> rowData = new();
            for (int i = 0; i < tsArr.Length; i++)
            {
                rowData.Add(tsArr[i]);
                rowData.Add(tinyIntArr[i].Equals(null) ? "NULL" : tinyIntArr[i]);
                rowData.Add(shortArr[i].Equals(null) ? "NULL" : shortArr[i]);
                rowData.Add(intArr[i].Equals(null) ? "NULL" : intArr[i]);
                rowData.Add(longArr[i].Equals(null) ? "NULL" : longArr[i]);
                rowData.Add(uTinyIntArr[i].Equals(null) ? "NULL" : uTinyIntArr[i]);
                rowData.Add(uShortArr[i].Equals(null) ? "NULL" : uShortArr[i]);
                rowData.Add(uIntArr[i].Equals(null) ? "NULL" : uIntArr[i]);
                rowData.Add(uLongArr[i].Equals(null) ? "NULL" : uLongArr[i]);
                rowData.Add(floatArr[i].Equals(null) ? "NULL" : floatArr[i]);
                rowData.Add(doubleArr[i].Equals(null) ? "NULL" : doubleArr[i]);
                rowData.Add(String.IsNullOrEmpty(binaryArr[i]) ? "NULL" : binaryArr[i]);
                rowData.Add(String.IsNullOrEmpty(ncharArr[i]) ? "NULL" : ncharArr[i]);
                rowData.Add(boolArr[i].Equals(null) ? "NULL" : boolArr[i]);
            }
            return rowData;
        }

        public static List<Object> JsonRowData(int seq = 1)
        {
            if (seq > 3 || seq < 1)
            {
                throw new ArgumentOutOfRangeException("seq should in range from 1-3");
            }
            List<Object> rowData = new List<Object>();
            List<Object> tag = GetTagData(seq, true);

            for (int i = 0; i < tsArr.Length; i++)
            {
                rowData.Add(tsArr[i]);
                rowData.Add(tinyIntArr[i].Equals(null) ? "NULL" : tinyIntArr[i]);
                rowData.Add(shortArr[i].Equals(null) ? "NULL" : shortArr[i]);
                rowData.Add(intArr[i].Equals(null) ? "NULL" : intArr[i]);
                rowData.Add(longArr[i].Equals(null) ? "NULL" : longArr[i]);
                rowData.Add(uTinyIntArr[i].Equals(null) ? "NULL" : uTinyIntArr[i]);
                rowData.Add(uShortArr[i].Equals(null) ? "NULL" : uShortArr[i]);
                rowData.Add(uIntArr[i].Equals(null) ? "NULL" : uIntArr[i]);
                rowData.Add(uLongArr[i].Equals(null) ? "NULL" : uLongArr[i]);
                rowData.Add(floatArr[i].Equals(null) ? "NULL" : floatArr[i]);
                rowData.Add(doubleArr[i].Equals(null) ? "NULL" : doubleArr[i]);
                rowData.Add(String.IsNullOrEmpty(binaryArr[i]) ? "NULL" : binaryArr[i]);
                rowData.Add(String.IsNullOrEmpty(ncharArr[i]) ? "NULL" : ncharArr[i]);
                rowData.Add(boolArr[i].Equals(null) ? "NULL" : boolArr[i]);
                rowData.AddRange(tag);
            }
            return rowData;
        }


        public static void FreeTaosMBind(TAOS_MULTI_BIND[] mBinds)
        {
            TaosMultiBind.FreeTaosBind(mBinds);
        }

    }
}