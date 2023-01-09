using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
//using TDengineDriver;
namespace TDengineDriver.Impl
{
    public static class LibTaos
    {
        public static List<Object> GetData(IntPtr taosRes)
        {
            IfNullReference(taosRes);

            List<TDengineMeta> metaList = GetMeta(taosRes);
            List<Object> list = new List<object>();

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
                        throw new Exception($"fetch_raw_block failed,code {code} reason:{TDengine.Error(taosRes)}");
                    }
                    int numOfRows = Marshal.ReadInt32(numOfRowsPrt);
                    int numOfFields = TDengine.FieldCount(taosRes);
                    if (numOfRows == 0)
                    {
                        break;
                    }
                    else
                    {

                        //list = new List<Object>(numOfRows * numOfFields);
                        pData = Marshal.ReadIntPtr(pDataPtr);
                        list.AddRange(ReadRawBlock(pData, metaList, numOfRows));
                        //return list;
                    }

                }
                return list;
            }
            finally
            {
                Marshal.FreeHGlobal(numOfRowsPrt);
                Marshal.FreeHGlobal(pDataPtr);
            }

        }
        public static List<TDengineMeta> GetMeta(IntPtr taosRes)
        {
            IfNullReference(taosRes);
            return TDengine.FetchFields(taosRes);
        }

        public static List<object> ReadRawBlock(IntPtr pData, List<TDengineMeta> metaList, int numOfRows)
        {

            int numOfFields = metaList.Count;
            List<Object> list = new List<Object>(numOfRows * numOfFields);

            pData = pData + (4 * 5) + 8 + (4 + 1) * numOfFields;

            int colLengthBlockSize = sizeof(Int32) * numOfFields;
            int bitMapSize = (numOfRows+((1<<3)-1))>>3;

            for (int i = 0; i < numOfRows; i++)
            {
                IntPtr colBlockHead = pData + colLengthBlockSize;
                IntPtr colDataHead = colBlockHead;
                for (int j = 0; j < numOfFields; j++)
                {
                    //solid length Type
                    if (_IsVarData(metaList[j]) == ColType.SOLID)
                    {
                        colDataHead = colBlockHead + bitMapSize + metaList[j].size * i;

                        // which range
                        var byteArrayIndex = i >> 3;
                        // locate position
                        var bitwiseOffset = 7 - (i & 7);

                        Byte[] bitMap = new Byte[bitMapSize];
                        Marshal.Copy(colBlockHead, bitMap, 0, bitMapSize);

                        var bitFlag = (bitMap[byteArrayIndex] & (1 << bitwiseOffset)) >> bitwiseOffset;

                        if (bitFlag == 1)
                        {
                            list.Add("NULL");

                        }
                        else
                        {
                            list.Add(_ReadSolidType((IntPtr)colDataHead, (IntPtr)colBlockHead, metaList[j]));
                        }

                        colBlockHead = colBlockHead + bitMapSize + Marshal.ReadInt32(pData, (j) * sizeof(Int32));
                    }
                    else
                    {

                        int varOffset = Marshal.ReadInt32(colBlockHead, sizeof(Int32) * i);
                        if (varOffset == -1)
                        {
                            list.Add("NULL");
                            colBlockHead = colBlockHead + sizeof(Int32) * numOfRows + Marshal.ReadInt32(pData, (j) * sizeof(Int32));
                        }
                        else
                        {
                            colDataHead = colBlockHead + sizeof(Int32) * numOfRows + varOffset;
                            int varTypeLength = Marshal.ReadInt16(colDataHead);

                            if (_IsVarData(metaList[j]) == ColType.VARCHAR)
                            {
                                list.Add(_ReadVarType(colDataHead + 2, varTypeLength));
                            }
                            else
                            {
                                list.Add(_ReadNcharType(colDataHead + 2, varTypeLength));
                            }

                            colBlockHead = colBlockHead + sizeof(Int32) * numOfRows + Marshal.ReadInt32(pData, (j) * sizeof(Int32));
                        }
                    }
                }
            }
            return list;
        }

        internal static void IfNullReference(IntPtr ptr)
        {
            if (ptr == IntPtr.Zero)
            {
                throw new NullReferenceException("null reference");
            }
        }


        private static Object _ReadSolidType(IntPtr pdata, IntPtr ifNullOffset, TDengineMeta field)
        {
            Object data;

            switch ((TDengineDataType)field.type)
            {
                case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                    data = Marshal.ReadByte(pdata) == 0 ? false : true;
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                    data = (sbyte)Marshal.ReadByte(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                    data = Marshal.ReadInt16(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_INT:
                    data = Marshal.ReadInt32(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                    data = Marshal.ReadInt64(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                    data = (float)Marshal.PtrToStructure(pdata, typeof(float));
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                    data = (double)Marshal.PtrToStructure(pdata, typeof(double));
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                    data = Marshal.ReadInt64(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                    data = Marshal.ReadByte(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_USMALLINT:
                    data = (ushort)Marshal.ReadInt16(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UINT:
                    data = (uint)Marshal.ReadInt32(pdata);
                    break;
                case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                    data = (ulong)Marshal.ReadInt64(pdata);
                    break;
                default:
                    throw new Exception($"TDengine unsupported data type {field.type}");
            }
            return data;


        }


        private static string _ReadVarType(IntPtr blockHead, int length)
        {
            string data;
            if (length != -1)
            {
                // need to decode with UTF-8
                Byte[] bytes = new Byte[length];
                Marshal.Copy(blockHead, bytes, 0, length);
                data = Encoding.UTF8.GetString(bytes);
            }
            else
            {
                data = "NULL";
            }
            return data;
        }
        private static string _ReadNcharType(IntPtr blockHead, int length)
        {
            string data;
            if (length != -1)
            {
                int cnt = length / 4;
                StringBuilder tmp = new StringBuilder(cnt);
                for (int i = 0; i < cnt; i++)
                {
                    Int32 utf32 = (Int32)Marshal.ReadInt32(blockHead, i * 4);
                    tmp.Append(Char.ConvertFromUtf32(utf32));

                }
                data = tmp.ToString();
                tmp.Clear();
            }
            else
            {
                data = "NULL";
            }
            return data;
        }

        private static ColType _IsVarData(TDengineMeta meta)
        {
            switch ((TDengineDataType)meta.type)
            {
                case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                    return ColType.NCHAR;
                case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                    return ColType.VARCHAR;
                default:
                    return ColType.SOLID;
            }
        }

        private enum ColType
        {
            SOLID = 0,
            VARCHAR = 1,
            NCHAR = 2,
        }
    }


}

