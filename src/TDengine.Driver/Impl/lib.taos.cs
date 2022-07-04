using System;
using System.Runtime.InteropServices;
using System.Collections.Generic;
//using TDengineDriver;
namespace TDengineDriver.Impl
{
    public static class LibTaos
    {
        public static List<Object> GetData(IntPtr taosRes)
        {
            IfNullReference(taosRes);

            List<TDengineMeta> metaList = GetMeta(taosRes);
            List<Object> list;

            IntPtr numOfRowsPrt = Marshal.AllocHGlobal(sizeof(Int32));
            IntPtr pDataPtr = Marshal.AllocHGlobal(IntPtr.Size);
            IntPtr pData;
            try
            {
                int code = TDengine.FetchRawBlock(taosRes, numOfRowsPrt, pDataPtr);
                if (code != 0)
                {
                    throw new Exception($"fetch_raw_block failed,code {code} reason:{TDengine.Error(taosRes)}");
                }
                else
                {
                    int numOfRows = Marshal.ReadInt32(numOfRowsPrt);
                    int numOfFileds = TDengine.FieldCount(taosRes);
                    list = new List<Object>(numOfRows * numOfFileds);
                    pData = Marshal.ReadIntPtr(pDataPtr);
                    list = ReadRawBlock(pData, metaList, numOfRows);
                    return list;
                }
            }
            finally
            {
                TDengine.FreeResult(taosRes);
            }

        }
        public static List<TDengineMeta> GetMeta(IntPtr taosRes)
        {
            IfNullReference(taosRes);
            return TDengine.FetchFields(taosRes);
        }

        public static List<object> ReadRawBlock(IntPtr pData, List<TDengineMeta> metaList, int numOfRows)
        {

            int numOfFileds = metaList.Count;
            List<Object> list = new List<Object>(numOfRows * numOfFileds);
            Console.WriteLine("numOfFileds:{0}", numOfFileds);
            Console.WriteLine("numOfRows:{0}", numOfRows);
            Console.WriteLine("pData:{0}", pData);


            // int colDataLength = 0;
            int colLengthBlockSize = sizeof(Int32) * numOfFileds;

            int bitMapSize = (int)Math.Ceiling(numOfRows / 8.0);
            Console.WriteLine("bitMapSize:{0}", bitMapSize);
            // offset pDataPtr 12 bytes
            pData = pData + 12;
            for (int i = 0; i < numOfRows; i++)
            {
                IntPtr colBlockHead = pData + colLengthBlockSize;
                IntPtr colDataHead = colBlockHead;
                for (int j = 0; j < numOfFileds; j++)
                {
                    Console.WriteLine(" Marshal.ReadInt32(pData, (j) * sizeof(Int32)):{0}|{1}", Marshal.ReadInt32(pData, (j) * sizeof(Int32)), j);

                    //solid length Type
                    if (!_IsVarData(metaList[j]))
                    {
                        colDataHead = colBlockHead + bitMapSize + metaList[j].size * i;
                        Console.WriteLine("solid:colDataHead:{0},colBlockHead:{1},pData:{2}", colDataHead, colBlockHead, pData);
                        // which range
                        var byteArrayIndex = i >> 3;
                        // locate position
                        var bitwiseOffset = 7 - (i & 7);
                        // var bitwiseOffset = 7- (i%8);
                        Console.WriteLine("bit map {0},{1}", byteArrayIndex, bitwiseOffset);
                        Byte[] bitMap = new Byte[bitMapSize];
                        Marshal.Copy(colBlockHead, bitMap, 0, bitMapSize);

                        var bitFlag = (bitMap[byteArrayIndex] & (1 << bitwiseOffset)) >> bitwiseOffset;
                        Console.WriteLine("bitFlag:{0},bitmapValue:{1}", bitFlag, Convert.ToString(bitMap[byteArrayIndex], 2));
                        if (bitFlag == 1)
                        {
                            list.Add(null);

                        }
                        else
                        {
                            list.Add(_ReadSolidType((IntPtr)colDataHead, (IntPtr)colBlockHead, metaList[j]));
                        }
                        colBlockHead = colBlockHead + bitMapSize + Marshal.ReadInt32(pData, (j) * sizeof(Int32));
                        Console.WriteLine("solid:colDataHead:{0},colBlockHead:{1},pData:{2}", colDataHead, colBlockHead, pData);
                    }
                    else
                    {

                        int varOffset = Marshal.ReadInt32(colBlockHead, sizeof(Int32) * i);
                        Console.WriteLine($"varOffset + (sizeof(Int16) * {j}):{varOffset}");
                        if (varOffset == -1)
                        {
                            list.Add("NULL");
                            colBlockHead = colBlockHead + sizeof(Int32) * numOfRows + Marshal.ReadInt32(pData, (j) * sizeof(Int32));
                        }
                        else
                        {
                            colDataHead = colBlockHead + sizeof(Int32) * numOfRows + varOffset;
                            int varTypeLength = Marshal.ReadInt16(colDataHead);
                            // Console.WriteLine("===var:colDataHead:{0},colBlockHead:{1},pData:{2}", colDataHead, colBlockHead, pData);
                            // Console.WriteLine("===varTypeLength:{0},varOffset:{1}", varTypeLength, varOffset);

                            list.Add(_ReadVarType(colDataHead + 2, varTypeLength));
                            colBlockHead = colBlockHead + sizeof(Int32) * numOfRows + Marshal.ReadInt32(pData, (j) * sizeof(Int32));
                            Console.WriteLine("var:colDataHead:{0},colBlockHead:{1},pData:{2}", colDataHead, colBlockHead, pData);
                        }
                    }
                }
            }
            return list;

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
            // if (Marshal.ReadByte(ifNullOffset) == 0) // read bit not byte 
            // {
            //     this.taosResult.Add("NULL");
            // }
            // else
            {
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
        }


        private static object _ReadVarType(IntPtr blockHead, int length)
        {
            Object data;
            if (length != -1)
            {
                Console.WriteLine("Marshal.PtrToStringAnsi(blockHead, length),{0}", Marshal.PtrToStringUTF8(blockHead, length));
                data = Marshal.PtrToStringUTF8(blockHead, length);
            }
            else
            {
                data = "NULL";
            }
            return data;
        }

        private static bool _IsVarData(TDengineMeta meta)
        {
            switch ((TDengineDataType)meta.type)
            {
                case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                    return true;
                default:
                    return false;
            }
        }

    }


}

