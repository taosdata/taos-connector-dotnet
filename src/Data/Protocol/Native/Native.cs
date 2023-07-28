using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using TDengine.Data.Client;
using TDengineDriver;

namespace TDengine.Data.Protocol.Native
{
    public class Native : ITDengineClient
    {
        public void Open(TDengineConnectionStringBuilder connectionStringBuilder, ref object connect)
        {
            var c = TDengineDriver.TDengine.Connect(
                connectionStringBuilder.Host == string.Empty ? null : connectionStringBuilder.Host,
                connectionStringBuilder.Username,
                connectionStringBuilder.Password, connectionStringBuilder.Database,
                (ushort)connectionStringBuilder.Port);
            if (c == IntPtr.Zero)
            {
                var errno = TDengineDriver.TDengine.ErrorNo(c);
                var errStr = TDengineDriver.TDengine.Error(c);
                throw new TDengineError(errno, errStr);
            }

            connect = c;
        }

        public string GetServerVersion(object connection)
        {
            return TDengineDriver.TDengine.GetServerInfo((IntPtr)connection);
        }

        public void ChangeDatabase(object connection, string db)
        {
            var errno = TDengineDriver.TDengine.SelectDatabase((IntPtr)connection, db);
            if (errno == 0) return;
            var errStr = TDengineDriver.TDengine.Error((IntPtr)connection);
            throw new TDengineError(errno, errStr);
        }

        public ITDengineRows Query(object connection, string sql)
        {
            IntPtr result = IntPtr.Zero;
            result = TDengineDriver.TDengine.Query((IntPtr)connection, sql);
            var errno = TDengineDriver.TDengine.ErrorNo(result);
            if (errno != 0)
            {
                throw new TDengineError(errno, TDengineDriver.TDengine.Error(result));
            }

            return new NativeRows(result);
        }

        public ITDengineRows Statement(object connection, string sql, Lazy<TDengineParameterCollection> parameters)
        {
            if (!parameters.IsValueCreated || parameters.Value.Count == 0)
            {
                return Query(connection, sql);
            }

            var stmt = TDengineDriver.TDengine.StmtInit((IntPtr)connection);
            if (stmt == IntPtr.Zero)
            {
                throw new TDengineError(-1, TDengineDriver.TDengine.StmtErrorStr(stmt));
            }


            var code = TDengineDriver.TDengine.StmtPrepare(stmt, sql);
            stmtCheckError(stmt, code);

            bool isInsert;
            IntPtr ptr = Marshal.AllocHGlobal(sizeof(int));
            try
            {
                code = TDengineDriver.TDengine.StmtIsInsert(stmt, ptr);
                stmtCheckError(stmt, code);
                isInsert = Marshal.ReadInt32(ptr) == 1;
            }
            finally
            {
                Marshal.FreeHGlobal(ptr);
            }

            var pms = parameters.Value;
            if (isInsert)
            {
                var tableName = Util.StmtParseTableName(pms);
                if (tableName != string.Empty)
                {
                    code = TDengineDriver.TDengine.StmtSetTbname(stmt, tableName);
                    stmtCheckError(stmt, code);
                }

                TaosFieldE[] colFields;
                TaosFieldE[] tagFields;

                try
                {
                    colFields = StmtGetColFields(stmt);
                }
                catch (Exception)
                {
                    TDengineDriver.TDengine.StmtClose(stmt);
                    throw;
                }

                try
                {
                    tagFields = StmtGetTagFields(stmt);
                }
                catch (Exception)
                {
                    tagFields = new TaosFieldE[0];
                }
                
                List<TAOS_MULTI_BIND> data;
                List<TAOS_MULTI_BIND> tags;
                var needFreePtr = Util.ParseParameters(pms, colFields, tagFields, out data, out tags);
                try
                {
                    if (tags.Count > 0)
                    {
                        code = TDengineDriver.TDengine.StmtSetTags(stmt, tags.ToArray());
                        stmtCheckError(stmt, code);
                    }

                    if (data.Count > 0)
                    {
                        code = TDengineDriver.TDengine.StmtBindParam(stmt, data.ToArray());
                        stmtCheckError(stmt, code);
                    }

                    code = TDengineDriver.TDengine.StmtAddBatch(stmt);
                    stmtCheckError(stmt, code);

                    code = TDengineDriver.TDengine.StmtExecute(stmt);
                    stmtCheckError(stmt, code);

                    var affected = TDengineDriver.TDengine.StmtAffetcedRowsOnce(stmt);

                    TDengineDriver.TDengine.StmtClose(stmt);
                    return new NativeRows(affected);
                }
                finally
                {
                    for (int i = 0; i < needFreePtr.Length; i++)
                    {
                        Marshal.FreeHGlobal(needFreePtr[i]);
                    }
                }
            }
            else
            {
                // set_table_name for INSERT only

                List<TAOS_MULTI_BIND> data;
                var needFreePtr = Util.ParseQueryParameters(pms, out data);
                try
                {
                    if (data.Count > 0)
                    {
                        code = TDengineDriver.TDengine.StmtBindParam(stmt, data.ToArray());
                        stmtCheckError(stmt, code);
                    }

                    code = TDengineDriver.TDengine.StmtAddBatch(stmt);
                    stmtCheckError(stmt, code);

                    code = TDengineDriver.TDengine.StmtExecute(stmt);
                    stmtCheckError(stmt, code);

                    var result = TDengineDriver.TDengine.StmtUseResult(stmt);
                    return new NativeRows(stmt, result);
                }
                finally
                {
                    for (int i = 0; i < needFreePtr.Length; i++)
                    {
                        Marshal.FreeHGlobal(needFreePtr[i]);
                    }
                }
            }
        }

        private void stmtCheckError(IntPtr stmt, int code)
        {
            if (code != 0)
            {
                var errorStr = TDengineDriver.TDengine.StmtErrorStr(stmt);
                TDengineDriver.TDengine.StmtClose(stmt);
                throw new TDengineError(code, errorStr);
            }
        }

        public void Close(object connection)
        {
            if ((IntPtr)connection != IntPtr.Zero)
            {
                TDengineDriver.TDengine.Close((IntPtr)connection);
            }
        }

        private static TaosFieldE[] StmtGetColFields(IntPtr stmt)
        {
            int fieldNum;
            IntPtr fieldsPtr;
            var code = TDengineDriver.TDengine.StmtGetColFields(stmt, out fieldNum, out fieldsPtr);
            if (code != 0)
            {
                throw new TDengineError(code, TDengineDriver.TDengine.StmtErrorStr(stmt));
            }

            TaosFieldE[] fields = new TaosFieldE[fieldNum];
            for (int i = 0; i < fieldNum; i++)
            {
                IntPtr fieldPtr = IntPtr.Add(fieldsPtr, i * Marshal.SizeOf(typeof(TaosFieldE)));
                fields[i] = (TaosFieldE)Marshal.PtrToStructure(fieldPtr, typeof(TaosFieldE));
            }

            TDengineDriver.TDengine.StmtReclaimFields(stmt, fieldsPtr);
            return fields;
        }

        private static TaosFieldE[] StmtGetTagFields(IntPtr stmt)
        {
            int fieldNum;
            IntPtr fieldsPtr;
            var code = TDengineDriver.TDengine.StmtGetTagFields(stmt, out fieldNum, out fieldsPtr);
            if (code != 0)
            {
                throw new TDengineError(code, TDengineDriver.TDengine.StmtErrorStr(stmt));
            }

            TaosFieldE[] fields = new TaosFieldE[fieldNum];
            for (int i = 0; i < fieldNum; i++)
            {
                IntPtr fieldPtr = IntPtr.Add(fieldsPtr, i * Marshal.SizeOf(typeof(TaosFieldE)));
                fields[i] = (TaosFieldE)Marshal.PtrToStructure(fieldPtr, typeof(TaosFieldE));
            }

            TDengineDriver.TDengine.StmtReclaimFields(stmt, fieldsPtr);
            return fields;
        }
    }
}