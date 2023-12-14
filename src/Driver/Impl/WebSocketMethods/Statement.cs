using System;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;

namespace TDengine.Driver.Impl.WebSocketMethods
{
    public partial class Connection
    {
    
        public WSStmtInitResp StmtInit(ulong reqId)
        {
            return SendJsonBackJson<WSStmtInitReq, WSStmtInitResp>(WSAction.STMTInit, new WSStmtInitReq
            {
                ReqId = reqId,
            });
        }

        public WSStmtPrepareResp StmtPrepare(ulong stmtId,string sql)
        {
            return SendJsonBackJson<WSStmtPrepareReq, WSStmtPrepareResp>(WSAction.STMTPrepare, new WSStmtPrepareReq
            {
                ReqId = _GetReqId(),
                StmtId = stmtId,
                SQL = sql
            });
        }
        
        public WSStmtSetTableNameResp StmtSetTableName(ulong stmtId,string tablename)
        {
            return SendJsonBackJson<WSStmtSetTableNameReq, WSStmtSetTableNameResp>(WSAction.STMTSetTableName, new WSStmtSetTableNameReq
            {
                ReqId = _GetReqId(),
                StmtId = stmtId,
                Name = tablename,
            });
        }

        public WSStmtSetTagsResp StmtSetTags(ulong stmtId,TaosFieldE[] fields, object[] tags)
        {
            //p0 uin64  req_id
            //p0+8 uint64  stmt_id
            //p0+16 uint64 (1 (set tag) 2 (bind))
            //p0+24 raw block
            Array[] param = new Array[tags.Length];
            for (int i = 0; i < tags.Length; i++)
            {
                if (tags[i] == null)
                {
                    var a = new object[1]{123};
                    Array newArray = Array.CreateInstance(TDengineConstant.ScanNullableType(fields[i].type), 1);
                    newArray.SetValue(null, 0);
                    param[i] = newArray;
                }
                else
                {
                    Array newArray = Array.CreateInstance(tags[i].GetType(), 1);
                    newArray.SetValue(tags[i], 0);
                    param[i] = newArray;
                }
            }

            var bytes = BlockWriter.Serialize(1, fields, param);
            var req = new byte[24 +bytes.Length];
            WriteUInt64ToBytes(req, _GetReqId(),0);
            WriteUInt64ToBytes(req,stmtId,8);
            WriteUInt64ToBytes(req,WSActionBinary.SetTagsMessage,16);
            Buffer.BlockCopy(bytes, 0, req, 24, bytes.Length);
            return SendBinaryBackJson<WSStmtSetTagsResp>(req);
        }
        
        public WSStmtBindResp StmtBind(ulong stmtId,TaosFieldE[] fields, object[] row)
        {
            //p0 uin64  req_id
            //p0+8 uint64  stmt_id
            //p0+16 uint64 (1 (set tag) 2 (bind))
            //p0+24 raw block
            Array[] param = new Array[row.Length];
            for (int i = 0; i < row.Length; i++)
            {
                if (row[i] == null)
                {
                    Array newArray = Array.CreateInstance(TDengineConstant.ScanNullableType(fields[i].type), 1);
                    newArray.SetValue(null, 0);
                    param[i] = newArray;
                }
                else
                {
                    Array newArray = Array.CreateInstance(row[i].GetType(), 1);
                    newArray.SetValue(row[i], 0);
                    param[i] = newArray;
                }
            }

            var bytes = BlockWriter.Serialize(1, fields, param);
            var req = new byte[24 +bytes.Length];
            WriteUInt64ToBytes(req, _GetReqId(),0);
            WriteUInt64ToBytes(req,stmtId,8);
            WriteUInt64ToBytes(req,WSActionBinary.BindMessage,16);
            Buffer.BlockCopy(bytes, 0, req, 24, bytes.Length);
            return SendBinaryBackJson<WSStmtBindResp>(req);
        }
        public WSStmtBindResp StmtBind(ulong stmtId,TaosFieldE[] fields, params Array[] param)
        {
            //p0 uin64  req_id
            //p0+8 uint64  stmt_id
            //p0+16 uint64 (1 (set tag) 2 (bind))
            //p0+24 raw block

            var bytes = BlockWriter.Serialize(param[0].Length, fields, param);
            var req = new byte[24 +bytes.Length];
            WriteUInt64ToBytes(req, _GetReqId(),0);
            WriteUInt64ToBytes(req,stmtId,8);
            WriteUInt64ToBytes(req,WSActionBinary.BindMessage,16);
            Buffer.BlockCopy(bytes, 0, req, 24, bytes.Length);
            return SendBinaryBackJson<WSStmtBindResp>(req);
        }

        public WSStmtAddBatchResp StmtAddBatch(ulong stmtId)
        {
            return SendJsonBackJson<WSStmtAddBatchReq, WSStmtAddBatchResp>(WSAction.STMTAddBatch, new WSStmtAddBatchReq
            {
                ReqId = _GetReqId(),
                StmtId = stmtId
            });
        }
        
        public WSStmtExecResp StmtExec(ulong stmtId)
        {
            return SendJsonBackJson<WSStmtExecReq, WSStmtExecResp>(WSAction.STMTExec, new WSStmtExecReq
            {
                ReqId = _GetReqId(),
                StmtId = stmtId
            });
        }

        public WSStmtGetColFieldsResp StmtGetColFields(ulong stmtId)
        {
            return SendJsonBackJson<WSStmtGetColFieldsReq, WSStmtGetColFieldsResp>(WSAction.STMTGetColFields, new WSStmtGetColFieldsReq
            {
                ReqId = _GetReqId(),
                StmtId = stmtId
            });
        }
        public WSStmtGetTagFieldsResp StmtGetTagFields(ulong stmtId)
        {
            return SendJsonBackJson<WSStmtGetTagFieldsReq, WSStmtGetTagFieldsResp>(WSAction.STMTGetTagFields, new WSStmtGetTagFieldsReq
            {
                ReqId = _GetReqId(),
                StmtId = stmtId
            });
        }

        public WSStmtUseResultResp StmtUseResult(ulong stmtId)
        {
            return SendJsonBackJson<WSStmtUseResultReq, WSStmtUseResultResp>(WSAction.STMTUseResult,
                new WSStmtUseResultReq
                {
                    ReqId = _GetReqId(),
                    StmtId = stmtId
                });
        }
        public void StmtClose(ulong stmtId)
        {
            SendJson(WSAction.STMTClose, new WSStmtCloseReq
            {
                ReqId = _GetReqId(),
                StmtId = stmtId
            });
        }
        
    }
}